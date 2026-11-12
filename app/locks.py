"""跨进程 / 跨线程的批次互斥锁。

多个 Gunicorn worker(独立进程)共用同一数据目录时,必须保证同一个批次
在全集群内同一时刻只被一个执行体推进,否则会:
  - 重复累加 processed_rows/applied_rows 等进度计数;
  - 重复写 increments 确认日志;
  - 基线切换的 DELETE+INSERT 被第二个进程再做一遍,清空正式表。

实现为两层互斥:

1. 进程内 ``threading.Lock``:同一 worker 的请求线程与后台监控线程之间
   互斥(flock 只在进程间互斥,同进程不同线程可同时持有)。
2. 进程间 ``fcntl.flock`` 顾问锁:锁文件位于共享数据目录
   ``<DATA_DIR>/locks/batch-<id>.lock``。flock 与打开文件描述符绑定,
   进程被 SIGKILL/崩溃时由内核自动释放,**无需心跳或租约**,存活 worker
   下一轮轮询即可获得锁并从持久化进度接续处理。
"""
import errno
import fcntl
import os
import threading
import time

# 进程内每批次一把锁(不同批次互不阻塞,允许并行)
_local_locks = {}
_local_locks_guard = threading.Lock()

_LOCK_BUSY_ERRNOS = (errno.EAGAIN, errno.EACCES)


class LockUnavailable(Exception):
    """批次正被其他执行体(worker 进程或本进程线程)处理。"""

    def __init__(self, batch_id, timeout=0.0):
        self.batch_id = batch_id
        self.timeout = timeout
        super().__init__(
            f"批次 {batch_id} 正被其他 worker 处理"
            + (f"(等待 {timeout:.1f}s 仍未获得锁)" if timeout else ""))


def _local_lock(batch_id):
    with _local_locks_guard:
        lock = _local_locks.get(batch_id)
        if lock is None:
            lock = threading.Lock()
            _local_locks[batch_id] = lock
        return lock


def _acquire_local(lock, blocking, timeout):
    if blocking:
        return lock.acquire(blocking=True, timeout=timeout)
    return lock.acquire(blocking=False)


class BatchGuard:
    """批次互斥上下文。

    - ``blocking=False``:获取不到锁立即抛 :class:`LockUnavailable`
      (后台监控线程轮询时使用)。
    - ``blocking=True``:在 ``timeout`` 秒内反复重试,超时仍失败才抛出
      (请求线程使用:增量必须在成功响应前确认完毕)。
    """

    def __init__(self, data_dir, batch_id, blocking=True, timeout=60.0,
                 poll_interval=0.05):
        self.data_dir = data_dir
        self.batch_id = batch_id
        self.blocking = blocking
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._local = None
        self._fh = None
        self._lock_path = os.path.join(
            data_dir, "locks", f"batch-{batch_id}.lock")

    def __enter__(self):
        # 1) 进程内互斥:请求线程与后台线程不会同时推进同一批次
        self._local = _local_lock(self.batch_id)
        if not _acquire_local(self._local, self.blocking, self.timeout):
            raise LockUnavailable(self.batch_id, self.timeout)

        # 2) 跨进程互斥:flock 锁文件(进程崩溃/被 kill 时内核自动释放)
        os.makedirs(os.path.join(self.data_dir, "locks"), exist_ok=True)
        self._fh = open(self._lock_path, "a+")
        try:
            if self.blocking:
                deadline = time.monotonic() + self.timeout
                while True:
                    try:
                        fcntl.flock(self._fh.fileno(),
                                    fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except OSError as exc:
                        if exc.errno not in _LOCK_BUSY_ERRNOS:
                            raise
                        if time.monotonic() >= deadline:
                            raise LockUnavailable(self.batch_id, self.timeout)
                        time.sleep(self.poll_interval)
            else:
                try:
                    fcntl.flock(self._fh.fileno(),
                                fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    if exc.errno not in _LOCK_BUSY_ERRNOS:
                        raise
                    raise LockUnavailable(self.batch_id)
            # 记录持有者,便于演示/排障(互斥本身仍以 flock 为准)
            self._fh.seek(0)
            self._fh.truncate()
            self._fh.write(f"pid={os.getpid()}\n")
            self._fh.flush()
        except BaseException:
            self._fh.close()
            self._fh = None
            self._local.release()
            raise
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._local.release()
        return False
