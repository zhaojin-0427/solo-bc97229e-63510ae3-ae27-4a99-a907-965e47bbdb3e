"""后台装载、断点续传与多 worker 协调。

- 全量:后台线程分块读 CSV 写入暂存区,每块与进度(文件偏移)同事务提交,
  进程崩溃/被 kill 后由存活 worker 从偏移处续装;装载完成后合并切换为新基线。
- 增量:上传请求内同步确认(立即更新正式表);若进程在确认途中被终止,
  存活 worker 的监控线程从未完成偏移继续,已确认增量因日志持久化而不丢失。
- 多 worker 协调:每个批次由 :class:`app.locks.BatchGuard` 保证
  “跨进程 flock + 进程内线程锁”双层互斥。两个 worker 同时发现同一待处理
  批次时,只有一个能取得锁推进进度、写确认日志或切换基线;另一个跳过,
  绝不重复处理。持锁进程被强制终止时内核自动释放 flock,存活 worker 下一轮
  轮询即可接续持久进度,无需重新上传,也无需重启整个服务。
"""
import logging
import os
import threading
import time

from . import db, logic
from .locks import BatchGuard, LockUnavailable

_log = logging.getLogger("lims.loader")
_monitor_lock = threading.Lock()
_monitor_started = False


class PauseProcessing(Exception):
    """测试用:达到 max_chunks 后暂停,保持批次未完成。"""


# ------------------------------------------------------------------ 全量

def process_full_batch(data_dir, batch_id, chunk_size, chunk_delay=0.0,
                       max_chunks=None, chunk_hook=None,
                       blocking=True, lock_timeout=60.0):
    """处理一个全量批次直至完成(或 PauseProcessing)。可重入:从记录偏移继续。

    返回 True 表示本调用完成了处理(或批次已不存在/已完成);
    非阻塞模式下拿不到锁返回 False。
    """
    guard = BatchGuard(data_dir, batch_id, blocking=blocking,
                       timeout=lock_timeout)
    with guard:
        conn = db.connect(data_dir)
        try:
            while True:
                batch = conn.execute(
                    "SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
                if batch is None or batch["kind"] != "full":
                    return True
                if batch["status"] == "staging_ready":
                    _merge_and_switch(conn, batch_id)
                    return True
                if batch["status"] != "loading":
                    return True

                path = batch["filename"]
                size = os.path.getsize(path)
                with open(path, "r", encoding="utf-8-sig", newline="") as f:
                    # 无论新建还是恢复,都先解析表头(列映射),再跳到数据偏移
                    header, data_start = logic.read_header(f)
                    missing = [c for c in logic.FULL_COLUMNS if c not in header]
                    if missing:
                        raise logic.CsvFormatError(f"全量CSV缺少列: {missing}")

                    offset = batch["processed_bytes"]
                    if offset == 0:
                        offset = data_start
                        conn.execute("BEGIN IMMEDIATE")
                        conn.execute(
                            "UPDATE batches SET processed_bytes=?, total_bytes=?,"
                            " updated_at=datetime('now') WHERE id=?",
                            (offset, size, batch_id))
                        conn.execute("COMMIT")
                    f.seek(offset)
                    reader = logic.csv_line_reader(f)

                    chunks = 0
                    while True:
                        if max_chunks is not None and chunks >= max_chunks:
                            raise PauseProcessing()
                        rows, last_offset, eof = [], None, False
                        while len(rows) < chunk_size:
                            row = next(reader, None)
                            if row is None:
                                eof, last_offset = True, f.tell()
                                break
                            rows.append(row)
                            last_offset = f.tell()

                        if chunk_hook is not None:
                            chunk_hook(chunks)  # 故障注入点:抛异常模拟进程崩溃

                        applied = errors = 0
                        conn.execute("BEGIN IMMEDIATE")
                        try:
                            for row in rows:
                                if not row or all(not (c or "").strip() for c in row):
                                    continue  # 跳过空行
                                try:
                                    r = logic.parse_full_row(header, row)
                                except ValueError:
                                    errors += 1
                                    continue
                                logic.stage_full_row(conn, *r)
                                applied += 1
                            conn.execute(
                                "UPDATE batches SET processed_bytes=?, total_rows=total_rows+?,"
                                " applied_rows=applied_rows+?, error_rows=error_rows+?,"
                                " status=?, updated_at=datetime('now') WHERE id=?",
                                (last_offset, len(rows), applied, errors,
                                 "staging_ready" if eof else "loading", batch_id))
                            conn.execute("COMMIT")
                        except BaseException:
                            conn.execute("ROLLBACK")
                            raise
                        chunks += 1
                        if chunk_delay:
                            time.sleep(chunk_delay)
                        if eof:
                            break  # 回到外层循环 -> staging_ready -> 合并切换
        finally:
            conn.close()


def _merge_and_switch(conn, batch_id):
    """单事务完成 墓碑承继+增量回放+切换;崩溃则整体回滚,恢复后重做。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        logic.merge_and_switch(conn, batch_id)
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


# ------------------------------------------------------------------ 增量

def process_inc_batch(data_dir, batch_id, chunk_size,
                      max_chunks=None, chunk_hook=None,
                      blocking=True, lock_timeout=60.0):
    """确认一个增量批次(请求内同步或恢复调用)。每块一个事务:日志+正式表+进度。

    双层锁保证同一批次在集群内只被一个执行体确认;非阻塞模式拿不到锁
    返回 False。进入锁后先复查批次状态,持锁方完成后等待方直接跳过,
    绝不重复写确认日志。
    """
    guard = BatchGuard(data_dir, batch_id, blocking=blocking,
                       timeout=lock_timeout)
    with guard:
        conn = db.connect(data_dir)
        try:
            batch = conn.execute(
                "SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            if batch is None or batch["kind"] != "inc" \
                    or batch["status"] != "processing":
                return True
            path = batch["filename"]
            size = os.path.getsize(path)
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                header, data_start = logic.read_header(f)
                missing = [c for c in logic.INC_COLUMNS if c not in header]
                if missing:
                    raise logic.CsvFormatError(f"增量CSV缺少列: {missing}")

                offset = batch["processed_bytes"]
                if offset == 0:
                    offset = data_start
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute("UPDATE batches SET processed_bytes=?, total_bytes=?,"
                                 " updated_at=datetime('now') WHERE id=?",
                                 (offset, size, batch_id))
                    conn.execute("COMMIT")
                f.seek(offset)
                reader = logic.csv_line_reader(f)

                chunks = 0
                while True:
                    if max_chunks is not None and chunks >= max_chunks:
                        raise PauseProcessing()
                    rows, last_offset, eof = [], None, False
                    while len(rows) < chunk_size:
                        row = next(reader, None)
                        if row is None:
                            eof, last_offset = True, f.tell()
                            break
                        rows.append(row)
                        last_offset = f.tell()
                    if not rows and eof:
                        conn.execute("BEGIN IMMEDIATE")
                        conn.execute("UPDATE batches SET status='done', processed_bytes=?,"
                                     " updated_at=datetime('now') WHERE id=?",
                                     (last_offset, batch_id))
                        conn.execute("COMMIT")
                        return True

                    if chunk_hook is not None:
                        chunk_hook(chunks)

                    applied = errors = 0
                    conn.execute("BEGIN IMMEDIATE")
                    try:
                        for row in rows:
                            if not row or all(not (c or "").strip() for c in row):
                                continue
                            try:
                                r = logic.parse_inc_row(header, row)
                            except ValueError:
                                errors += 1
                                continue
                            logic.confirm_increment(conn, batch_id, *r)
                            applied += 1
                        conn.execute(
                            "UPDATE batches SET processed_bytes=?, total_rows=total_rows+?,"
                            " applied_rows=applied_rows+?, error_rows=error_rows+?,"
                            " status=?, updated_at=datetime('now') WHERE id=?",
                            (last_offset, len(rows), applied, errors,
                             "done" if eof else "processing", batch_id))
                        conn.execute("COMMIT")
                    except BaseException:
                        conn.execute("ROLLBACK")
                        raise
                    chunks += 1
                    if eof:
                        return True
        finally:
            conn.close()


# ------------------------------------------------------------------ 调度与恢复

def find_pending(conn):
    """返回需要继续处理的批次 id(先增量后全量)。"""
    inc = conn.execute(
        "SELECT id FROM batches WHERE kind='inc' AND status='processing' ORDER BY id").fetchall()
    full = conn.execute(
        "SELECT id FROM batches WHERE kind='full' AND status IN ('loading','staging_ready')"
        " ORDER BY id").fetchall()
    return [r["id"] for r in inc], [r["id"] for r in full]


def run_pending(data_dir, chunk_size, chunk_delay=0.0,
                max_chunks=None, chunk_hook=None, lock_timeout=60.0):
    """同步执行所有待处理工作(恢复/测试用)。返回处理过的批次。

    CSV 格式错误属于确定性错误:标记批次失败并继续处理其余批次;
    其他异常(如模拟断电)原样抛出,保持批次未完成以便恢复。
    """
    done = []
    while True:
        conn = db.connect(data_dir)
        try:
            inc_ids, full_ids = find_pending(conn)
        finally:
            conn.close()
        if not inc_ids and not full_ids:
            break
        try:
            for bid in inc_ids:
                if process_inc_batch(data_dir, bid, chunk_size,
                                     max_chunks=max_chunks,
                                     chunk_hook=chunk_hook,
                                     lock_timeout=lock_timeout):
                    done.append(("inc", bid))
            for bid in full_ids:
                if process_full_batch(data_dir, bid, chunk_size, chunk_delay,
                                      max_chunks=max_chunks,
                                      chunk_hook=chunk_hook,
                                      lock_timeout=lock_timeout):
                    done.append(("full", bid))
        except LockUnavailable:
            # 被其他执行体持锁:等一轮再看,避免忙等
            time.sleep(0.1)
        except logic.CsvFormatError as exc:
            for bid in inc_ids + full_ids:
                mark_failed(data_dir, bid, exc)
        except PauseProcessing:
            break
        if max_chunks is not None:
            break
    return done


def mark_failed(data_dir, batch_id, message):
    conn = db.connect(data_dir)
    try:
        conn.execute("UPDATE batches SET status='failed', error=?,"
                     " updated_at=datetime('now') WHERE id=?", (str(message), batch_id))
    finally:
        conn.close()


# ------------------------------------------------------------------ 常驻监控

def _process_one(data_dir, kind, bid, chunk_size, chunk_delay):
    """监控线程尝试推进一个批次;锁被占用时安静跳过,等待下一轮。"""
    try:
        if kind == "inc":
            return process_inc_batch(data_dir, bid, chunk_size, blocking=False)
        return process_full_batch(data_dir, bid, chunk_size, chunk_delay,
                                  blocking=False)
    except LockUnavailable:
        return False
    except Exception as exc:                     # 单批失败不拖垮其他批次
        _log.exception("批次 %s 处理失败", bid)
        mark_failed(data_dir, bid, exc)
        return True


def _monitor_loop(app):
    """每个 worker 一个常驻守护线程:轮询共享数据目录中的待处理批次。

    两个 worker 同时发现同一批次时,只有一个能拿到 BatchGuard;
    持锁 worker 被强制终止后 flock 由内核释放,存活 worker 下一轮自动接续。
    """
    data_dir = app.config["DATA_DIR"]
    chunk_size = app.config["CHUNK_SIZE"]
    chunk_delay = app.config["CHUNK_DELAY"]
    idle_seconds = float(app.config.get("MONITOR_IDLE_SECONDS", 1.0))
    busy_seconds = float(app.config.get("MONITOR_BUSY_SECONDS", 0.2))
    while True:
        conn = db.connect(data_dir)
        try:
            inc_ids, full_ids = find_pending(conn)
        finally:
            conn.close()
        pending = [("inc", i) for i in inc_ids] + [("full", i) for i in full_ids]
        if not pending:
            time.sleep(idle_seconds)
            continue
        worked = False
        for kind, bid in pending:
            if _process_one(data_dir, kind, bid, chunk_size, chunk_delay):
                worked = True
        time.sleep(busy_seconds if worked else idle_seconds)


def poke_background(app):
    """确保本 worker 的常驻监控线程在运行(幂等;上传后立即唤醒)。"""
    global _monitor_started
    with _monitor_lock:
        if _monitor_started:
            return
        _monitor_started = True
    thread = threading.Thread(
        target=_monitor_loop, args=(app,),
        name=f"lims-monitor-pid{os.getpid()}", daemon=True)
    thread.start()


def start_recovery_thread(app):
    """应用(每个 worker)启动时调用:恢复并持续接续未完成工作。"""
    if app.config.get("TESTING"):
        return
    poke_background(app)
