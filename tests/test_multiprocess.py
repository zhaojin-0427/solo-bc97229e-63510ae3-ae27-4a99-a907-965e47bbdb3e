"""多 Gunicorn worker(多进程)协调测试。

用真实 OS 进程(fork)模拟两个独立 worker 共用同一 DATA_DIR,验证:

1. 两个进程同时发现同一待处理批次,只有一个推进进度/写确认日志/切换基线;
2. 持锁进程被 SIGKILL 强制终止后,存活进程自动接续持久进度,不重复不丢失;
3. 同一批次的增量确认日志恰好写入一次;
4. flock 在进程终止后由内核释放,新进程立刻可获取;
5. 真实 gunicorn 双 worker 下服务可用,杀掉持锁 worker 后由新 worker 接续。
"""
import multiprocessing as mp
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from urllib import request as urlrequest
from urllib.error import URLError

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app, db, loader, logic          # noqa: E402
from app.locks import BatchGuard, LockUnavailable      # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLES = os.path.join(ROOT, "samples")


# ------------------------------------------------------------------ 工具

def _make_app(data_dir):
    return create_app({
        "TESTING": True,
        "DATA_DIR": data_dir,
        "SAMPLES_DIR": SAMPLES,
        "CHUNK_SIZE": 50,
        "CHUNK_DELAY": 0.0,
    })


def _insert_full_batch(data_dir, filename):
    """登记一个全量批次(等价于 /upload/full 的入库部分),返回批次 id。"""
    path = os.path.join(SAMPLES, filename)
    conn = db.connect(data_dir)
    try:
        cur = conn.execute(
            "INSERT INTO batches (kind, filename, orig_name, status, total_bytes, start_seq)"
            " VALUES ('full', ?, ?, 'loading', ?, ?)",
            (path, filename, os.path.getsize(path), logic.max_increment_seq(conn)))
        bid = cur.lastrowid
    finally:
        conn.close()
    return bid


def _insert_inc_batch(data_dir, csv_text, name="inc.csv"):
    up = os.path.join(data_dir, "uploads")
    os.makedirs(up, exist_ok=True)
    path = os.path.join(up, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(csv_text)
    conn = db.connect(data_dir)
    try:
        cur = conn.execute(
            "INSERT INTO batches (kind, filename, orig_name, status, total_bytes)"
            " VALUES ('inc', ?, ?, 'processing', ?)",
            (path, name, os.path.getsize(path)))
        bid = cur.lastrowid
    finally:
        conn.close()
    return bid


def _result_count(data_dir, include_withdrawn=False):
    conn = db.connect(data_dir)
    try:
        return len(logic.current_results(
            conn, include_withdrawn=include_withdrawn))
    finally:
        conn.close()


# --- 子进程入口(fork 后运行;函数必须可 pickle/顶层可见) ---

def child_process_full(data_dir, bid, ready_q, keep_running):
    """独立 worker:先登记就绪,再循环尝试推进全量批次(非阻塞)。"""
    app = _make_app(data_dir)
    chunk = app.config["CHUNK_SIZE"]
    ready_q.put(os.getpid())
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if not keep_running.value:
            return
        conn = db.connect(data_dir)
        try:
            inc_ids, full_ids = loader.find_pending(conn)
        finally:
            conn.close()
        if bid not in full_ids:
            return  # 批次已完成
        try:
            loader.process_full_batch(data_dir, bid, chunk, blocking=False)
        except loader.LockUnavailable:
            time.sleep(0.05)
        except loader.PauseProcessing:
            return
        time.sleep(0.05)


def child_block_on_lock(data_dir, bid, result_q):
    """独立 worker:阻塞式获取同一批次锁,记录是否拿到。"""
    try:
        with BatchGuard(data_dir, bid, blocking=True, timeout=2):
            result_q.put(("acquired", os.getpid()))
            time.sleep(0.3)
    except LockUnavailable:
        result_q.put(("timeout", os.getpid()))


# ------------------------------------------------------------------ 1. 双进程互斥

def test_two_processes_full_batch_processed_once(tmp_path):
    data_dir = str(tmp_path / "data")
    _make_app(data_dir)  # 初始化 schema
    bid = _insert_full_batch(data_dir, "full_baseline.csv")

    ctx = mp.get_context("fork")
    ready_q = ctx.Queue()
    keep = ctx.Value("i", 1)
    procs = [ctx.Process(target=child_process_full,
                         args=(data_dir, bid, ready_q, keep)) for _ in range(2)]
    for p in procs:
        p.start()
    pids = [ready_q.get(timeout=10) for _ in procs]
    assert len(set(pids)) == 2
    for p in procs:
        p.join(timeout=40)
        assert p.exitcode == 0
    keep.value = 0

    conn = db.connect(data_dir)
    try:
        batch = conn.execute("SELECT * FROM batches WHERE id=?", (bid,)).fetchone()
        staged = conn.execute("SELECT COUNT(*) c FROM results_staging").fetchone()["c"]
    finally:
        conn.close()
    # 恰好装载一次:进度计数无重复累加,暂存区已清空,正式表为 10 条
    assert batch["status"] == "done"
    assert batch["total_rows"] == 10
    assert batch["applied_rows"] == 10
    assert staged == 0
    assert _result_count(data_dir) == 10


def test_flock_excludes_other_process_then_releases(tmp_path):
    data_dir = str(tmp_path / "data")
    _make_app(data_dir)
    bid = 999
    with BatchGuard(data_dir, bid, blocking=False):
        ctx = mp.get_context("fork")
        q = ctx.Queue()
        p = ctx.Process(target=child_block_on_lock, args=(data_dir, bid, q))
        p.start()
        # 持锁期间,另一进程在短超时内拿不到
        outcome, _pid = q.get(timeout=5)
        p.join(timeout=5)
        assert outcome == "timeout"
    # 本进程释放后,新进程应立即拿到锁(flock 随进程/描述符释放)
    ctx = mp.get_context("fork")
    q = ctx.Queue()
    p = ctx.Process(target=child_block_on_lock, args=(data_dir, bid, q))
    p.start()
    outcome, _pid = q.get(timeout=10)
    p.join(timeout=5)
    assert outcome == "acquired"


# ------------------------------------------------------------------ 2. SIGKILL 接管

def test_surviving_process_takes_over_after_sigkill(tmp_path):
    data_dir = str(tmp_path / "data")
    app = _make_app(data_dir)
    bid = _insert_full_batch(data_dir, "full_baseline_large.csv")  # 20000 行

    ctx = mp.get_context("fork")
    ready_q = ctx.Queue()
    keep = ctx.Value("i", 1)
    victim = ctx.Process(target=child_process_full,
                         args=(data_dir, bid, ready_q, keep))
    victim.start()
    victim_pid = ready_q.get(timeout=10)

    # 等待 victim 提交至少一块进度
    conn = db.connect(data_dir)
    try:
        deadline = time.monotonic() + 15
        processed = 0
        while time.monotonic() < deadline:
            processed = conn.execute(
                "SELECT processed_bytes FROM batches WHERE id=?", (bid,)
            ).fetchone()["processed_bytes"]
            if processed > 0:
                break
            time.sleep(0.05)
        assert processed > 0
        staged_before = conn.execute(
            "SELECT COUNT(*) c FROM results_staging").fetchone()["c"]
        assert staged_before > 0
    finally:
        conn.close()

    # 强制终止持锁 worker(模拟 kill -9 / 断电),不做任何清理
    os.kill(victim_pid, signal.SIGKILL)
    victim.join(timeout=10)
    assert not victim.is_alive()

    # 存活 worker 启动:flock 已被内核释放,从持久偏移接续,无需重新上传
    survivor = ctx.Process(target=child_process_full,
                           args=(data_dir, bid, ready_q, keep))
    survivor.start()
    survivor.join(timeout=60)
    assert survivor.exitcode == 0
    keep.value = 0

    conn = db.connect(data_dir)
    try:
        batch = conn.execute("SELECT * FROM batches WHERE id=?", (bid,)).fetchone()
        staged = conn.execute("SELECT COUNT(*) c FROM results_staging").fetchone()["c"]
    finally:
        conn.close()
    assert batch["status"] == "done"
    # 不重复推进:文件 20000 行,计数恰好 20000(若重复处理会远超)
    assert batch["total_rows"] == 20000
    assert batch["applied_rows"] == 20000
    assert staged == 0
    assert _result_count(data_dir) == 20000


def test_sigkill_mid_increment_then_resume_writes_each_log_once(tmp_path):
    data_dir = str(tmp_path / "data")
    app = _make_app(data_dir)
    # 先建基线
    base = _insert_full_batch(data_dir, "full_baseline.csv")
    loader.run_pending(data_dir, app.config["CHUNK_SIZE"])
    assert _result_count(data_dir) == 10

    # 8 行增量,块大小 50 -> 单块;用故障注入在确认前“杀死”子进程
    csv_text = "report_no,item,action,result,revision\n" + "".join(
        f"R2026-000{i % 9 + 1},汞(Hg),correct,9.{i},5\n" for i in range(8))
    bid = _insert_inc_batch(data_dir, csv_text)

    def boom(_idx):
        os.kill(os.getpid(), signal.SIGKILL)

    ctx = mp.get_context("fork")

    def killed_worker():
        a = _make_app(data_dir)
        try:
            loader.process_inc_batch(data_dir, bid, a.config["CHUNK_SIZE"],
                                     chunk_hook=boom)
        except Exception:
            pass

    p = ctx.Process(target=killed_worker)
    p.start()
    p.join(timeout=20)
    assert p.exitcode != 0  # 被信号杀死

    conn = db.connect(data_dir)
    try:
        batch = conn.execute("SELECT * FROM batches WHERE id=?", (bid,)).fetchone()
        logged = conn.execute(
            "SELECT COUNT(*) c FROM increments WHERE batch_id=?", (bid,)
        ).fetchone()["c"]
    finally:
        conn.close()
    # 杀死发生在块事务提交前:批次仍 processing,尚无确认日志
    assert batch["status"] == "processing"
    assert logged == 0

    # 存活 worker 接续确认
    loader.run_pending(data_dir, app.config["CHUNK_SIZE"])
    conn = db.connect(data_dir)
    try:
        batch = conn.execute("SELECT * FROM batches WHERE id=?", (bid,)).fetchone()
        logged = conn.execute(
            "SELECT COUNT(*) c FROM increments WHERE batch_id=?", (bid,)
        ).fetchone()["c"]
        total_log = conn.execute("SELECT COUNT(*) c FROM increments").fetchone()["c"]
    finally:
        conn.close()
    assert batch["status"] == "done"
    assert logged == 8            # 每行恰好确认一次
    assert total_log == 8         # 无重复确认日志


# ------------------------------------------------------------------ 3. 并发增量不重复

def test_concurrent_increment_requests_single_confirmation(tmp_path):
    data_dir = str(tmp_path / "data")
    app = _make_app(data_dir)
    base = _insert_full_batch(data_dir, "full_baseline.csv")
    loader.run_pending(data_dir, app.config["CHUNK_SIZE"])

    csv_text = ("report_no,item,action,result,revision\n"
                "R2026-0001,铅(Pb),correct,0.99,9\n")
    bid = _insert_inc_batch(data_dir, csv_text)

    ctx = mp.get_context("fork")
    q = ctx.Queue()

    def worker():
        a = _make_app(data_dir)
        try:
            loader.process_inc_batch(data_dir, bid, a.config["CHUNK_SIZE"],
                                     blocking=True, lock_timeout=15)
            q.put(("done", os.getpid()))
        except loader.LockUnavailable:
            q.put(("lock-timeout", os.getpid()))

    procs = [ctx.Process(target=worker) for _ in range(2)]
    for p in procs:
        p.start()
    outcomes = [q.get(timeout=30) for _ in procs]
    for p in procs:
        p.join(timeout=10)
        assert p.exitcode == 0

    conn = db.connect(data_dir)
    try:
        logged = conn.execute(
            "SELECT COUNT(*) c FROM increments WHERE batch_id=?", (bid,)
        ).fetchone()["c"]
        row = conn.execute(
            "SELECT result, revision FROM results "
            "WHERE report_no='R2026-0001' AND item='铅(Pb)'").fetchone()
    finally:
        conn.close()
    assert logged == 1                       # 确认日志仅一条
    assert row["result"] == "0.99" and row["revision"] == 9
    assert {o[0] for o in outcomes} <= {"done", "lock-timeout"}


# ------------------------------------------------------------------ 4. 真实 gunicorn 双 worker

def _free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _http_get(base, path, timeout=5):
    with urlrequest.urlopen(base + path, timeout=timeout) as resp:
        import json
        return resp.status, json.loads(resp.read().decode())


def _wait_health(base, timeout=30):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            status, body = _http_get(base, "/api/health")
            if status == 200 and body.get("ok"):
                return body
        except URLError:
            pass
        time.sleep(0.3)
    raise AssertionError(f"服务未在 {timeout}s 内就绪: {last}")


@pytest.mark.skipif(not shutil.which("gunicorn"),
                    reason="环境中未安装 gunicorn")
def test_real_gunicorn_two_workers_and_kill_takeover(tmp_path):
    data_dir = str(tmp_path / "data")
    port = _free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ)
    env.update({
        "DATA_DIR": data_dir,
        "SAMPLES_DIR": SAMPLES,
        "LIMS_CHUNK_SIZE": "200",
        "LIMS_CHUNK_DELAY_MS": "30",
        "LIMS_LOCK_TIMEOUT_S": "30",
        "PYTHONPATH": ROOT,
    })
    proc = subprocess.Popen(
        [sys.executable, "-m", "gunicorn",
         "--workers", "2", "--threads", "2",
         "--bind", f"127.0.0.1:{port}", "--timeout", "60",
         "wsgi:app"],
        cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        _wait_health(base)
        # 多次健康检查应观察到两个不同的 worker pid
        pids = set()
        for _ in range(20):
            _, body = _http_get(base, "/api/health")
            pids.add(body["pid"])
            if len(pids) >= 2:
                break
            time.sleep(0.3)
        assert len(pids) == 2, f"未观察到两个 worker pid: {pids}"

        # 触发大文件全量装载
        req = urlrequest.Request(base + "/load_sample/full_large", method="POST")
        with urlrequest.urlopen(req, timeout=10) as resp:
            assert resp.status == 200

        # 等待出现进行中的批次并定位持锁 worker
        lock_file = None
        deadline = time.monotonic() + 20
        batch_id = None
        while time.monotonic() < deadline:
            _, status = _http_get(base, "/api/status")
            if status.get("full_load_in_progress"):
                batch_id = status["full_load_in_progress"]["id"]
                lock_file = os.path.join(
                    data_dir, "locks", f"batch-{batch_id}.lock")
                if os.path.exists(lock_file):
                    break
            time.sleep(0.3)
        assert lock_file and os.path.exists(lock_file), "未找到批次锁文件"

        with open(lock_file, encoding="utf-8") as f:
            holder_pid = int(f.read().strip().split("=")[1])
        assert holder_pid in pids

        # 确认已有持久进度后,杀掉持锁 worker(轮询等待首块提交)
        conn = db.connect(data_dir)
        try:
            deadline = time.monotonic() + 20
            processed = 0
            while time.monotonic() < deadline:
                processed = conn.execute(
                    "SELECT processed_bytes FROM batches WHERE id=?",
                    (batch_id,)).fetchone()["processed_bytes"]
                if processed > 0:
                    break
                time.sleep(0.2)
        finally:
            conn.close()
        assert processed > 0, "worker 未在预期时间内提交任何装载块"
        os.kill(holder_pid, signal.SIGKILL)

        # gunicorn master 自动拉起新 worker;无需重启服务,装载接续完成
        deadline = time.monotonic() + 90
        final_status = None
        while time.monotonic() < deadline:
            _, st = _http_get(base, "/api/status")
            if not st.get("full_load_in_progress"):
                final_status = st
                break
            time.sleep(0.5)
        assert final_status is not None, "杀死 worker 后装载未接续完成"
        assert final_status["active_results"] == 20000  # 不重复、不丢失、无半批

        _, batches = _http_get(base, "/api/batches")
        b = next(x for x in batches["batches"] if x["id"] == batch_id)
        assert b["status"] == "done" and b["applied_rows"] == 20000
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
