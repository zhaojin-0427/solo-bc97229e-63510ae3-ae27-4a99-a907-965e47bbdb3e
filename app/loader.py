"""后台装载与恢复。

- 全量:后台线程分块读 CSV 写入暂存区,每块与进度(文件偏移)同事务提交,
  进程崩溃重启后从偏移处续装;装载完成后合并切换为新基线。
- 增量:上传请求内同步确认(立即更新正式表);若进程在确认途中崩溃,
  重启后由恢复流程从未完成偏移继续,已确认增量因日志持久化而不丢失。
- 全程只允许一个全量装载:数据库部分唯一索引强制 + 进程内认领锁。
"""
import os
import threading
import time

from . import db, logic

_claim_lock = threading.Lock()
_claimed = set()          # 进程内已认领的批次 id
_worker_lock = threading.Lock()
_worker = None            # 后台线程
_recovery_started = False


class PauseProcessing(Exception):
    """测试用:达到 max_chunks 后暂停,保持批次未完成。"""


def try_claim(batch_id):
    with _claim_lock:
        if batch_id in _claimed:
            return False
        _claimed.add(batch_id)
        return True


def release(batch_id):
    with _claim_lock:
        _claimed.discard(batch_id)


def is_claimed(batch_id):
    with _claim_lock:
        return batch_id in _claimed


# ------------------------------------------------------------------ 全量

def process_full_batch(conn, batch_id, chunk_size, chunk_delay=0.0,
                       max_chunks=None, chunk_hook=None):
    """处理一个全量批次直至完成(或 PauseProcessing)。可重入:从记录偏移继续。"""
    if not try_claim(batch_id):
        return False
    try:
        while True:
            batch = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
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
                        " updated_at=datetime('now') WHERE id=?", (offset, size, batch_id))
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
        release(batch_id)


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

def process_inc_batch(conn, batch_id, chunk_size, max_chunks=None, chunk_hook=None):
    """确认一个增量批次(同步或恢复调用)。每块一个事务:日志+正式表+进度。"""
    if not try_claim(batch_id):
        return False
    try:
        batch = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if batch is None or batch["kind"] != "inc" or batch["status"] != "processing":
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
        release(batch_id)


# ------------------------------------------------------------------ 调度与恢复

def find_pending(conn):
    """返回需要继续处理的批次 id(先增量后全量)。"""
    inc = conn.execute(
        "SELECT id FROM batches WHERE kind='inc' AND status='processing' ORDER BY id").fetchall()
    full = conn.execute(
        "SELECT id FROM batches WHERE kind='full' AND status IN ('loading','staging_ready')"
        " ORDER BY id").fetchall()
    return [r["id"] for r in inc], [r["id"] for r in full]


def run_pending(data_dir, chunk_size, chunk_delay=0.0, max_chunks=None, chunk_hook=None):
    """同步执行所有待处理工作(恢复/测试用)。返回处理过的批次。

    CSV 格式错误属于确定性错误:标记批次失败并继续处理其余批次;
    其他异常(如模拟断电)原样抛出,保持批次未完成以便恢复。
    """
    conn = db.connect(data_dir)
    done = []
    try:
        while True:
            inc_ids, full_ids = find_pending(conn)
            if not inc_ids and not full_ids:
                break
            try:
                for bid in inc_ids:
                    if process_inc_batch(conn, bid, chunk_size,
                                         max_chunks=max_chunks, chunk_hook=chunk_hook):
                        done.append(("inc", bid))
                for bid in full_ids:
                    if process_full_batch(conn, bid, chunk_size, chunk_delay,
                                          max_chunks=max_chunks, chunk_hook=chunk_hook):
                        done.append(("full", bid))
            except logic.CsvFormatError as exc:
                for bid in inc_ids + full_ids:
                    mark_failed(data_dir, bid, exc)
            except PauseProcessing:
                break
            if max_chunks is not None:
                break
    finally:
        conn.close()
    return done


def mark_failed(data_dir, batch_id, message):
    conn = db.connect(data_dir)
    try:
        conn.execute("UPDATE batches SET status='failed', error=?,"
                     " updated_at=datetime('now') WHERE id=?", (str(message), batch_id))
    finally:
        conn.close()


def _background_loop(app):
    data_dir = app.config["DATA_DIR"]
    chunk_size = app.config["CHUNK_SIZE"]
    chunk_delay = app.config["CHUNK_DELAY"]
    while True:
        conn = db.connect(data_dir)
        try:
            inc_ids, full_ids = find_pending(conn)
            pending = [("inc", i) for i in inc_ids] + [("full", i) for i in full_ids]
        finally:
            conn.close()
        if not pending:
            return
        kind, bid = pending[0]
        if is_claimed(bid):          # 正由请求线程同步处理,稍后再看
            time.sleep(0.2)
            continue
        conn = db.connect(data_dir)
        try:
            if kind == "inc":
                process_inc_batch(conn, bid, chunk_size)
            else:
                process_full_batch(conn, bid, chunk_size, chunk_delay)
        except Exception as exc:     # 单批失败不拖垮其他批次
            app.logger.exception("批次 %s 处理失败", bid)
            mark_failed(data_dir, bid, exc)
        finally:
            conn.close()


def poke_background(app):
    """上传后唤醒后台线程(幂等)。"""
    global _worker
    with _worker_lock:
        if _worker is not None and _worker.is_alive():
            return
        _worker = threading.Thread(target=_background_loop, args=(app,),
                                   name="lims-loader", daemon=True)
        _worker.start()


def start_recovery_thread(app):
    """应用启动时调用:继续上次进程未完成的工作(每进程仅一次)。"""
    global _recovery_started
    if app.config.get("TESTING"):
        return
    with _worker_lock:
        if _recovery_started:
            return
        _recovery_started = True
    poke_background(app)
