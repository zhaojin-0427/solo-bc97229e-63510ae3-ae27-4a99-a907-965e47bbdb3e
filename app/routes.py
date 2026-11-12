"""HTTP 接口:上传、进度、取数、页面。"""
import os
import sqlite3
import uuid

from flask import (Blueprint, current_app, jsonify, render_template, request)

from . import db, loader, logic
from .loader import LockUnavailable

bp = Blueprint("main", __name__)

SAMPLE_FILES = {
    "full_baseline": "full_baseline.csv",
    "full_baseline_v2": "full_baseline_v2.csv",
    "full_large": "full_baseline_large.csv",
    "inc_corrections": "inc_corrections.csv",
    "inc_withdrawals": "inc_withdrawals.csv",
    "inc_after_withdraw": "inc_after_withdraw.csv",
    "inc_during_load": "inc_during_load.csv",
}


def _err(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code


def _save_upload(file_storage):
    uploads = os.path.join(current_app.config["DATA_DIR"], "uploads")
    os.makedirs(uploads, exist_ok=True)
    orig = file_storage.filename or "upload.csv"
    path = os.path.join(uploads, f"{uuid.uuid4().hex}.csv")
    file_storage.save(path)
    return path, orig


def _create_full_batch(path, orig):
    """登记全量批次;数据库部分唯一索引保证同一时刻仅一个装载。"""
    conn = db.get_db()
    size = os.path.getsize(path)
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM results_staging")  # 清掉上次失败残留的暂存区
        cur = conn.execute(
            "INSERT INTO batches (kind, filename, orig_name, status, total_bytes, start_seq)"
            " VALUES ('full', ?, ?, 'loading', ?, ?)",
            (path, orig, size, logic.max_increment_seq(conn)))
        conn.execute("COMMIT")
        return cur.lastrowid
    except sqlite3.IntegrityError:
        conn.execute("ROLLBACK")
        raise
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def _create_inc_batch(path, orig):
    conn = db.get_db()
    size = os.path.getsize(path)
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "INSERT INTO batches (kind, filename, orig_name, status, total_bytes)"
            " VALUES ('inc', ?, ?, 'processing', ?)", (path, orig, size))
        conn.execute("COMMIT")
        return cur.lastrowid
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def _batch_json(row):
    total = row["total_bytes"] or 0
    done = row["processed_bytes"] or 0
    return {
        "id": row["id"], "kind": row["kind"], "status": row["status"],
        "orig_name": row["orig_name"], "error": row["error"],
        "total_rows": row["total_rows"], "applied_rows": row["applied_rows"],
        "error_rows": row["error_rows"],
        "progress": round(100.0 * done / total, 1) if total else 0.0,
        "created_at": row["created_at"], "updated_at": row["updated_at"],
    }


# ------------------------------------------------------------------ 页面

def _poke(app):
    """唤醒本 worker 的常驻监控线程;测试模式下由测试用例手动驱动装载。"""
    if not app.config.get("TESTING"):
        loader.poke_background(app)


@bp.get("/")
def index():
    return render_template("index.html", samples=sorted(SAMPLE_FILES.keys()))


# ------------------------------------------------------------------ 上传

@bp.post("/upload/full")
def upload_full():
    f = request.files.get("file")
    if f is None or not f.filename:
        return _err("请选择要上传的全量 CSV 文件")
    path, orig = _save_upload(f)
    try:
        batch_id = _create_full_batch(path, orig)
    except sqlite3.IntegrityError:
        os.unlink(path)
        return _err("已有一个全量装载正在进行,请等待其完成后再上传", 409)
    _poke(current_app._get_current_object())
    return jsonify({"ok": True, "batch_id": batch_id,
                    "message": f"全量批次 #{batch_id} 已开始后台装载"})


@bp.post("/upload/inc")
def upload_inc():
    f = request.files.get("file")
    if f is None or not f.filename:
        return _err("请选择要上传的增量 CSV 文件")
    path, orig = _save_upload(f)
    batch_id = _create_inc_batch(path, orig)
    try:
        # 增量在请求内同步确认:响应返回时已立即更新正式表。
        # 多 worker 下若另一进程正持有该批次锁,阻塞等待其完成后复查跳过,
        # 保证同一批次只确认一次;等待超时则让客户端重试。
        loader.process_inc_batch(
            current_app.config["DATA_DIR"], batch_id,
            current_app.config["CHUNK_SIZE"],
            lock_timeout=current_app.config["LOCK_TIMEOUT"])
    except LockUnavailable:
        return _err("该增量批次正由其他 worker 处理,请稍后重试", 503)
    except logic.CsvFormatError as exc:
        loader.mark_failed(current_app.config["DATA_DIR"], batch_id, exc)
        return _err(f"增量文件格式错误: {exc}")
    batch = db.get_db().execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
    return jsonify({"ok": True, "batch": _batch_json(batch),
                    "message": f"增量批次 #{batch_id} 已确认并应用到正式结果表"})


@bp.post("/load_sample/<name>")
def load_sample(name):
    """一键载入随附示例文件(演示装载中更正/撤回)。"""
    if name not in SAMPLE_FILES:
        return _err("未知示例", 404)
    path = os.path.join(current_app.config["SAMPLES_DIR"], SAMPLE_FILES[name])
    if not os.path.exists(path):
        return _err(f"示例文件不存在: {SAMPLE_FILES[name]}", 404)
    orig = f"sample:{SAMPLE_FILES[name]}"
    if name.startswith("full"):
        try:
            batch_id = _create_full_batch(path, orig)
        except sqlite3.IntegrityError:
            return _err("已有一个全量装载正在进行,请等待其完成", 409)
        _poke(current_app._get_current_object())
        return jsonify({"ok": True, "batch_id": batch_id,
                        "message": f"示例全量批次 #{batch_id} 已开始后台装载"})
    batch_id = _create_inc_batch(path, orig)
    try:
        loader.process_inc_batch(
            current_app.config["DATA_DIR"], batch_id,
            current_app.config["CHUNK_SIZE"],
            lock_timeout=current_app.config["LOCK_TIMEOUT"])
    except LockUnavailable:
        return _err("该增量批次正由其他 worker 处理,请稍后重试", 503)
    except logic.CsvFormatError as exc:
        loader.mark_failed(current_app.config["DATA_DIR"], batch_id, exc)
        return _err(f"增量文件格式错误: {exc}")
    batch = db.get_db().execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
    return jsonify({"ok": True, "batch": _batch_json(batch),
                    "message": f"示例增量批次 #{batch_id} 已确认并应用"})


# ------------------------------------------------------------------ 取数与进度

@bp.get("/api/results")
def api_results():
    """报告系统取数接口:单条 SELECT,保证每次读到切换前或后的完整有效结果。"""
    conn = db.get_db()
    report_no = request.args.get("report_no") or None
    include_withdrawn = request.args.get("all") == "1"
    rows = logic.current_results(conn, report_no=report_no,
                                 include_withdrawn=include_withdrawn)
    results = [{
        "report_no": r["report_no"], "item": r["item"], "result": r["result"],
        "revision": r["revision"],
        **({"active": bool(r["active"]), "withdrawn_rev": r["withdrawn_rev"]}
           if include_withdrawn else {}),
    } for r in rows]
    return jsonify({"count": len(results), "results": results})


@bp.get("/api/batches")
def api_batches():
    rows = db.get_db().execute(
        "SELECT * FROM batches ORDER BY id DESC LIMIT 50").fetchall()
    return jsonify({"batches": [_batch_json(r) for r in rows]})


@bp.get("/api/increments")
def api_increments():
    limit = min(int(request.args.get("limit", 50)), 500)
    rows = db.get_db().execute(
        "SELECT * FROM increments ORDER BY seq DESC LIMIT ?", (limit,)).fetchall()
    return jsonify({"increments": [dict(r) for r in rows]})


@bp.get("/api/status")
def api_status():
    conn = db.get_db()
    active = conn.execute(
        "SELECT COUNT(*) AS c FROM results WHERE active=1").fetchone()["c"]
    withdrawn = conn.execute(
        "SELECT COUNT(*) AS c FROM results WHERE active=0").fetchone()["c"]
    inc = conn.execute("SELECT COUNT(*) AS c FROM increments").fetchone()["c"]
    loading = conn.execute(
        "SELECT id, status FROM batches WHERE kind='full'"
        " AND status IN ('loading','staging_ready')").fetchone()
    return jsonify({
        "active_results": active, "withdrawn_records": withdrawn,
        "confirmed_increments": inc,
        "full_load_in_progress": dict(loading) if loading else None,
    })


@bp.get("/api/health")
def api_health():
    # 连续多次调用可观察到响应来自不同 worker 进程(多 worker 演示)
    return jsonify({"ok": True, "pid": os.getpid()})
