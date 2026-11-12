"""需求覆盖测试:

1. 全量装载为基线,增量立即更新正式表
2. 同报告同项目取最高修订;撤回后旧修订不能恢复
3. 切换合入装载期间全部已确认增量;全量缺失且未被更正的旧项目移除
4. 取数接口读取切换前/后的完整有效结果
5. 进程中断后继续未完成装载,已确认增量不丢失
6. 同一时刻只允许一个全量装载
"""
import io
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app          # noqa: E402
from app import db, loader, logic   # noqa: E402


@pytest.fixture()
def app(tmp_path):
    return create_app({
        "TESTING": True,
        "DATA_DIR": str(tmp_path / "data"),
        "SAMPLES_DIR": os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "samples"),
        "CHUNK_SIZE": 3,          # 小块以便测试多块/断点
        "CHUNK_DELAY": 0.0,
    })


@pytest.fixture()
def client(app):
    return app.test_client()


def csv_upload(client, rows, kind="full", header=None):
    header = header or (["report_no", "item", "result", "revision"] if kind == "full"
                        else ["report_no", "item", "action", "result", "revision"])
    buf = io.StringIO()
    buf.write(",".join(header) + "\n")
    for r in rows:
        buf.write(",".join(str(c) for c in r) + "\n")
    data = {"file": (io.BytesIO(buf.getvalue().encode("utf-8")), "up.csv")}
    return client.post(f"/upload/{kind}", data=data,
                       content_type="multipart/form-data")


def finish_pending(app):
    loader.run_pending(app.config["DATA_DIR"], app.config["CHUNK_SIZE"])


def results_map(app, include_withdrawn=False):
    conn = db.connect(app.config["DATA_DIR"])
    try:
        rows = logic.current_results(conn, include_withdrawn=include_withdrawn)
        return {(r["report_no"], r["item"]): (r["result"], r["revision"], r["active"])
                for r in rows}
    finally:
        conn.close()


def active_map(app):
    return {k: (v[0], v[1]) for k, v in results_map(app).items() if v[2] == 1}


# ---------------------------------------------------------------- 基础装载与增量

def test_full_load_becomes_baseline(app, client):
    r = csv_upload(client, [("R1", "铅", "0.1", 1), ("R1", "镉", "0.2", 2),
                            ("R2", "铅", "0.3", 1)])
    assert r.status_code == 200
    finish_pending(app)
    assert active_map(app) == {("R1", "铅"): ("0.1", 1),
                               ("R1", "镉"): ("0.2", 2),
                               ("R2", "铅"): ("0.3", 1)}
    b = client.get("/api/batches").get_json()["batches"][0]
    assert b["status"] == "done" and b["applied_rows"] == 3


def test_full_file_duplicate_keys_keep_max_revision(app, client):
    csv_upload(client, [("R1", "铅", "0.1", 1), ("R1", "铅", "0.5", 3),
                        ("R1", "铅", "0.4", 2)])
    finish_pending(app)
    assert active_map(app) == {("R1", "铅"): ("0.5", 3)}


def test_increment_applies_immediately_and_max_revision(app, client):
    csv_upload(client, [("R1", "铅", "0.1", 2)])
    finish_pending(app)
    # 更高修订更正 -> 生效
    r = csv_upload(client, [("R1", "铅", "correct", "0.9", 3)], kind="inc")
    assert r.status_code == 200
    assert active_map(app)[("R1", "铅")] == ("0.9", 3)   # 请求返回时已生效
    # 更低/相同修订 -> 忽略
    csv_upload(client, [("R1", "铅", "correct", "0.5", 2)], kind="inc")
    csv_upload(client, [("R1", "铅", "correct", "0.55", 3)], kind="inc")
    assert active_map(app)[("R1", "铅")] == ("0.9", 3)
    incs = client.get("/api/increments").get_json()["increments"]
    effects = {i["revision"]: i["effect"] for i in incs}
    assert effects[3] == "applied" and effects[2] == "stale"


def test_withdraw_then_old_revision_cannot_restore(app, client):
    csv_upload(client, [("R1", "铅", "0.1", 2)])
    finish_pending(app)
    csv_upload(client, [("R1", "铅", "withdraw", "", 3)], kind="inc")
    assert ("R1", "铅") not in active_map(app)            # 已撤回
    csv_upload(client, [("R1", "铅", "correct", "0.2", 2)], kind="inc")  # 旧修订
    assert ("R1", "铅") not in active_map(app)            # 不能恢复
    csv_upload(client, [("R1", "铅", "correct", "0.2", 3)], kind="inc")  # 等于水位也不行
    assert ("R1", "铅") not in active_map(app)
    csv_upload(client, [("R1", "铅", "correct", "0.2", 4)], kind="inc")  # 更高修订
    assert active_map(app)[("R1", "铅")] == ("0.2", 4)    # 可以(新修订)


def test_withdraw_unknown_item_blocks_future_stale_correct(app, client):
    csv_upload(client, [("R9", "汞", "withdraw", "", 5)], kind="inc")   # 无此项目
    csv_upload(client, [("R9", "汞", "correct", "1.0", 4)], kind="inc")
    assert ("R9", "汞") not in active_map(app)
    csv_upload(client, [("R9", "汞", "correct", "1.0", 6)], kind="inc")
    assert active_map(app)[("R9", "汞")] == ("1.0", 6)


# ---------------------------------------------------------------- 切换语义

def test_switch_merges_increments_and_prunes(app, client):
    # 基线 v1
    csv_upload(client, [("A", "铅", "1", 1), ("B", "镉", "2", 1), ("C", "汞", "3", 1)])
    finish_pending(app)
    # 新全量(仅含 A,且 A 修订号 1),分多块装载
    r = csv_upload(client, [("A", "铅", "1.0", 1), ("D", "砷", "4", 1),
                            ("E", "铅", "5", 1), ("F", "铅", "6", 1)])
    assert r.status_code == 200
    # 装载进行中:增量立即更新现有正式表
    csv_upload(client, [("A", "铅", "correct", "1.1", 2),
                        ("B", "镉", "correct", "2.2", 2),     # B 不在新全量中但被更正
                        ("D", "砷", "withdraw", "", 1)],       # D 在新全量中但被撤回
                       kind="inc")
    mid = active_map(app)
    assert mid[("A", "铅")] == ("1.1", 2) and mid[("B", "镉")] == ("2.2", 2)
    finish_pending(app)  # 切换
    final = active_map(app)
    assert final[("A", "铅")] == ("1.1", 2)      # 期间增量合入(高于全量修订)
    assert final[("B", "镉")] == ("2.2", 2)      # 全量缺失但被增量更正 -> 保留
    assert ("C", "汞") not in final              # 全量缺失且未被更正 -> 移除
    assert ("D", "砷") not in final              # 期间撤回 -> 不进入新基线
    assert final[("E", "铅")] == ("5", 1) and final[("F", "铅")] == ("6", 1)


def test_increment_with_higher_revision_beats_full_file(app, client):
    csv_upload(client, [("A", "铅", "1", 1)])
    finish_pending(app)
    csv_upload(client, [("A", "铅", "9.9", 5)])          # 新全量 rev5,开始装载
    csv_upload(client, [("A", "铅", "correct", "7.7", 9)], kind="inc")  # 装载期间 rev9
    finish_pending(app)
    assert active_map(app)[("A", "铅")] == ("7.7", 9)


def test_full_file_lower_revision_loses_to_during_load_increment(app, client):
    csv_upload(client, [("A", "铅", "1", 1)])
    finish_pending(app)
    csv_upload(client, [("A", "铅", "9.9", 5)])          # 全量 rev5
    csv_upload(client, [("A", "铅", "correct", "7.7", 9)], kind="inc")  # 期间 rev9
    finish_pending(app)
    assert active_map(app)[("A", "铅")] == ("7.7", 9)


def test_pre_load_increment_expected_in_full_export(app, client):
    """边界语义:装载开始前确认的增量应已包含在全量导出中;
    切换只合入“装载期间”的增量,新基线以全量文件为准。"""
    csv_upload(client, [("A", "铅", "1", 1)])
    finish_pending(app)
    csv_upload(client, [("A", "铅", "correct", "7.7", 9)], kind="inc")  # 装载前确认
    assert active_map(app)[("A", "铅")] == ("7.7", 9)
    csv_upload(client, [("A", "铅", "9.9", 5)])          # 全量导出未含该增量
    finish_pending(app)
    assert active_map(app)[("A", "铅")] == ("9.9", 5)    # 新基线以全量为准


def test_tombstone_carries_across_baseline_switch(app, client):
    csv_upload(client, [("X", "铅", "1", 1), ("Y", "镉", "2", 1)])
    finish_pending(app)
    csv_upload(client, [("X", "铅", "withdraw", "", 2),
                        ("Y", "镉", "withdraw", "", 2)], kind="inc")
    # 新全量:X 给旧修订 1(<=撤回水位2) -> 仍撤回;Y 给新修订 3 -> 恢复
    csv_upload(client, [("X", "铅", "1.1", 1), ("Y", "镉", "2.2", 3)])
    finish_pending(app)
    final = active_map(app)
    assert ("X", "铅") not in final
    assert final[("Y", "镉")] == ("2.2", 3)
    # 切换后旧修订更正仍不能复活 X
    csv_upload(client, [("X", "铅", "correct", "9", 2)], kind="inc")
    assert ("X", "铅") not in active_map(app)


# ---------------------------------------------------------------- 断点续传与恢复

def test_crash_during_full_load_resumes_without_losing_increments(app, client, tmp_path):
    rows = [(f"R{i}", "铅", f"0.{i}", 1) for i in range(1, 11)]  # 10 行,块大小 3
    csv_upload(client, rows)
    data_dir = app.config["DATA_DIR"]
    # 处理 1 块后“进程崩溃”(暂停)
    loader.run_pending(data_dir, 3, max_chunks=1)
    conn = db.connect(data_dir)
    staged = conn.execute("SELECT COUNT(*) c FROM results_staging").fetchone()["c"]
    assert staged == 3
    # 崩溃前确认的增量
    conn.close()
    csv_upload(client, [("R1", "铅", "correct", "9.9", 2)], kind="inc")
    # 模拟重启:新建应用(同数据目录),恢复并继续
    app2 = create_app({"TESTING": True, "DATA_DIR": data_dir,
                       "SAMPLES_DIR": app.config["SAMPLES_DIR"],
                       "CHUNK_SIZE": 3, "CHUNK_DELAY": 0.0})
    loader.run_pending(app2.config["DATA_DIR"], 3)
    final = active_map(app2)
    assert len(final) == 10
    assert final[("R1", "铅")] == ("9.9", 2)   # 崩溃前确认的增量未丢失且已合入


def test_crash_mid_chunk_rolls_back_and_reprocesses(app, client):
    rows = [(f"R{i}", "铅", f"0.{i}", 1) for i in range(1, 8)]
    csv_upload(client, rows)
    data_dir = app.config["DATA_DIR"]

    calls = {"n": 0}

    def boom(_chunk_idx):
        calls["n"] += 1
        raise RuntimeError("simulated power loss")

    with pytest.raises(RuntimeError):
        loader.run_pending(data_dir, 3, chunk_hook=boom)
    # 崩溃后:批次仍处于装载中,已提交块数为零(首块事务已回滚)
    conn = db.connect(data_dir)
    b = conn.execute("SELECT * FROM batches WHERE kind='full'").fetchone()
    staged = conn.execute("SELECT COUNT(*) c FROM results_staging").fetchone()["c"]
    assert b["status"] == "loading" and staged == 0
    conn.close()
    # 重启恢复:完整装载
    loader.run_pending(data_dir, 3)
    assert len(active_map(app)) == 7


def test_crash_between_staging_and_switch_redoes_merge(app, client):
    csv_upload(client, [("A", "铅", "1", 1)])
    finish_pending(app)
    csv_upload(client, [("A", "铅", "2", 2), ("B", "铅", "3", 1)])   # 新全量开始装载
    csv_upload(client, [("B", "铅", "correct", "3.3", 2)], kind="inc")  # 装载期间确认
    data_dir = app.config["DATA_DIR"]

    # 在合并事务提交前“断电”:直接注入故障到 merge_and_switch
    from app import logic as logic_mod
    orig = logic_mod.merge_and_switch

    def boom(conn, batch_id):
        raise RuntimeError("power loss during switch")
    logic_mod.merge_and_switch = boom
    try:
        with pytest.raises(RuntimeError):
            loader.run_pending(data_dir, 500)
    finally:
        logic_mod.merge_and_switch = orig
    # 事务已回滚:正式表仍是旧基线 + 已立即应用的期间增量,批次待切换
    assert active_map(app) == {("A", "铅"): ("1", 1), ("B", "铅"): ("3.3", 2)}
    conn = db.connect(data_dir)
    assert conn.execute("SELECT status FROM batches WHERE kind='full' ORDER BY id DESC"
                        ).fetchone()["status"] == "staging_ready"
    conn.close()
    # 重启后重做合并切换,期间增量照样合入
    loader.run_pending(data_dir, 500)
    final = active_map(app)
    assert final == {("A", "铅"): ("2", 2), ("B", "铅"): ("3.3", 2)}


def test_crash_mid_increment_resume_no_duplicates(app, client):
    csv_upload(client, [("A", "铅", "1", 1)])
    finish_pending(app)
    rows = [(f"N{i}", "铅", "correct", f"0.{i}", 1) for i in range(1, 8)]
    csv_upload(client, rows, kind="inc")   # 同步处理,块大小 3 -> 全部完成
    # 再上传一批,但用故障注入模拟确认途中崩溃
    rows2 = [(f"M{i}", "镉", "correct", f"1.{i}", 1) for i in range(1, 8)]
    buf = "report_no,item,action,result,revision\n" + \
          "".join(",".join(map(str, r)) + "\n" for r in rows2)
    data = {"file": (io.BytesIO(buf.encode()), "inc2.csv")}
    # 手动建批次以控制处理过程
    up_dir = os.path.join(app.config["DATA_DIR"], "uploads")
    os.makedirs(up_dir, exist_ok=True)
    path = os.path.join(up_dir, "inc2.csv")
    with open(path, "w", encoding="utf-8") as f:
        f.write(buf)
    conn = db.connect(app.config["DATA_DIR"])
    conn.execute("INSERT INTO batches (kind, filename, orig_name, status, total_bytes)"
                 " VALUES ('inc', ?, 'inc2.csv', 'processing', ?)",
                 (path, os.path.getsize(path)))
    bid = conn.execute("SELECT MAX(id) i FROM batches").fetchone()["i"]
    conn.close()

    def boom(idx):
        if idx == 1:
            raise RuntimeError("crash mid increment")
    with pytest.raises(RuntimeError):
        loader.run_pending(app.config["DATA_DIR"], 3, chunk_hook=boom)
    seq_before = logic.max_increment_seq(db.connect(app.config["DATA_DIR"]))
    # 恢复继续
    loader.run_pending(app.config["DATA_DIR"], 3)
    conn = db.connect(app.config["DATA_DIR"])
    n_log = conn.execute("SELECT COUNT(*) c FROM increments WHERE batch_id=?",
                         (bid,)).fetchone()["c"]
    assert n_log == 7                      # 每行恰好确认一次
    assert logic.max_increment_seq(conn) == seq_before + (7 - 3)  # 只补未确认的
    conn.close()
    final = active_map(app)
    for i in range(1, 8):
        assert final[(f"M{i}", "镉")] == (f"1.{i}", 1)


# ---------------------------------------------------------------- 单装载约束

def test_only_one_full_load_at_a_time(app, client):
    assert csv_upload(client, [("A", "铅", "1", 1)]).status_code == 200
    r = csv_upload(client, [("B", "铅", "2", 1)])
    assert r.status_code == 409
    assert "全量装载" in r.get_json()["error"]
    finish_pending(app)
    assert csv_upload(client, [("B", "铅", "2", 1)]).status_code == 200


def test_failed_load_allows_retry(app, client):
    bad = {"file": (io.BytesIO("wrong,header\n1,2\n".encode()), "bad.csv")}
    r = client.post("/upload/full", data=bad, content_type="multipart/form-data")
    assert r.status_code == 200  # 批次已受理,后台发现格式错误
    loader.run_pending(app.config["DATA_DIR"], 3)
    b = client.get("/api/batches").get_json()["batches"][0]
    assert b["status"] == "failed" and "表头" in (b["error"] or "")
    assert csv_upload(client, [("A", "铅", "1", 1)]).status_code == 200


# ---------------------------------------------------------------- 取数接口

def test_results_api_reads_consistent_active_set(app, client):
    csv_upload(client, [("R1", "铅", "0.1", 1), ("R2", "镉", "0.2", 1)])
    finish_pending(app)
    csv_upload(client, [("R2", "镉", "withdraw", "", 1)], kind="inc")
    d = client.get("/api/results").get_json()
    assert d["count"] == 1 and d["results"][0]["report_no"] == "R1"
    d2 = client.get("/api/results?all=1").get_json()
    assert d2["count"] == 2
    wd = [r for r in d2["results"] if r["report_no"] == "R2"][0]
    assert wd["active"] is False and wd["withdrawn_rev"] == 1
    d3 = client.get("/api/results?report_no=R1").get_json()
    assert d3["count"] == 1


def test_results_during_load_are_old_baseline(app, client):
    csv_upload(client, [("OLD", "铅", "1", 1)])
    finish_pending(app)
    csv_upload(client, [(f"N{i}", "铅", "2", 1) for i in range(9)])
    # 只处理一块,装载未完成
    loader.run_pending(app.config["DATA_DIR"], 3, max_chunks=1)
    d = client.get("/api/results").get_json()
    assert d["count"] == 1 and d["results"][0]["report_no"] == "OLD"
    finish_pending(app)
    d2 = client.get("/api/results").get_json()
    assert d2["count"] == 9


# ---------------------------------------------------------------- 格式与兼容性

def test_chinese_headers_and_actions(app, client):
    buf = "报告号,检测项目,结果,修订号\nC1,铅,0.1,1\n"
    client.post("/upload/full",
                data={"file": (io.BytesIO(buf.encode()), "c.csv")},
                content_type="multipart/form-data")
    finish_pending(app)
    buf2 = "报告号,检测项目,动作,结果,修订号\nC1,铅,更正,0.2,2\nC1,铅,撤回,,3\n"
    client.post("/upload/inc",
                data={"file": (io.BytesIO(buf2.encode()), "c2.csv")},
                content_type="multipart/form-data")
    assert ("C1", "铅") not in active_map(app)


def test_invalid_rows_counted_not_fatal(app, client):
    csv_upload(client, [("R1", "铅", "0.1", 1), ("R2", "镉", "0.2", 0),  # 修订号非法
                        ("R3", "汞", "0.3", "abc"), ("R4", "砷", "0.4", 2)])
    finish_pending(app)
    m = active_map(app)
    assert ("R1", "铅") in m and ("R4", "砷") in m and len(m) == 2
    b = client.get("/api/batches").get_json()["batches"][0]
    assert b["error_rows"] == 2 and b["applied_rows"] == 2


def test_sample_files_end_to_end(app, client):
    """随附示例文件的完整演示流程。"""
    assert client.post("/load_sample/full_baseline").status_code == 200
    finish_pending(app)
    assert len(active_map(app)) == 10
    client.post("/load_sample/inc_corrections")
    m = active_map(app)
    assert m[("R2026-0001", "铅(Pb)")] == ("0.10", 2)
    assert m[("R2026-0002", "铅(Pb)")] == ("0.20", 2)   # 旧修订更正被忽略
    assert m[("R2026-0009", "铅(Pb)")] == ("0.33", 1)
    client.post("/load_sample/inc_withdrawals")
    m = active_map(app)
    assert ("R2026-0005", "镉(Cd)") not in m and ("R2026-0004", "砷(As)") not in m
    client.post("/load_sample/inc_after_withdraw")
    m = active_map(app)
    assert m[("R2026-0005", "镉(Cd)")] == ("0.09", 4)   # 新修订恢复,旧修订被拒
    # 基线 v2 切换
    client.post("/load_sample/full_baseline_v2")
    finish_pending(app)
    m = active_map(app)
    assert ("R2026-0006", "汞(Hg)") not in m            # 全量缺失 -> 移除
    assert ("R2026-0007", "砷(As)") not in m
    assert ("R2026-0004", "砷(As)") not in m            # 墓碑承继:旧修订不复活
    assert m[("R2026-0008", "铅(Pb)")] == ("0.095", 2)  # 新修订合法恢复
    assert m[("R2026-0005", "镉(Cd)")] == ("0.09", 4)
