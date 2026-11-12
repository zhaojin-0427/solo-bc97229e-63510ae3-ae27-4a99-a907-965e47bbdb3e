"""核心领域逻辑:修订号裁决、撤回墓碑、基线合并切换。

规则(对同一 报告号+检测项目):
  - 更正:仅当 新修订号 > 当前生效修订号 且 > 已撤回修订号 时生效;
    即“同报告同项目取最高修订”,且“撤回后旧修订不能恢复结果”。
  - 撤回:记录 withdrawn_rev = max(撤回修订号);若撤回修订号 >= 当前
    生效修订号,则结果失效(墓碑)。墓碑跨基线保留,防止旧修订复活。
"""
import csv

CORRECT = "correct"
WITHDRAW = "withdraw"

ACTION_ALIASES = {
    "correct": CORRECT, "c": CORRECT, "更正": CORRECT,
    "withdraw": WITHDRAW, "w": WITHDRAW, "撤回": WITHDRAW,
}

HEADER_ALIASES = {
    "report_no": "report_no", "报告号": "report_no",
    "item": "item", "检测项目": "item",
    "result": "result", "结果": "result",
    "revision": "revision", "修订号": "revision",
    "action": "action", "动作": "action",
}

FULL_COLUMNS = ("report_no", "item", "result", "revision")
INC_COLUMNS = ("report_no", "item", "action", "result", "revision")


class CsvFormatError(Exception):
    """CSV 结构(表头)非法。"""


# ---------------------------------------------------------------- CSV 解析

def normalize_header(raw_header):
    header = []
    for cell in raw_header:
        key = HEADER_ALIASES.get((cell or "").strip().lower()) \
            or HEADER_ALIASES.get((cell or "").strip())
        if key is None:
            raise CsvFormatError(f"无法识别的表头列: {cell!r}")
        header.append(key)
    return header


def parse_full_row(header, row):
    """返回 (report_no, item, result, revision) 或抛出 ValueError。"""
    rec = dict(zip(header, row + [""] * (len(header) - len(row))))
    report_no = (rec.get("report_no") or "").strip()
    item = (rec.get("item") or "").strip()
    result = (rec.get("result") or "").strip()
    revision = _parse_revision(rec.get("revision"))
    if not report_no or not item:
        raise ValueError("报告号/检测项目不能为空")
    if result == "":
        raise ValueError("结果不能为空")
    return report_no, item, result, revision


def parse_inc_row(header, row):
    """返回 (report_no, item, action, result, revision) 或抛出 ValueError。"""
    rec = dict(zip(header, row + [""] * (len(header) - len(row))))
    report_no = (rec.get("report_no") or "").strip()
    item = (rec.get("item") or "").strip()
    result = (rec.get("result") or "").strip()
    revision = _parse_revision(rec.get("revision"))
    action = ACTION_ALIASES.get((rec.get("action") or "").strip().lower()) \
        or ACTION_ALIASES.get((rec.get("action") or "").strip())
    if not report_no or not item:
        raise ValueError("报告号/检测项目不能为空")
    if action is None:
        raise ValueError(f"未知动作: {rec.get('action')!r}(应为 更正/correct 或 撤回/withdraw)")
    if action == CORRECT and result == "":
        raise ValueError("更正动作必须给出结果值")
    return report_no, item, action, result, revision


def _parse_revision(raw):
    s = (raw or "").strip()
    try:
        rev = int(s)
    except ValueError:
        raise ValueError(f"修订号必须为正整数: {s!r}")
    if rev < 1:
        raise ValueError(f"修订号必须为正整数: {s!r}")
    return rev


def csv_line_reader(f):
    """包装 csv.reader:以 readline 供行,保持 f.tell()/seek() 可用。

    直接迭代文本文件对象会触发预读缓冲并禁用 tell(),而断点续传
    依赖每行处理后的文件偏移,因此必须用 readline 驱动。
    """
    def gen():
        while True:
            line = f.readline()
            if line == "":
                return
            yield line
    return csv.reader(gen())


def read_header(f):
    """读取并校验表头,返回 (规范化表头, 表头后的文件偏移)。"""
    reader = csv_line_reader(f)
    try:
        raw = next(reader)
    except StopIteration:
        raise CsvFormatError("空文件:缺少表头")
    header = normalize_header(raw)
    return header, f.tell()


# ------------------------------------------------------------ 单行应用(可作用于 results 或 results_staging)

def apply_correction(conn, table, report_no, item, result, revision):
    """更正:仅更高修订且高于撤回水位时生效。返回是否生效。"""
    cur = conn.execute(
        f"""
        INSERT INTO {table} (report_no, item, result, revision, withdrawn_rev, active)
        VALUES (?, ?, ?, ?, 0, 1)
        ON CONFLICT(report_no, item) DO UPDATE SET
            result   = excluded.result,
            revision = excluded.revision,
            active   = 1
        WHERE excluded.revision > {table}.revision
          AND excluded.revision > {table}.withdrawn_rev
        """,
        (report_no, item, result, revision),
    )
    return cur.rowcount > 0


def apply_withdrawal(conn, table, report_no, item, revision):
    """撤回:记录撤回水位;水位 >= 当前修订时结果失效(墓碑)。"""
    conn.execute(
        f"""
        INSERT INTO {table} (report_no, item, result, revision, withdrawn_rev, active)
        VALUES (?, ?, NULL, 0, ?, 0)
        ON CONFLICT(report_no, item) DO UPDATE SET
            withdrawn_rev = MAX({table}.withdrawn_rev, excluded.withdrawn_rev),
            active = CASE WHEN excluded.withdrawn_rev >= {table}.revision
                          THEN 0 ELSE {table}.active END,
            result = CASE WHEN excluded.withdrawn_rev >= {table}.revision
                          THEN NULL ELSE {table}.result END
        """,
        (report_no, item, revision),
    )


def stage_full_row(conn, report_no, item, result, revision):
    """全量行入暂存区;文件内重复键取最高修订。"""
    conn.execute(
        """
        INSERT INTO results_staging (report_no, item, result, revision, withdrawn_rev, active)
        VALUES (?, ?, ?, ?, 0, 1)
        ON CONFLICT(report_no, item) DO UPDATE SET
            result   = excluded.result,
            revision = excluded.revision,
            active   = 1
        WHERE excluded.revision > results_staging.revision
        """,
        (report_no, item, result, revision),
    )


# ---------------------------------------------------------------- 增量确认

def confirm_increment(conn, batch_id, report_no, item, action, result, revision):
    """确认一条增量:写日志 + 立即应用到正式表(须在事务内调用)。

    返回 (seq, effect)。
    """
    cur = conn.execute("SELECT revision, withdrawn_rev, active FROM results "
                       "WHERE report_no=? AND item=?", (report_no, item))
    old = cur.fetchone()
    if action == CORRECT:
        applied = apply_correction(conn, "results", report_no, item, result, revision)
        effect = "applied" if applied else "stale"
    else:
        was_active = bool(old and old["active"])
        apply_withdrawal(conn, "results", report_no, item, revision)
        effect = "withdrawn" if (was_active and revision >= old["revision"]) else "recorded"
    cur = conn.execute(
        "INSERT INTO increments (batch_id, report_no, item, action, result, revision, effect)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (batch_id, report_no, item, action, result if action == CORRECT else None, revision, effect),
    )
    return cur.lastrowid, effect


# ---------------------------------------------------------------- 基线合并切换

def merge_and_switch(conn, batch_id):
    """在已开启的事务内执行:墓碑承继 -> 回放期间增量 -> 原子切换。"""
    batch = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
    if batch is None or batch["kind"] != "full":
        raise RuntimeError(f"全量批次不存在: {batch_id}")
    start_seq = batch["start_seq"]

    # 1) 旧正式表中的撤回墓碑承继到暂存区:
    #    暂存区修订 <= 撤回水位 => 保持失效;缺失的键保留墓碑,杜绝旧修订复活。
    conn.execute(
        """
        INSERT INTO results_staging (report_no, item, result, revision, withdrawn_rev, active)
        SELECT report_no, item, result, revision, withdrawn_rev, active
          FROM results WHERE active = 0
        ON CONFLICT(report_no, item) DO UPDATE SET
            withdrawn_rev = MAX(results_staging.withdrawn_rev, excluded.withdrawn_rev),
            active = CASE WHEN excluded.withdrawn_rev >= results_staging.revision
                          THEN 0 ELSE results_staging.active END,
            result = CASE WHEN excluded.withdrawn_rev >= results_staging.revision
                          THEN NULL ELSE results_staging.result END
        """
    )

    # 2) 回放装载期间(及恢复后)全部已确认增量,按确认顺序
    pending = conn.execute(
        "SELECT seq, report_no, item, action, result, revision"
        " FROM increments WHERE seq > ? ORDER BY seq", (start_seq,),
    ).fetchall()
    for row in pending:
        if row["action"] == CORRECT:
            apply_correction(conn, "results_staging", row["report_no"], row["item"],
                             row["result"], row["revision"])
        else:
            apply_withdrawal(conn, "results_staging", row["report_no"], row["item"],
                             row["revision"])

    # 3) 原子切换:暂存区整体成为新正式表。
    #    全量缺失且未被增量更正的旧项目自然消失(移除)。
    conn.execute("DELETE FROM results")
    conn.execute(
        "INSERT INTO results (report_no, item, result, revision, withdrawn_rev, active)"
        " SELECT report_no, item, result, revision, withdrawn_rev, active FROM results_staging"
    )
    conn.execute("DELETE FROM results_staging")
    conn.execute(
        "UPDATE batches SET status='done', updated_at=datetime('now') WHERE id=?",
        (batch_id,),
    )
    return len(pending)


# ---------------------------------------------------------------- 查询

def current_results(conn, report_no=None, include_withdrawn=False):
    """单条 SELECT => 一致快照:每次读取均为切换前或切换后的完整有效结果。"""
    sql = "SELECT report_no, item, result, revision, withdrawn_rev, active FROM results"
    cond, args = [], []
    if not include_withdrawn:
        cond.append("active = 1")
    if report_no:
        cond.append("report_no = ?")
        args.append(report_no)
    if cond:
        sql += " WHERE " + " AND ".join(cond)
    sql += " ORDER BY report_no, item"
    return conn.execute(sql, args).fetchall()


def max_increment_seq(conn):
    row = conn.execute("SELECT COALESCE(MAX(seq), 0) AS s FROM increments").fetchone()
    return row["s"]
