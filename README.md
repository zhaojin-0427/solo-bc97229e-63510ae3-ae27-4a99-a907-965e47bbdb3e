# LIMS 检测结果同步平台

检测实验室数据工程师的入库工具:将 LIMS 导出的**全量结果**与**复测更正/撤回增量**同步入库,
为报告系统提供一致的取数接口。Flask 提供上传与进度页面,SQLite 持久保存导入记录、
装载进度与正式结果表。服务以**两个 Gunicorn worker 进程**共用同一数据目录运行,
任一进程退出后,存活进程自动接续未完成的工作,无需重新上传或重启整个服务。

## 快速启动

### Docker Compose(推荐,默认两个 worker)

```bash
docker compose up --build
# 打开 http://localhost:8000
```

容器内 Gunicorn 启动 **2 个 worker × 4 线程**(可用环境变量 `LIMS_WORKERS`/`LIMS_THREADS`
调整)。两个进程共享命名卷 `lims_data`(容器内 `/data`)中的 SQLite 数据库、上传文件与
批次锁。健康检查接口 `/api/health` 返回处理当前请求的 worker PID,连续刷新首页状态区
即可看到响应在两个 worker 之间轮换。

`docker compose restart`、容器异常重启,或某个 worker 被强制终止后:
未完成的全量装载由存活 worker 自动续装,已确认增量不丢失。

### 本地开发

```bash
pip install -r requirements.txt
# 单进程调试
python wsgi.py                          # http://localhost:8000
# 本地以两个 worker 运行(与容器一致)
gunicorn --workers 2 --threads 4 --bind 0.0.0.0:8000 wsgi:app
# 测试:
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

环境变量:`DATA_DIR`(数据目录,默认 `./data`)、`LIMS_CHUNK_SIZE`(装载块大小,默认 500)、
`LIMS_CHUNK_DELAY_MS`(演示用块间延迟,默认 150ms,生产可设 0)、`LIMS_MAX_UPLOAD_MB`(默认 50)、
`LIMS_WORKERS`(默认 2)、`LIMS_THREADS`(默认 4)、`LIMS_LOCK_TIMEOUT_S`(请求线程等待批次锁
的最长秒数,默认 60)、`LIMS_MONITOR_IDLE_S`/`LIMS_MONITOR_BUSY_S`(监控线程轮询间隔)。

> 注意:`DATA_DIR` 必须位于支持 `fcntl.flock` 的本地文件系统(容器卷、普通磁盘均可);
> 不要放在 NFS 等不保证顾问锁语义的网络盘上。

## 多 worker 协调与故障接续(核心设计)

两个 worker 是**独立进程**,各自启动一个常驻监控线程轮询数据目录中的待处理批次。
当它们同时发现同一待处理批次时,由双层互斥保证只有一个执行体推进:

1. **进程内线程锁**(`threading.Lock`,按批次):同一 worker 的请求线程与监控线程互斥。
2. **跨进程文件锁**(`fcntl.flock`,锁文件 `<DATA_DIR>/locks/batch-<id>.lock`):
   worker 进程之间互斥。flock 与打开的文件描述符绑定,**进程被 SIGKILL/崩溃/断电时
   由内核自动释放,无需心跳或租约**。存活 worker 下一轮轮询拿到锁,从数据库中持久化的
   文件偏移继续处理——无需重新上传,也无需重启服务。

由此得到的保证:

- 同一批次的进度计数、`increments` 确认日志、基线切换在整个集群内**只发生一次**,
  不会重复推进、重复写日志或重复切换清空正式表。
- 持锁 worker 被 `kill -9` 后,Gunicorn master 拉起的新 worker 与另一存活 worker
  都会发现待处理批次;抢到锁的那个从断点接续,另一个复查到批次已完成后直接跳过。
- 拿到锁后会先复查批次状态:等待方在持锁方完成后才获得锁,此时状态已是 `done`,
  立即返回,不做任何重复工作。
- 纵深防御:`merge_and_switch` 在事务内再次校验批次状态,即便绕过锁,
  已完成的批次也无法再次执行 `DELETE FROM results`,正式表不可能被重复切换清空。

### 手动演示故障接续

```bash
# 1. 首页点击“全量大文件”,装载进行中时再点“增量·装载中更正/撤回”
# 2. 查看持锁 worker(锁文件内记录其 PID):
docker exec lims-results-sync sh -c 'cat /data/locks/batch-*.lock'
# 3. 强制终止该 worker(模拟进程崩溃)
docker exec lims-results-sync kill -9 <PID>
# 4. 观察首页:批次进度继续增长直至 100%,无需重新上传;取数始终是完整结果
```

## 演示流程(页面一键示例)

首页提供示例按钮,建议按以下顺序观察:

1. **全量基线 v1** → 10 条结果入库(批次区可见进度,完成后切换为正式基线)。
2. **增量·更正** → `R2026-0001 铅` 更新为 `0.10`(rev2);`R2026-0002 铅` 的 rev1 更正
   低于现有 rev2,被忽略(同报告同项目取最高修订);`R2026-0009` 新增。
3. **增量·撤回** → `R2026-0005 镉`、`R2026-0008 铅`、`R2026-0004 砷` 撤回。
4. **增量·撤回后更正** → `R2026-0005 镉` 的 rev2 更正 ≤ 撤回水位 3,**不能恢复**;
   rev4 更正 > 水位,生效为 `0.09`。
5. **全量大文件(2 万行)** → 装载约数秒,期间立即点击 **增量·装载中更正/撤回**:
   增量实时作用于当前正式表;装载完成切换时自动合入。切换后验证:
   - `L2026-00010 铅` = `9.99`(期间增量 rev9 高于全量文件修订);
   - `L2026-00020 镉` 不存在(期间撤回);
   - `R2026-0006 汞` 保留(全量缺失但被期间增量更正);
   - `R2026-0007 砷` 被移除(全量缺失且未被更正);
   - 装载中再次上传全量会被拒绝(同一时刻仅允许一个全量装载)。
6. **全量基线 v2(切换)** → 演示墓碑承继:`R2026-0004 砷` 在全量 v2 中以 rev1 出现,
   但撤回水位为 2,**旧修订不能复活**;`R2026-0008 铅` 以 rev2(>水位 1)合法恢复。

可随时点击 **读取当前有效结果** 演示报告系统取数;**含撤回墓碑** 可查看撤回水位。

## CSV 格式

表头支持英文或中文(可混用),UTF-8(兼容 BOM):

- 全量:`report_no,item,result,revision`(或 `报告号,检测项目,结果,修订号`)
- 增量:`report_no,item,action,result,revision`(动作列 `动作`;
  取值 `correct/c/更正`、`withdraw/w/撤回`,撤回行结果列留空)

修订号为**正整数**。错误行(空键、非法修订号、更正缺结果等)计入批次 `error_rows`,
不影响其余行入库。

## 语义规则

对同一 `报告号+检测项目`:

| 事件 | 规则 |
|---|---|
| 更正 rev R | 仅当 `R > 当前修订` 且 `R > 撤回水位` 时生效(取最高修订) |
| 撤回 rev R | 水位记为 `max(水位, R)`;`R ≥ 当前修订` 时结果失效(墓碑) |
| 撤回后旧修订更正 | 拒绝(`R ≤ 水位`),结果不能恢复 |
| 撤回后新修订更正 | 允许(`R > 水位`) |
| 全量文件内重复键 | 取最高修订 |
| 基线切换 | 暂存区 + 承继撤回墓碑 + 回放**装载期间**全部已确认增量 → 原子替换正式表 |
| 全量缺失的旧项目 | 未被期间增量更正 → 移除;被更正 → 按增量保留 |

> 边界语义:切换只合入**装载开始后**确认的增量;更早确认的增量应已包含在该次
> 全量导出中,新基线以全量文件为准。撤回墓碑跨基线保留,因此“撤回后旧修订不能
> 恢复结果”在切换后依然成立。

## HTTP 接口

| 方法/路径 | 说明 |
|---|---|
| `GET /` | 演示页面(上传、进度、取数,状态区显示响应 worker PID) |
| `POST /upload/full` | 上传全量 CSV(multipart `file`),后台装载;已有装载进行中返回 `409` |
| `POST /upload/inc` | 上传增量 CSV,请求内同步确认,响应返回时已更新正式表;批次锁被占用且等待超时返回 `503` |
| `POST /load_sample/<name>` | 载入随附示例(`full_baseline`、`inc_corrections`、`inc_withdrawals`、`inc_after_withdraw`、`full_large`、`inc_during_load`、`full_baseline_v2`) |
| `GET /api/results` | **报告系统取数接口**:当前完整有效结果;参数 `report_no` 过滤、`all=1` 含墓碑 |
| `GET /api/batches` | 导入批次与进度(全量/增量、状态、行数、百分比) |
| `GET /api/increments` | 已确认增量日志(全局序号、动作、效果) |
| `GET /api/status` | 汇总:有效结果数、墓碑数、增量数、是否有装载进行中 |
| `GET /api/health` | 健康检查,返回 `{ok, pid}`,pid 为当前服务的 worker 进程号 |

取数接口为单条 `SELECT`(WAL 快照读),每次读取均为**切换前或切换后的完整有效结果**,
不会读到切换中间态,也不会读到半批数据。

## 可靠性与一致性设计

- **跨进程互斥**:批次级 `flock` + 进程内线程锁,两个 worker 不会重复推进同一批次;
  锁随进程死亡由内核自动释放,存活 worker 自动接管(详见上文)。
- **断点续传**:全量分块写入暂存表,每块与进度(文件偏移)在同一事务提交;
  进程中断后由任一存活 worker 从最近偏移继续装载。
- **增量不丢且不重**:确认一条增量 = 写 `increments` 日志 + 应用正式表,同一事务;
  日志序号 `seq` 全局单调,切换时回放 `seq > 装载起始水位` 的全部增量。
  批次锁保证同一增量批次只被确认一次,接续时从持久偏移处理未确认部分。
- **原子切换**:墓碑承继 + 增量回放 + 正式表替换在**单个事务**内完成,并在事务内
  复查批次状态;崩溃则整体回滚,恢复后重做,读者只见切换前或切换后。
- **唯一全量装载**:数据库部分唯一索引(`status IN (loading, staging_ready)`)
  强制同一时刻至多一个全量装载,冲突返回 409。
- **即时生效**:增量在上传请求内同步确认,立即更新现有正式表,与后台装载并行不悖
  (SQLite 写事务串行化 + busy_timeout,WAL 下读不阻塞)。

## 目录结构

```
app/
  __init__.py   应用工厂(每个 worker 启动时触发恢复监控)
  db.py         SQLite 连接与表结构(batches/results/results_staging/increments)
  logic.py      修订裁决、撤回墓碑、合并切换、CSV 解析
  locks.py      跨进程 flock + 进程内线程锁的批次互斥(BatchGuard)
  loader.py     后台装载、断点续传、每 worker 常驻监控线程与多 worker 协调
  routes.py     上传/进度/取数接口与页面
  templates/index.html
samples/        全量与装载中更正/撤回示例文件
tests/          用例:语义、切换、恢复、单装载、接口、多进程协调与 kill 接续
wsgi.py         gunicorn wsgi:app
Dockerfile / docker-compose.yml   默认 2 worker × 4 线程,共享 /data 卷
```
