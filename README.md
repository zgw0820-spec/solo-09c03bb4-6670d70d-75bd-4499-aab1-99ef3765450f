# PostgreSQL 16 设备事件表 VACUUM 维护 API

值班员在 `device_events` 等表删除旧数据后，用于：查看死元组**估算**与可能阻碍
回收的旧快照会话 → 受控地终止原会话 → 执行 VACUUM 并回读统计结果。

技术栈：FastAPI + psycopg 3 + uvicorn，PostgreSQL 16，Docker Compose 一键启动。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# API:      http://localhost:8000
# Swagger:  http://localhost:8000/docs
# Postgres: localhost:55432（库 devicedb，用户 appuser/apppass）
```

初始化脚本 `db/init/01_device_events.sql` 会建按 `device_id` HASH 分区的
`public.device_events`（32 个分区，贴近“分区多、VERBOSE 通知多”的生产形态），
插入 10 万行后删除一批行制造死元组，并**仅对该演示表**关闭 autovacuum。

验证服务已启动：

```bash
docker compose ps
curl -s http://localhost:8000/health
```

停止并清空数据（含卷）：

```bash
docker compose down -v
```

## 接口

| 方法/路径 | 说明 |
| --- | --- |
| `GET /health` | 数据库连通性 |
| `GET /diagnostics?schema_name=public&table_name=device_events` | 死元组/活元组**估算**、最后 VACUUM 时间、表当前占用字节、可能阻碍回收的旧快照会话（含 `pid`/`backend_start`/`xact_start`）、预备事务与非活跃复制槽 |
| `POST /sessions/terminate` | 凭诊断返回的 `pid`+`backend_start`+`xact_start` 终止会话；再校验失败即拒绝 |
| `POST /maintenance/vacuum` | 对表执行 `VACUUM (VERBOSE, ANALYZE)`，同表单飞，返回作业 id 与 `log_url` |
| `GET /maintenance/jobs/{id}` | 作业摘要（状态、统计、日志摘要）；`interrupted` 作业附带基于统计基线的核实结论 |
| `GET /maintenance/jobs/{id}/logs?after_seq=&limit=` | **按游标分页读取 VERBOSE 日志**，作业运行中也可读，`limit` 最大 100 |
| `GET /maintenance/jobs` | 某表最近的作业列表（仅状态/统计摘要，不含日志明细） |

### VERBOSE 日志：边采集边保存

- 执行器把每条 `INFO` 通知立即交给采集器，由独立写线程分批落库到
  `maintenance.vacuum_job_logs`。采集端只有一个**固定容量**的队列，
  内存占用与日志累计条数无关。
- 值班员在 Swagger 或 curl 里用 `GET /maintenance/jobs/{id}/logs` 按
  `after_seq`（初次传 0）逐页读取，每页最多 100 条；响应中的 `next_seq`
  就是下一页的 `after_seq`。作业运行中就能读到已保存的部分。
- 翻到当前末尾（`has_more=false`）时，用 `state` / `end_reason` 区分：
  - `collecting`：作业仍在采集，稍后用同一个 `next_seq` 续读；
  - `complete`：作业已结束，日志已全部收齐；
  - `incomplete`：作业中断，或日志写入失败/有丢弃；**已保存的日志保留**，
    仍可沿原游标读取（不会再有新内容），`error`/`entries_dropped` 说明原因。
- 列表与详情只返回状态、统计等摘要（`logs.entries_saved` 等）；旧版本作业
  内嵌在 `result.vacuum_verbose_notices` 里的日志，服务启动时会自动迁入同一
  分页入口，详情不再整段返回。
- 日志写入失败不影响 VACUUM 作业的成败：作业状态只以 VACUUM 语句是否真正
  返回为准。
- 服务重启时，遗留的 `running` 作业标为 `interrupted`，日志状态标为
  `incomplete`，已落库的日志一条不删，重启后仍可沿原游标续读。
- 优雅关停（SIGTERM）会先取消在跑作业、等它把 `interrupted` + 已保存条数
  落库，再关闭连接池（`SHUTDOWN_GRACE_SECONDS`、compose 的
  `stop_grace_period`）；进程被 `kill -9` 时则由下次启动的扫描补记。
- 响应形状提示：`GET /maintenance/jobs` 仍为 `{"jobs": [...]}`（只含摘要）；
  `GET /maintenance/jobs/{id}` 的 `result` 不再含 `vacuum_verbose_notices`，
  改为 `logs` 摘要 + `log_url`。

```bash
# 发起作业（异步）
JOB=$(curl -s -X POST http://localhost:8000/maintenance/vacuum \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"device_events"}' | jq -r .job_id)

# 作业运行中就能读日志；next_seq 作为下一页游标
curl -s "http://localhost:8000/maintenance/jobs/$JOB/logs?after_seq=0&limit=100" | jq
curl -s "http://localhost:8000/maintenance/jobs/$JOB/logs?after_seq=100&limit=100" | jq
```

### 典型流程

```bash
# 1) 诊断：拿到死元组估算与阻碍会话
curl -s http://localhost:8000/diagnostics | jq

# 2) 仅当确认某个 idle in transaction 会话应终止时，原样回传三个字段
curl -s -X POST http://localhost:8000/sessions/terminate \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"device_events","pid":123,
       "backend_start":"2026-11-19T01:00:00+00:00",
       "xact_start":"2026-11-19T01:01:00+00:00"}'

# 3) 执行 VACUUM（异步受理，凭 job_id 轮询；也可 wait=true 同步等）
curl -s -X POST http://localhost:8000/maintenance/vacuum \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"device_events"}'

curl -s http://localhost:8000/maintenance/jobs/1 | jq
```

## 关键设计与事实边界

1. **终止会话的再校验（默认拒绝）**：执行 `pg_terminate_backend` 前重新查询
   `pg_stat_activity`，必须同时满足：同库、PID 存在、`backend_start` 一致、
   `xact_start` 一致、`state = 'idle in transaction'`。会话已退出、被替换
   （PID 复用但 backend_start 已变）、事务重开（xact_start 已变）或状态改变，
   一律拒绝。所有尝试（含拒绝）写入 `maintenance.termination_audit`。
2. **同表并发维护不重复执行**：
   - 持久化第一道保险：`maintenance.vacuum_jobs` 上的部分唯一索引
     `WHERE status='running'`，同表第二个请求直接收到 `409`；
   - 进程内/跨进程第二道保险：按表派生 key 的 session-level advisory lock。
3. **超时/重启绝不虚报完成**：
   - 作业状态只有 `running/succeeded/failed/interrupted`，结果只由后台执行器
     在 VACUUM 真正返回后落库；HTTP 请求超时只影响调用方等待，不改变作业状态；
   - 服务启动时把遗留的 `running` 全部标记为 `interrupted`，并在查询时对比
     作业登记的统计基线（`vacuum_count`、死元组估算）给出核实结论，支持重试；
   - 重试会新建作业，不覆盖历史记录。
4. **估值不是磁盘释放量**：`n_dead_tup_estimate`/`n_live_tup_estimate` 是
   规划器统计估算（PG16 中由统计子系统异步投递，存在可见延迟）；
   `relation_size_bytes` 是当前占用。普通 `VACUUM` 把空间留在库内复用，
   通常**不向操作系统归还磁盘**，因此 API 不提供也不使用“已释放磁盘字节”
   之类的字段或表述。
5. **VACUUM 不能在事务块中运行**：执行器使用独立的 autocommit 连接。
6. **日志采集内存有界**：通知回调只做“分配序号 + 入队”，落库在独立线程完成；
   队列容量、批次大小与刷盘间隔都是固定配置，因此无论 VACUUM 产生多少条
   通知，采集端内存都不随累计条数增长。队列满或写库失败时只丢当前条目/批次，
   计入 `entries_dropped`，已落库的日志不受影响。
7. **分区表的统计按叶子汇总**：PG16 的分区父表没有存储，父表上的
   `pg_stat_get_*`/`pg_relation_size` 恒为 0。诊断与作业回读对分区表按
   `pg_partition_tree` 的叶子分区求和，避免把“实际已执行”误判为“没执行”。
   `vacuum_count` 等仍为统计估算值，存在异步投递延迟。

## 本地自测（无需 Docker）

仓库 `tests/selftest.py` 对真实 PostgreSQL 16 实例做端到端验证，覆盖：
诊断字段、伪造/过期凭证拒绝、会话状态改变拒绝、会话替换拒绝、正常终止与审计、
VACUUM 成功路径与统计回读、同表 409 单飞、重启后 interrupted 核实与重试、
statement_timeout 失败落库后可重新发起。

```bash
# 需要一个可达的 PG16 实例，通过 DATABASE_URL 指定
export DATABASE_URL='postgresql://user:pass@localhost:5432/devicedb'
python3 tests/selftest.py

# 日志“边采集边保存”专项：运行中分页读取、续读、写失败、重启中断续航、
# 旧作业内嵌日志迁移、分页参数边界
python3 tests/logstream_selftest.py
```

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://postgres:postgres@db:5432/devicedb` | 维护连接串 |
| `MAINT_SCHEMA` | `maintenance` | 作业/审计表 schema（API 自建） |
| `VACUUM_STATEMENT_TIMEOUT_MS` | `0`（不限） | VACUUM 语句超时 |
| `SYNC_WAIT_SECONDS` | `30` | `wait=true` 最长等待秒数 |
| `MAINT_LOG_TABLE` | `vacuum_job_logs` | VERBOSE 日志明细表（与作业表同 schema） |
| `LOG_QUEUE_SIZE` | `5000` | 采集队列容量（固定内存上限） |
| `LOG_BATCH_SIZE` | `500` | 每批写入条数 |
| `LOG_FLUSH_INTERVAL_MS` | `250` | 无新日志时强制刷盘间隔 |
| `LOG_CLOSE_TIMEOUT_SECONDS` | `10` | 作业收尾时排空队列的最长等待 |
| `POOL_MIN_SIZE` / `POOL_MAX_SIZE` | `1` / `10` | 连接池容量 |
