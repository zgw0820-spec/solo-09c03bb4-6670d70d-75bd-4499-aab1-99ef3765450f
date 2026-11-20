"""FastAPI 入口: 诊断、受控终止会话、VACUUM 作业（单飞 + 可核实/重试）。

VACUUM VERBOSE 日志采用“边采集边保存”：作业运行中即可按作业号 + 游标
分页续读；列表/详情只返回状态、统计等摘要，不整段搬运日志。
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query, status
from psycopg import sql

from . import db, maintenance
from . import sql_queries as q
from .config import settings
from .schemas import (
    DiagnosticsResponse,
    HealthResponse,
    JobListResponse,
    JobLogPage,
    JobResponse,
    JobSummary,
    TerminateRequest,
    TerminateResponse,
    VacuumRequest,
)


logger = logging.getLogger("app")

# 后台 VACUUM 任务句柄：服务关停时统一取消并等待收尾，
# 确保“未确认”状态与已保存日志在进程退出前落库。
_background_tasks: set[asyncio.Task] = set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.open_pool()
    # 建表 + 把上次进程遗留的 running 作业标记为 interrupted（日志标为
    # incomplete，已保存日志保留），并把旧作业的内嵌日志迁移到分页明细表。
    await db.init_schema()
    try:
        yield
    finally:
        await _shutdown_tasks()
        await db.close_pool()


async def _shutdown_tasks() -> None:
    """取消仍在执行的作业并等待其把“未确认”结果落库。"""
    tasks = [t for t in _background_tasks if not t.done()]
    for t in tasks:
        t.cancel()
    if tasks:
        done, pending = await asyncio.wait(
            tasks, timeout=settings.shutdown_grace_seconds
        )
        for t in pending:
            logger.warning("作业收尾超时: %s", t.get_name())
        for t in done:
            exc = t.exception()
            if exc is not None and not isinstance(exc, asyncio.CancelledError):
                logger.warning("作业收尾异常: %s", exc)
    _background_tasks.clear()


app = FastAPI(
    title="PostgreSQL 16 设备事件表 VACUUM 维护 API",
    version="1.1.0",
    description=(
        "展示设备事件表死元组【估算】与可能阻碍回收的旧快照会话；"
        "凭诊断返回的 PID/backend_start/事务起点，仅可终止同库且仍为 "
        "idle in transaction 的原会话；随后执行 VACUUM 并回读结果。\n\n"
        "- 同表并发维护单飞（部分唯一索引 + advisory lock）。\n"
        "- VERBOSE 日志边采集边保存：作业运行中即可按作业号 + 游标分页续读"
        "（每页 ≤100 条）；采集内存固定，与日志累计条数无关。\n"
        "- 列表/详情只含状态与统计摘要，日志统一走分页入口；"
        "翻到末尾时明确区分 collecting / complete / incomplete。\n"
        "- 请求超时或服务重启后，未确认的清理不会被报为完成，"
        "可经作业接口核实或重试。\n"
        "- 所有 n_dead_tup/尺寸字段均为估值或当前占用，"
        "不代表“已释放磁盘字节”。"
    ),
    lifespan=lifespan,
)


async def _check_relation(schema_name: str, table_name: str) -> None:
    if not await db.relation_exists(schema_name, table_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"relation {schema_name}.{table_name} does not exist",
        )


@app.get("/health", response_model=HealthResponse, tags=["meta"])
async def health() -> HealthResponse:
    try:
        pool = await db.open_pool()
        async with pool.connection() as conn:
            cur = await conn.execute("SELECT current_database() AS db")
            row = await cur.fetchone()
        return HealthResponse(status="ok", database=row["db"])
    except Exception as exc:  # 连通性失败要显式报错
        return HealthResponse(status="error", detail=str(exc))


@app.get(
    "/diagnostics",
    response_model=DiagnosticsResponse,
    tags=["diagnostics"],
    summary="死元组估算 + 可能阻碍回收的旧快照会话",
)
async def diagnostics(schema_name: str = "public",
                      table_name: str = "device_events") -> DiagnosticsResponse:
    await _check_relation(schema_name, table_name)
    stats = await db.fetch_table_stats(schema_name, table_name)
    sessions = await db.fetch_blocking_sessions()
    prepared = await db.fetch_prepared_xacts()
    slots = await db.fetch_inactive_slots()
    return DiagnosticsResponse(
        generated_at=datetime.now(timezone.utc),
        table=stats,
        blocking_sessions=sessions,
        prepared_transactions=prepared,
        inactive_replication_slots=slots,
    )


@app.post(
    "/sessions/terminate",
    response_model=TerminateResponse,
    tags=["sessions"],
    summary="终止 idle in transaction 会话（身份/状态变化即拒绝）",
)
async def terminate_session(req: TerminateRequest) -> TerminateResponse:
    await _check_relation(req.schema_name, req.table_name)
    result = await maintenance.terminate_idle_session(
        req.pid, req.backend_start, req.xact_start,
        req.schema_name, req.table_name,
    )
    return TerminateResponse(**result)


@app.post(
    "/maintenance/vacuum",
    tags=["maintenance"],
    status_code=status.HTTP_202_ACCEPTED,
    summary="对目标表执行 VACUUM (VERBOSE, ANALYZE)，同表单飞",
)
async def start_vacuum(req: VacuumRequest) -> dict:
    await _check_relation(req.schema_name, req.table_name)
    job_id, conflict = await maintenance.create_vacuum_job(
        req.schema_name, req.table_name
    )
    if job_id is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "该表已有正在执行的维护作业，拒绝重复执行",
                "running_job": conflict,
            },
        )

    async def _runner() -> None:
        await maintenance.run_vacuum_job(
            job_id, req.schema_name, req.table_name
        )

    task = asyncio.create_task(_runner(), name=f"vacuum-job-{job_id}")
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)

    if req.wait:
        deadline = asyncio.get_event_loop().time() + settings.sync_wait_seconds
        while True:
            job = await _load_job(job_id)
            if job["status"] != "running":
                return {"job": _with_log_fields(_serialize_job(job))}
            if asyncio.get_event_loop().time() >= deadline:
                return {
                    "job": _with_log_fields(_serialize_job(job)),
                    "message": "等待超时，作业仍在后台执行；"
                               "未确认完成，不会报为成功，请轮询作业接口；"
                               "日志可用 log_url 按游标续读",
                }
            await asyncio.sleep(0.25)

    return {
        "job_id": job_id,
        "status": "running",
        "message": "作业已受理；VACUUM 在后台执行，凭 job_id 轮询核实；"
                   "日志在作业运行中即可按游标续读",
        "log_url": f"/maintenance/jobs/{job_id}/logs?after_seq=0&limit=100",
    }


async def _load_job(job_id: int) -> dict:
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(
            sql.SQL("SELECT * FROM {sch}.{tbl} WHERE id = %s").format(
                sch=sql.Identifier(settings.job_schema),
                tbl=sql.Identifier(settings.job_table),
            ),
            (job_id,),
        )
        row = await cur.fetchone()
    if row is None:
        raise HTTPException(404, f"job {job_id} not found")
    return dict(row)


async def _load_job_summary(job_id: int) -> dict:
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(q.GET_JOB_SUMMARY, (job_id,))
        row = await cur.fetchone()
    if row is None:
        raise HTTPException(404, f"job {job_id} not found")
    return dict(row)


def _log_summary(row: dict) -> dict:
    return {
        "state": row.get("log_state") or "collecting",
        "entries_saved": int(row.get("log_entries") or 0),
        "entries_dropped": int(row.get("log_dropped") or 0),
        "error": row.get("log_error"),
    }


def _log_url(job_id: int) -> str:
    return f"/maintenance/jobs/{job_id}/logs"


def _serialize_job(row: dict) -> dict:
    out = dict(row)
    if out.get("status") == "interrupted":
        out["verification_hint"] = "调用 GET /maintenance/jobs/{id} 触发核实"
    if out.get("status") == "succeeded":
        out["disk_notice"] = maintenance.DISK_NOTICE
    return out


@app.get(
    "/maintenance/jobs/{job_id}",
    response_model=JobResponse,
    tags=["maintenance"],
    summary="查询作业摘要；日志请走 /maintenance/jobs/{id}/logs 分页续读",
)
async def get_job(job_id: int) -> JobResponse:
    job = await _load_job_summary(job_id)
    hint: str | None = None
    if job["status"] == "interrupted":
        # 核实需要完整作业行（含 before_snapshot），与摘要读取解耦
        hint = await maintenance.verify_interrupted_job(await _load_job(job_id))
    return JobResponse(**_job_response(job, hint))


def _job_response(job: dict, verification_hint: str | None = None) -> dict:
    """作业摘要 + 日志摘要；日志明细一律走分页入口，不内嵌。"""
    out = _serialize_job(job)
    out.pop("vacuum_verbose_notices", None)
    if out.get("result"):
        out["result"] = {
            k: v for k, v in out["result"].items()
            if k != "vacuum_verbose_notices"
        }
    out["logs"] = _log_summary(job)
    out["log_url"] = _log_url(job["id"])
    if verification_hint is not None:
        out["verification_hint"] = verification_hint
    return out


def _with_log_fields(job: dict) -> dict:
    """给完整作业行补上日志摘要与分页入口（用于 wait=true 同步返回）。

    原始 log_* 列不外露，统一用 logs 摘要，避免两套字段。
    """
    out = {k: v for k, v in job.items() if not k.startswith("log_")}
    # 旧版本内嵌的日志明细不在此返回；统一走分页入口
    out.pop("vacuum_verbose_notices", None)
    if out.get("result"):
        out["result"] = {
            k: v for k, v in out["result"].items()
            if k != "vacuum_verbose_notices"
        }
    out["logs"] = _log_summary(job)
    out["log_url"] = _log_url(job["id"])
    return out


@app.get(
    "/maintenance/jobs/{job_id}/logs",
    response_model=JobLogPage,
    tags=["maintenance"],
    summary="按游标分页读取 VERBOSE 日志（作业运行中也可读，每页 ≤100 条）",
)
async def get_job_logs(
    job_id: int,
    after_seq: int = Query(
        0, ge=0,
        description="游标：上次响应的 next_seq；首次传 0 或不传",
    ),
    limit: int = Query(
        100, ge=1, le=100, description="每页最多 100 条",
    ),
) -> JobLogPage:
    job = await _load_job_summary(job_id)
    # 多取一条用于判断是否还有下一页（捕获读取期间新写入的日志）
    entries = await db.fetch_job_log_page(job_id, after_seq, limit + 1)
    has_more = len(entries) > limit
    entries = entries[:limit]
    next_seq = entries[-1]["seq"] if entries else after_seq
    state = job.get("log_state") or "collecting"
    saved = int(job.get("log_entries") or 0)
    dropped = int(job.get("log_dropped") or 0)
    log_error = job.get("log_error")

    if has_more:
        end_reason = "还有更多日志；用 next_seq 作为 after_seq 继续读取"
    elif state == "collecting":
        end_reason = (
            "已到当前末尾，但作业仍在采集：稍后用同一个 next_seq 续读，"
            "不要据此认为日志已结束"
        )
    elif state == "complete":
        end_reason = "已到末尾且采集已完成：作业已结束，日志已全部收齐"
    else:
        end_reason = (
            "已到末尾但采集不完整：作业中断或日志写入失败/有丢弃；"
            "已保存的日志保留，可继续用 next_seq 读取（不会有新内容）"
        )

    return JobLogPage(
        job_id=job_id,
        job_status=job["status"],
        items=entries,
        next_seq=next_seq,
        has_more=has_more,
        state=state,
        entries_saved=saved,
        entries_dropped=dropped,
        end_reason=end_reason,
        error=log_error,
    )


@app.get(
    "/maintenance/jobs",
    response_model=JobListResponse,
    tags=["maintenance"],
    summary="列出某表最近的作业（仅状态/统计摘要，不含日志明细）",
)
async def list_jobs(schema_name: str = "public",
                    table_name: str = "device_events",
                    limit: int = Query(20, ge=1, le=100)) -> JobListResponse:
    await _check_relation(schema_name, table_name)
    pool = await db.open_pool()
    async with pool.connection() as conn:
        cur = await conn.execute(q.LIST_JOB_SUMMARIES,
                                 (schema_name, table_name, limit))
        rows = await cur.fetchall()
    return JobListResponse(jobs=[
        JobSummary(**{**dict(r), "logs": _log_summary(r),
                      "log_url": _log_url(r["id"])})
        for r in rows
    ])


@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": app.title,
        "docs": "/docs",
        "endpoints": [
            "/health",
            "/diagnostics?schema_name=public&table_name=device_events",
            "/sessions/terminate",
            "/maintenance/vacuum",
            "/maintenance/jobs/{job_id}",
            "/maintenance/jobs/{job_id}/logs?after_seq=0&limit=100",
            "/maintenance/jobs",
        ],
    }
