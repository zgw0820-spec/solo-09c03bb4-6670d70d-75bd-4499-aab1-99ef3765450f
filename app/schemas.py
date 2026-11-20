"""Pydantic 模型 —— 字段描述同时生成 OpenAPI/Swagger 文档。"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, Field


class TableRef(BaseModel):
    schema_name: str = Field(default="public", description="目标表所在 schema")
    table_name: str = Field(..., description="目标表名（如 device_events）")


class SnapshotSession(BaseModel):
    pid: int
    role_name: str | None = None
    application_name: str | None = None
    client_addr: str | None = None
    state: str | None = None
    backend_start: datetime
    xact_start: datetime | None = None
    state_change: datetime | None = None
    xact_age: timedelta | None = Field(
        None, description="事务已持续时长（序列化为秒）"
    )
    backend_age: timedelta | None = None
    backend_xid: str | None = None
    backend_xmin: str | None = None
    current_query: str | None = None


class TableStats(BaseModel):
    db_name: str
    table_schema: str
    table_name: str
    relation: str
    n_dead_tup_estimate: int = Field(
        ..., description="死元组【估算】值，来自规划器统计，非精确计数"
    )
    n_live_tup_estimate: int = Field(..., description="活元组【估算】值")
    dead_tuple_ratio_estimate: float | None = Field(
        None, description="死元组占比【估算】，统计未刷新时可能为 null"
    )
    vacuum_count: int
    autovacuum_count: int
    last_vacuum: datetime | None = None
    last_autovacuum: datetime | None = None
    last_analyze: datetime | None = None
    last_autoanalyze: datetime | None = None
    relation_size_bytes: int = Field(
        ..., description="表（不含索引）当前占用字节；非本次清理释放量"
    )
    total_relation_size_bytes: int = Field(
        ..., description="表+索引+TOAST 当前占用字节；非本次清理释放量"
    )


class PreparedXact(BaseModel):
    gid: str
    owner: str
    database: str
    prepared: datetime
    age: timedelta | None = None


class ReplicationSlotInfo(BaseModel):
    slot_name: str
    slot_type: str
    active: bool
    xmin: str | None = None
    catalog_xmin: str | None = None
    retained_wal_bytes: int | None = None


class DiagnosticsResponse(BaseModel):
    generated_at: datetime
    table: TableStats
    blocking_sessions: list[SnapshotSession] = Field(
        ...,
        description="持有旧快照、可能阻碍死元组回收的会话；"
        "终止操作必须回传其中的 pid/backend_start/xact_start",
    )
    prepared_transactions: list[PreparedXact]
    inactive_replication_slots: list[ReplicationSlotInfo]
    estimates_notice: str = Field(
        "n_dead_tup_estimate 等为规划器估算值，不是精确死元组计数，"
        "更不代表可释放的磁盘字节；普通 VACUUM 通常不向操作系统归还磁盘空间。"
    )


class TerminateRequest(BaseModel):
    pid: int = Field(..., description="诊断结果中给出的 PID")
    backend_start: datetime = Field(
        ..., description="诊断结果中给出的 backend_start，必须原样回传"
    )
    xact_start: datetime = Field(
        ..., description="诊断结果中给出的事务起点 xact_start，必须原样回传"
    )
    schema_name: str = Field(default="public")
    table_name: str = Field(..., description="目标表名，用于审计关联")


class TerminateResponse(BaseModel):
    terminated: bool
    decision: Literal["terminated", "rejected"]
    target_pid: int
    reason: str
    observed_session: dict[str, Any] | None = Field(
        None, description="再校验时观察到的会话实际状态（审计留痕）"
    )


class VacuumRequest(BaseModel):
    schema_name: str = Field(default="public")
    table_name: str
    wait: bool = Field(
        False,
        description="true 时同步等待本次 VACUUM 结束（受 SYNC_WAIT_SECONDS 限制）；"
        "false 立即返回作业 id，凭作业接口轮询",
    )


class StatsSnapshot(BaseModel):
    n_dead_tup_estimate: int
    n_live_tup_estimate: int
    vacuum_count: int
    autovacuum_count: int
    last_vacuum: datetime | None = None
    last_autovacuum: datetime | None = None
    relation_size_bytes: int
    total_relation_size_bytes: int


LogState = Literal["collecting", "complete", "incomplete"]


class JobLogSummary(BaseModel):
    """作业行上的日志摘要字段；列表与详情只暴露这些，不暴露日志明细。"""

    state: LogState = Field(
        ...,
        description=(
            "collecting: 作业运行中，日志仍在采集；"
            "complete: 作业已结束且日志已全部收齐；"
            "incomplete: 作业中断或日志写入失败/有丢弃，已保存部分仍可读"
        ),
    )
    entries_saved: int = Field(..., description="已持久化的日志条数（不含丢弃）")
    entries_dropped: int = Field(0, description="因队列满或写库失败而丢弃的条数")
    error: str | None = Field(None, description="日志采集/写入的问题说明")


class JobResult(BaseModel):
    """VACUUM 结果摘要；不含日志明细（日志走分页入口）。

    旧版本作业可能只有 duration_ms 等部分字段，因此除 duration_ms 外均可缺省。
    """

    duration_ms: float | None = None
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = Field(
        None, description="VACUUM 后回读的统计（估值）；回读失败时为 null"
    )
    disk_notice: str | None = None
    completed_at: datetime | None = None
    stats_error: str | None = Field(
        None, description="VACUUM 已成功但回读统计失败时的原因；不改变成败结论"
    )


class JobSummary(BaseModel):
    """列表用摘要：状态、时间、统计结果，不含 result 整包与日志明细。"""

    id: int
    table_schema: str
    table_name: str
    status: Literal["running", "succeeded", "failed", "interrupted"]
    started_at: datetime
    finished_at: datetime | None = None
    statement_timeout_ms: int
    error_message: str | None = None
    after_stats: dict[str, Any] | None = Field(
        None, description="VACUUM 后回读的统计摘要（估值），仅成功作业有值"
    )
    logs: JobLogSummary
    log_url: str = Field(..., description="日志分页入口（GET，按游标续读）")


class JobListResponse(BaseModel):
    """作业列表：只含摘要字段，不含 result 整包与日志明细。"""

    jobs: list["JobSummary"]


class JobLogEntry(BaseModel):
    seq: int = Field(..., description="游标；下一页把它作为 after_seq 回传")
    logged_at: datetime
    severity: str
    message: str


class JobLogPage(BaseModel):
    job_id: int
    job_status: Literal["running", "succeeded", "failed", "interrupted"] = Field(
        ..., description="作业本身的执行状态；与日志采集状态分开呈现"
    )
    items: list[JobLogEntry]
    next_seq: int = Field(
        ..., description="下一页的 after_seq；没有更多时等于最后一条的 seq"
    )
    has_more: bool
    state: LogState = Field(..., description="当前末尾的日志采集状态")
    entries_saved: int
    entries_dropped: int = 0
    end_reason: str = Field(
        ...,
        description=(
            "本页之后是否还有内容：has_more=true 时阅读方式不变；"
            "has_more=false 时给出结论 —— "
            "collecting=作业仍在采集，稍后用同一 next_seq 续读；"
            "complete=日志已收齐；"
            "incomplete=采集不完整（作业中断或写入失败/丢弃），已保存部分保留"
        ),
    )
    error: str | None = None


class JobResponse(BaseModel):
    id: int
    table_schema: str
    table_name: str
    status: Literal["running", "succeeded", "failed", "interrupted"]
    started_at: datetime
    finished_at: datetime | None = None
    statement_timeout_ms: int
    before_snapshot: dict[str, Any]
    result: JobResult | None = Field(
        None,
        description="VACUUM 结果（不含日志明细；VERBOSE 日志请走 log_url 分页读取）",
    )
    error_message: str | None = None
    logs: JobLogSummary
    log_url: str = Field(..., description="日志分页入口（GET，按游标续读）")
    verification_hint: str | None = Field(
        None,
        description="interrupted 作业的核实结论: 重启后对比统计基线判断 VACUUM 是否实际完成",
    )
    disk_notice: str | None = Field(
        None,
        description="relation_size_bytes 为当前占用，差值不等于'已释放磁盘字节'",
    )


class HealthResponse(BaseModel):
    status: Literal["ok", "error"]
    database: str | None = None
    detail: str | None = None
