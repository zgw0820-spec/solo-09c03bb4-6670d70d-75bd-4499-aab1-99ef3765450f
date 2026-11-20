"""运行期配置，全部来自环境变量，均带默认值。"""
from __future__ import annotations

import os


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


class Settings:
    # 例: postgresql://user:pass@db:5432/devicedb
    database_url: str = os.environ.get(
        "DATABASE_URL", "postgresql://postgres:postgres@db:5432/devicedb"
    )
    # 维护作业元数据所在的 schema/表（API 自建，不与业务表混用）
    job_schema: str = os.environ.get("MAINT_SCHEMA", "maintenance")
    job_table: str = os.environ.get("MAINT_JOB_TABLE", "vacuum_jobs")
    audit_table: str = os.environ.get("MAINT_AUDIT_TABLE", "termination_audit")
    # VACUUM VERBOSE 日志逐条落库的目标表
    log_table: str = os.environ.get("MAINT_LOG_TABLE", "vacuum_job_logs")
    # 日志采集的有界缓冲：队列最多容纳多少条、每批最多写多少条、
    # 空闲多久强制刷盘（毫秒）。内存占用由这三个值决定，与日志累计条数无关。
    log_queue_size: int = _int("LOG_QUEUE_SIZE", 5000)
    log_batch_size: int = _int("LOG_BATCH_SIZE", 500)
    log_flush_interval_ms: int = _int("LOG_FLUSH_INTERVAL_MS", 250)
    # 关闭日志写入器时等待线程排空/刷盘的最长秒数
    log_close_timeout_seconds: int = _int("LOG_CLOSE_TIMEOUT_SECONDS", 10)
    # 服务关停时等待后台作业收尾（落库“未确认”状态）的最长秒数
    shutdown_grace_seconds: int = _int("SHUTDOWN_GRACE_SECONDS", 15)
    # 连接池容量
    pool_min_size: int = _int("POOL_MIN_SIZE", 1)
    pool_max_size: int = _int("POOL_MAX_SIZE", 10)
    # VACUUM 语句级超时（毫秒），0 表示不限制
    vacuum_statement_timeout_ms: int = _int("VACUUM_STATEMENT_TIMEOUT_MS", 0)
    # POST /vacuum?wait=true 时等待作业结束的最长秒数
    sync_wait_seconds: int = _int("SYNC_WAIT_SECONDS", 30)


settings = Settings()
