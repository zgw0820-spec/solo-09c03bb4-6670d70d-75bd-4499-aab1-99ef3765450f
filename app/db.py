"""连接池、初始化与统计读取。"""
from __future__ import annotations

import json
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from . import sql_queries as q
from .config import settings

pool: AsyncConnectionPool | None = None


async def open_pool() -> AsyncConnectionPool:
    global pool
    if pool is None:
        pool = AsyncConnectionPool(
            conninfo=settings.database_url,
            min_size=settings.pool_min_size,
            max_size=settings.pool_max_size,
            kwargs={"autocommit": True, "row_factory": dict_row},
            open=False,
        )
        await pool.open()
    return pool


async def close_pool() -> None:
    global pool
    if pool is not None:
        await pool.close()
        pool = None


async def init_schema() -> None:
    """建维护表，并把上次进程遗留的 running 作业标记为 interrupted。

    日志表与作业行上的日志摘要列在此一并建好；旧版本把 VERBOSE 通知内嵌在
    result 里，启动时搬到日志明细表，保证旧作业也能从同一分页入口续读。
    """
    assert pool is not None
    async with pool.connection() as conn:
        await conn.execute(q.CREATE_SCHEMA)
        await conn.execute(q.CREATE_JOB_TABLE)
        await conn.execute(q.CREATE_JOB_UNIQUE)
        for stmt in q.ALTER_JOB_LOG_COLUMNS:
            await conn.execute(stmt)
        await conn.execute(q.CREATE_LOG_TABLE)
        await conn.execute(q.CREATE_AUDIT_TABLE)
        # 先把中断作业标出来（日志状态随之变为 incomplete），再迁移旧日志、
        # 规范化旧作业的日志状态，最后以明细表为准重算条数。
        await conn.execute(q.SWEEP_INTERRUPTED)
        await conn.execute(q.MIGRATE_LEGACY_NOTICES)
        await conn.execute(q.NORMALIZE_LEGACY_LOG_STATE)
        await conn.execute(q.RECOUNT_JOB_LOG_ENTRIES)


async def relation_exists(schema_name: str, table_name: str) -> bool:
    assert pool is not None
    async with pool.connection() as conn:
        cur = await conn.execute(q.RELATION_EXISTS, (schema_name, table_name))
        return cur.rowcount > 0


async def fetch_table_stats(schema_name: str, table_name: str) -> dict[str, Any]:
    """读统计。

    普通表直接读 pg_stat_user_tables；分区表按叶子分区汇总——PG16 的分区父表
    没有存储，父表上的 pg_stat_get_* 恒为 0，若只看父表会把“实际已执行”的
    VACUUM 误判为没执行过。
    """
    assert pool is not None
    async with pool.connection() as conn:
        cur = await conn.execute(q.TABLE_STATS, (schema_name, table_name))
        row = await cur.fetchone()
        if row is None:
            raise LookupError(f"relation {schema_name}.{table_name} does not exist")
        row = dict(row)
        if row["relkind"] == "p":
            relation = row["relation"]
            cur = await conn.execute(
                q.TABLE_TREE_STATS,
                (relation, schema_name, table_name, relation),
            )
            tree = await cur.fetchone()
            if tree is not None:
                row = dict(tree)
    return row


async def fetch_blocking_sessions(limit: int = 20) -> list[dict[str, Any]]:
    assert pool is not None
    async with pool.connection() as conn:
        cur = await conn.execute(q.BLOCKING_SNAPSHOTS, (limit,))
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def fetch_prepared_xacts() -> list[dict[str, Any]]:
    assert pool is not None
    async with pool.connection() as conn:
        cur = await conn.execute(q.PREPARED_XACTS)
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def fetch_inactive_slots() -> list[dict[str, Any]]:
    assert pool is not None
    async with pool.connection() as conn:
        cur = await conn.execute(q.REPLICATION_SLOTS)
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


def stats_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    """只保留可 JSON 序列化的统计字段。"""
    keys = (
        "n_dead_tup_estimate",
        "n_live_tup_estimate",
        "vacuum_count",
        "autovacuum_count",
        "last_vacuum",
        "last_autovacuum",
        "relation_size_bytes",
        "total_relation_size_bytes",
    )
    out: dict[str, Any] = {}
    for k in keys:
        v = row.get(k)
        out[k] = v.isoformat() if hasattr(v, "isoformat") else v
    return out


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


async def fetch_job_log_page(
    job_id: int, after_seq: int, limit: int
) -> list[dict[str, Any]]:
    """按游标 (seq) 取一页日志，升序；limit 由调用方限制在 100 以内。"""
    assert pool is not None
    async with pool.connection() as conn:
        cur = await conn.execute(q.READ_LOG_PAGE, (job_id, after_seq, limit))
        rows = await cur.fetchall()
    return [dict(r) for r in rows]


def to_jsonable(obj: Any) -> Any:
    """递归把 datetime/date 等转成可 JSON 序列化的值。"""
    from datetime import date, datetime, timedelta

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, timedelta):
        return obj.total_seconds()
    if isinstance(obj, dict):
        return {k: to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    return obj
