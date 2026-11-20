"""全部 SQL 集中在此，动态标识符用 psycopg.sql 安全拼接。

注意: n_dead_tup / n_live_tup 是 *planner 的估算值*，不是精确计数，
也不能被解读为"可释放磁盘字节"。
"""
from __future__ import annotations

from psycopg import sql

from .config import settings


def _q(ident: str) -> sql.Composed:
    return sql.Identifier(ident)


# --- 维护作业表 / 审计表 -----------------------------------------------------

CREATE_SCHEMA = sql.SQL("CREATE SCHEMA IF NOT EXISTS {sch}").format(
    sch=_q(settings.job_schema)
)

CREATE_JOB_TABLE = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {sch}.{tbl} (
        id              bigserial PRIMARY KEY,
        table_schema    text        NOT NULL,
        table_name      text        NOT NULL,
        status          text        NOT NULL CHECK (status IN
                            ('running','succeeded','failed','interrupted')),
        started_at      timestamptz NOT NULL DEFAULT now(),
        finished_at     timestamptz,
        statement_timeout_ms integer NOT NULL,
        -- VACUUM 启动前的统计基线，用于重启后核实是否真的跑过
        before_snapshot jsonb       NOT NULL,
        result          jsonb,
        error_message   text
    )
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

# 同一张表同一时刻只允许一个 running 作业（跨进程/跨重启生效）
CREATE_JOB_UNIQUE = sql.SQL(
    """
    CREATE UNIQUE INDEX IF NOT EXISTS {idx}
        ON {sch}.{tbl} (table_schema, table_name)
        WHERE status = 'running'
    """
).format(
    idx=_q(f"{settings.job_table}_one_running_per_table"),
    sch=_q(settings.job_schema),
    tbl=_q(settings.job_table),
)

# 日志采集/落库状态直接挂在作业行上，列表与详情只读这些摘要字段，
# 不需要扫日志明细表。
#   collecting  作业运行中，日志仍在采集
#   complete    作业已结束且日志已全部收齐
#   incomplete  作业中断，或写入失败/有丢弃，已保存的部分保留
ALTER_JOB_LOG_COLUMNS = [
    sql.SQL(
        "ALTER TABLE {sch}.{tbl} ADD COLUMN IF NOT EXISTS "
        "log_state text NOT NULL DEFAULT 'collecting'"
    ).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table)),
    sql.SQL(
        "ALTER TABLE {sch}.{tbl} ADD COLUMN IF NOT EXISTS "
        "log_entries bigint NOT NULL DEFAULT 0"
    ).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table)),
    sql.SQL(
        "ALTER TABLE {sch}.{tbl} ADD COLUMN IF NOT EXISTS "
        "log_dropped bigint NOT NULL DEFAULT 0"
    ).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table)),
    sql.SQL(
        "ALTER TABLE {sch}.{tbl} ADD COLUMN IF NOT EXISTS log_error text"
    ).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table)),
]

# 逐条持久化的 VACUUM VERBOSE 日志。(job_id, seq) 主键同时是游标分页索引，
# seq 由采集端单调递增分配；ON CONFLICT 保证重放/重试幂等。
CREATE_LOG_TABLE = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {sch}.{log} (
        job_id    bigint      NOT NULL,
        seq       bigint      NOT NULL,
        logged_at timestamptz NOT NULL DEFAULT now(),
        severity  text        NOT NULL,
        message   text        NOT NULL,
        PRIMARY KEY (job_id, seq)
    )
    """
).format(sch=_q(settings.job_schema), log=_q(settings.log_table))

INSERT_LOG_ENTRY = sql.SQL(
    """
    INSERT INTO {sch}.{log} (job_id, seq, logged_at, severity, message)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (job_id, seq) DO NOTHING
    """
).format(sch=_q(settings.job_schema), log=_q(settings.log_table))

# 进度用“累计值覆盖”而不是自增：批写入若提交成功但响应丢失会重试，
# ON CONFLICT DO NOTHING 保证行不重复，累计值覆盖保证计数也不重复。
UPDATE_JOB_LOG_PROGRESS = sql.SQL(
    """
    UPDATE {sch}.{tbl}
       SET log_entries = %s, log_dropped = %s, log_error = %s
     WHERE id = %s AND status = 'running'
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

READ_LOG_PAGE = sql.SQL(
    """
    SELECT seq, logged_at, severity, message
      FROM {sch}.{log}
     WHERE job_id = %s AND seq > %s
     ORDER BY seq
     LIMIT %s
    """
).format(sch=_q(settings.job_schema), log=_q(settings.log_table))

# 旧版本把 VERBOSE 通知整段塞在 result->'vacuum_verbose_notices' 里。
# 启动时把它们搬进日志明细表，使旧作业也能从同一分页入口读取。
MIGRATE_LEGACY_NOTICES = sql.SQL(
    """
    INSERT INTO {sch}.{log} (job_id, seq, severity, message)
    SELECT j.id,
           s.ord,
           CASE WHEN s.elem ~ '^(INFO|WARNING|NOTICE|ERROR|LOG|DEBUG): '
                THEN split_part(s.elem, ': ', 1)
                ELSE 'INFO' END,
           regexp_replace(
               s.elem, '^(INFO|WARNING|NOTICE|ERROR|LOG|DEBUG): ', '')
      FROM {sch}.{tbl} j
      CROSS JOIN LATERAL jsonb_array_elements_text(
               CASE WHEN jsonb_typeof(j.result->'vacuum_verbose_notices')
                         = 'array'
                    THEN j.result->'vacuum_verbose_notices'
                    ELSE '[]'::jsonb END) WITH ORDINALITY
               AS s(elem, ord)
     WHERE j.result IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM {sch}.{log} l WHERE l.job_id = j.id)
    ON CONFLICT (job_id, seq) DO NOTHING
    """
).format(
    sch=_q(settings.job_schema),
    tbl=_q(settings.job_table),
    log=_q(settings.log_table),
)

RECOUNT_JOB_LOG_ENTRIES = sql.SQL(
    """
    UPDATE {sch}.{tbl} j
       SET log_entries = c.n
      FROM (SELECT job_id, count(*) AS n
              FROM {sch}.{log} GROUP BY job_id) c
     WHERE c.job_id = j.id
       AND j.log_entries <> c.n
    """
).format(
    sch=_q(settings.job_schema),
    tbl=_q(settings.job_table),
    log=_q(settings.log_table),
)

# 旧版本作业没有 log_state（默认 collecting）。进程在旧版本里已结束时，
# 其 VERBOSE 通知要么已随 result 完整落库（已迁移），要么作业是 interrupted。
# 只处理已结束的作业；运行中的作业由启动扫描先改成 interrupted。
NORMALIZE_LEGACY_LOG_STATE = sql.SQL(
    """
    UPDATE {sch}.{tbl}
       SET log_state = CASE WHEN status = 'interrupted'
                            THEN 'incomplete' ELSE 'complete' END,
           log_error  = CASE WHEN status = 'interrupted'
                             THEN COALESCE(log_error,
                                  '作业在服务重启时被标记为 interrupted，'
                                  '日志可能未收齐')
                             ELSE log_error END
     WHERE log_state = 'collecting'
       AND status IN ('succeeded', 'failed', 'interrupted')
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

# 列表只取状态/统计摘要，不取 result 整包，更不取日志明细。
# after_stats 是从 result 中抽出的小对象（before/after 统计），供值班员概览。
LIST_JOB_SUMMARIES = sql.SQL(
    """
    SELECT id, table_schema, table_name, status, started_at, finished_at,
           statement_timeout_ms, error_message,
           log_state, log_entries, log_dropped, log_error,
           result->'after' AS after_stats
      FROM {sch}.{tbl}
     WHERE table_schema = %s AND table_name = %s
     ORDER BY id DESC
     LIMIT %s
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

CREATE_AUDIT_TABLE = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {sch}.{tbl} (
        id              bigserial PRIMARY KEY,
        occurred_at     timestamptz NOT NULL DEFAULT now(),
        target_pid      integer     NOT NULL,
        claimed_backend_start timestamptz NOT NULL,
        claimed_xact_start    timestamptz NOT NULL,
        decision        text        NOT NULL,
        terminated      boolean     NOT NULL,
        observed_state  jsonb       NOT NULL,
        detail          text
    )
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.audit_table))

# 服务重启/崩溃恢复: 把没有结局的作业标为 interrupted。
# 同时把日志状态改成 incomplete —— 已落库的日志一条不删，游标仍可续读。
SWEEP_INTERRUPTED = sql.SQL(
    """
    UPDATE {sch}.{tbl}
       SET status = 'interrupted',
           finished_at = now(),
           error_message = '进程退出或服务重启，VACUUM 结果未经确认；可凭此作业核实或重试',
           log_state = 'incomplete',
           log_error = COALESCE(log_error,
                                '进程退出或服务重启，日志未收齐；已保存部分保留')
     WHERE status = 'running'
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

INSERT_JOB = sql.SQL(
    """
    INSERT INTO {sch}.{tbl}
        (table_schema, table_name, status, statement_timeout_ms, before_snapshot)
    VALUES (%s, %s, 'running', %s, %s)
    RETURNING id
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

FINISH_JOB = sql.SQL(
    """
    UPDATE {sch}.{tbl}
       SET status = %s, finished_at = now(), result = %s, error_message = %s,
           log_entries = %s, log_state = %s, log_dropped = %s, log_error = %s
     WHERE id = %s AND status = 'running'
    RETURNING id
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

# 服务关停等主动取消: 只落“未确认完成”，不碰已有日志
MARK_INTERRUPTED = sql.SQL(
    """
    UPDATE {sch}.{tbl}
       SET status = 'interrupted', finished_at = now(),
           error_message = %s, log_entries = %s,
           log_state = 'incomplete', log_dropped = %s, log_error = %s
     WHERE id = %s AND status = 'running'
    RETURNING id
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

GET_JOB = sql.SQL(
    "SELECT * FROM {sch}.{tbl} WHERE id = %s"
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

# 单作业摘要（详情接口用）：不带日志明细，日志从分页入口读。
GET_JOB_SUMMARY = sql.SQL(
    """
    SELECT j.id, j.table_schema, j.table_name, j.status,
           j.started_at, j.finished_at, j.statement_timeout_ms,
           j.before_snapshot,
           -- 详情只保留结果摘要：剔除旧版本内嵌的日志明细
           CASE WHEN j.result IS NULL THEN NULL
                ELSE j.result - 'vacuum_verbose_notices' END AS result,
           j.error_message,
           j.log_state, j.log_entries, j.log_dropped, j.log_error
      FROM {sch}.{tbl} j
     WHERE j.id = %s
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.job_table))

# --- 业务表诊断 --------------------------------------------------------------

# 表存在性
RELATION_EXISTS = sql.SQL(
    """
    SELECT relpersistence
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = %s AND c.relname = %s
    """
)

# 死元组等统计: 估算值
TABLE_STATS = sql.SQL(
    """
    SELECT current_database() AS db_name,
           n.nspname AS table_schema,
           c.relname AS table_name,
           c.oid::regclass::text AS relation,
           c.relkind::text AS relkind,
           pg_stat_get_dead_tuples(c.oid)  AS n_dead_tup_estimate,
           pg_stat_get_live_tuples(c.oid)  AS n_live_tup_estimate,
           pg_stat_get_vacuum_count(c.oid) AS vacuum_count,
           pg_stat_get_autovacuum_count(c.oid)      AS autovacuum_count,
           stat.last_vacuum,
           stat.last_autovacuum,
           pg_relation_size(c.oid)                 AS relation_size_bytes,
           pg_total_relation_size(c.oid)           AS total_relation_size_bytes,
           GREATEST(pg_stat_get_dead_tuples(c.oid), 0)
               / NULLIF(pg_stat_get_live_tuples(c.oid)
                        + pg_stat_get_dead_tuples(c.oid), 0)
               AS dead_tuple_ratio_estimate,
           stat.last_analyze,
           stat.last_autoanalyze
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      LEFT JOIN pg_stat_user_tables stat
             ON stat.relid = c.oid
     WHERE n.nspname = %s AND c.relname = %s
    """
)

# 分区表的统计按叶子分区汇总。PG16 中父表（relkind='p'）没有存储，
# pg_stat_get_* 与 pg_relation_size 恒为 0；只看父表会把“已执行”误判为“没执行”。
TABLE_TREE_STATS = sql.SQL(
    """
    WITH leaves AS (
        SELECT relid
          FROM pg_partition_tree(%s::regclass)
         WHERE isleaf
    )
    SELECT current_database() AS db_name,
           %s AS table_schema,
           %s AS table_name,
           %s::regclass::text AS relation,
           'p' AS relkind,
           coalesce(sum(pg_stat_get_dead_tuples(l.relid)), 0)::bigint
               AS n_dead_tup_estimate,
           coalesce(sum(pg_stat_get_live_tuples(l.relid)), 0)::bigint
               AS n_live_tup_estimate,
           coalesce(sum(pg_stat_get_vacuum_count(l.relid)), 0)::bigint
               AS vacuum_count,
           coalesce(sum(pg_stat_get_autovacuum_count(l.relid)), 0)::bigint
               AS autovacuum_count,
           max(stat.last_vacuum)       AS last_vacuum,
           max(stat.last_autovacuum)   AS last_autovacuum,
           coalesce(sum(pg_relation_size(l.relid)), 0)::bigint
               AS relation_size_bytes,
           coalesce(sum(pg_total_relation_size(l.relid)), 0)::bigint
               AS total_relation_size_bytes,
           GREATEST(coalesce(sum(pg_stat_get_dead_tuples(l.relid)), 0), 0)
               / NULLIF(coalesce(sum(pg_stat_get_live_tuples(l.relid)), 0)
                        + coalesce(sum(pg_stat_get_dead_tuples(l.relid)), 0), 0)
               AS dead_tuple_ratio_estimate,
           max(stat.last_analyze)      AS last_analyze,
           max(stat.last_autoanalyze)  AS last_autoanalyze,
           count(*)::int               AS partition_count
      FROM leaves l
      LEFT JOIN pg_stat_user_tables stat ON stat.relid = l.relid
    """
)

# 可能阻碍死元组回收的旧快照会话:
# 判据是"事务仍开启"（xact_start 非空且非本维护进程）。
# 注意 READ COMMITTED 下语句结束后快照会注销、backend_xmin 可能为 NULL，
# 但只要事务还开着，下一条语句即可重新持有旧 xmin 视角的快照，
# 因此 idle in transaction 会话仍是最常见的回收阻碍，均列出并标明状态。
BLOCKING_SNAPSHOTS = sql.SQL(
    """
    SELECT pid,
           usename              AS role_name,
           application_name,
           client_addr,
           state,
           backend_start,
           xact_start,
           state_change,
           now() - xact_start   AS xact_age,
           now() - backend_start AS backend_age,
           backend_xid,
           backend_xmin,
           left(query, 200)     AS current_query
      FROM pg_stat_activity
     WHERE datname = current_database()
       AND pid <> pg_backend_pid()
       AND xact_start IS NOT NULL
       AND state <> 'idle'
     ORDER BY xact_start, backend_start
     LIMIT %s
    """
)

# 其它同样可能让 VACUUM 无法回收的因素，一并呈现
PREPARED_XACTS = sql.SQL(
    """
    SELECT transaction AS gid, owner, database,
           prepared, now() - prepared AS age
      FROM pg_prepared_xacts
     WHERE database = current_database()
     ORDER BY prepared
    """
)

REPLICATION_SLOTS = sql.SQL(
    """
    SELECT slot_name, slot_type, active, xmin, catalog_xmin,
           pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal_bytes
      FROM pg_replication_slots
     WHERE NOT active
     ORDER BY slot_name
    """
)

# 只能结束: 同库 + PID + backend_start + xact_start 全部吻合 +
# 仍是 idle in transaction 的原会话，任一变化即拒绝。
RECHECK_SESSION = sql.SQL(
    """
    SELECT pid, datname, state, backend_start, xact_start,
           usename AS role_name, application_name, client_addr,
           left(query, 200) AS current_query
      FROM pg_stat_activity
     WHERE pid = %s
       AND datname = current_database()
       AND backend_start = %s
       AND xact_start = %s
    """
)

AUDIT_TERMINATION = sql.SQL(
    """
    INSERT INTO {sch}.{tbl}
        (target_pid, claimed_backend_start, claimed_xact_start,
         decision, terminated, observed_state, detail)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
).format(sch=_q(settings.job_schema), tbl=_q(settings.audit_table))
