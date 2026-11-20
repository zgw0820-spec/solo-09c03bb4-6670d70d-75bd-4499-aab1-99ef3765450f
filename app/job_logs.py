"""VACUUM (VERBOSE) 日志：有界流式采集 + 逐批持久化。

设计要点
--------
* 通知回调只做“分配序号 + 放入有界队列”，不做 I/O。VACUUM 在事件循环里
  阻塞等待时，通知仍会逐条送达；队列容量固定，内存占用与日志累计条数无关。
* 独立的写线程用同步 psycopg 连接把队列里的日志分批 INSERT 到
  ``maintenance.<log_table>``，因此作业运行中就能按游标读到已保存的部分。
* 队列满、写库持续失败时只丢弃“当前这条/这一批”，并累计 dropped 计数、
  记录错误；已成功写入的行一律保留（绝不因为后续失败而回滚或删除）。
* 关闭时先排空队列、等最后一批落库，再把最终计数交给作业收尾环节，
  据此标记日志状态 collecting / complete / incomplete。
"""
from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

from . import sql_queries as q
from .config import settings

_STOP = object()


@dataclass(frozen=True)
class LogEntry:
    seq: int
    logged_at: datetime
    severity: str
    message: str


@dataclass
class CollectorStats:
    enqueued: int = 0
    persisted: int = 0
    dropped: int = 0
    write_errors: int = 0
    overflowed: bool = False
    last_error: str | None = None
    writer_alive_after_close: bool = False

    @property
    def log_state(self) -> str:
        if self.dropped or self.write_errors or self.overflowed \
                or self.writer_alive_after_close:
            return "incomplete"
        return "complete"

    def error_text(self) -> str | None:
        parts: list[str] = []
        if self.overflowed:
            parts.append(
                f"采集队列已满，丢弃了 {self.dropped} 条日志"
                f"（内存占用固定，不随日志总量增长）"
            )
        elif self.dropped:
            parts.append(f"日志写入失败，丢弃了 {self.dropped} 条日志")
        if self.write_errors and not self.overflowed:
            parts.append(f"日志写入失败 {self.write_errors} 次")
        if self.writer_alive_after_close:
            parts.append("关闭时日志写线程未在超时内结束")
        if self.last_error:
            parts.append(f"最近错误: {self.last_error}")
        return "; ".join(parts) if parts else None


@dataclass
class _Shared:
    """采集端与写线程共享的计数，统一用一把锁保护。"""

    next_seq: int = 1
    closing: bool = False
    stats: CollectorStats = field(default_factory=CollectorStats)
    lock: threading.Lock = field(default_factory=threading.Lock)


class VacuumLogCollector:
    """把一条 VACUUM 连接的 VERBOSE 通知逐条保存到数据库。"""

    def __init__(
        self,
        job_id: int,
        *,
        queue_size: int | None = None,
        batch_size: int | None = None,
        flush_interval_ms: int | None = None,
    ) -> None:
        self.job_id = job_id
        self._queue: queue.Queue[Any] = queue.Queue(
            maxsize=max(1, queue_size if queue_size is not None
                        else settings.log_queue_size)
        )
        self._batch_size = max(1, batch_size if batch_size is not None
                               else settings.log_batch_size)
        self._flush_interval = max(
            1, flush_interval_ms if flush_interval_ms is not None
            else settings.log_flush_interval_ms
        ) / 1000.0
        self._shared = _Shared()
        self._thread: threading.Thread | None = None
        self._close_lock = threading.Lock()
        self._closed = False

    # --- 采集端 ---------------------------------------------------------
    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._writer_loop,
            name=f"vacuum-log-writer-{self.job_id}",
            daemon=True,
        )
        self._thread.start()

    def handle_notice(self, notice: psycopg.errors.Diagnostic) -> None:
        """psycopg 通知回调：只入队，不阻塞、不写库。"""
        severity = (
            getattr(notice, "severity_nonlocalized", None)
            or getattr(notice, "severity", None)
            or "INFO"
        )
        message = getattr(notice, "message_primary", None) or ""
        with self._shared.lock:
            # 与 close() 的 shutting 标记在同一把锁内判定，避免关闭后仍入队
            if self._shared.closing:
                return
            seq = self._shared.next_seq
            self._shared.next_seq += 1
            entry = LogEntry(
                seq, datetime.now(timezone.utc), severity, message
            )
            try:
                self._queue.put_nowait(entry)
            except queue.Full:
                self._shared.stats.dropped += 1
                self._shared.stats.overflowed = True
                self._shared.stats.last_error = (
                    f"日志采集队列已满（容量 {self._queue.maxsize}），"
                    "该条日志被丢弃"
                )
                return
            self._shared.stats.enqueued += 1

    # --- 写线程 ---------------------------------------------------------
    def _writer_loop(self) -> None:
        conn: psycopg.Connection | None = None
        try:
            while True:
                try:
                    first = self._queue.get(timeout=self._flush_interval)
                except queue.Empty:
                    with self._shared.lock:
                        if self._shared.closing:
                            break
                    continue
                if first is _STOP:
                    break

                batch = [first]
                stop_after = False
                while len(batch) < self._batch_size:
                    try:
                        item = self._queue.get_nowait()
                    except queue.Empty:
                        break
                    if item is _STOP:
                        stop_after = True
                        break
                    batch.append(item)

                conn = self._write_batch(conn, batch)
                if stop_after:
                    break
        except Exception as exc:  # 线程意外退出也要让作业知道日志不完整
            with self._shared.lock:
                self._shared.stats.write_errors += 1
                self._shared.stats.last_error = (
                    f"日志写线程异常退出: {type(exc).__name__}: {exc}"
                )
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(
            settings.database_url,
            autocommit=False,
            row_factory=dict_row,
        )

    def _write_batch(
        self, conn: psycopg.Connection | None, batch: list[LogEntry]
    ) -> psycopg.Connection | None:
        rows = [
            (self.job_id, e.seq, e.logged_at, e.severity, e.message)
            for e in batch
        ]
        last_exc: Exception | None = None
        for _attempt in range(2):
            try:
                if conn is None or conn.closed:
                    conn = self._connect()
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.executemany(q.INSERT_LOG_ENTRY, rows)
                    # 作业仍是 running 时只推进进度计数；最终状态由收尾环节写。
                    saved, dropped, err = self._progress(len(batch))
                    conn.execute(
                        q.UPDATE_JOB_LOG_PROGRESS,
                        (saved, dropped, err, self.job_id),
                    )
                with self._shared.lock:
                    self._shared.stats.persisted += len(batch)
                return conn
            except Exception as exc:  # 连接/表结构/权限等任何写失败
                last_exc = exc
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                conn = None

        # 两次都失败：只丢这一批，已保存的行原样保留，作业继续执行
        with self._shared.lock:
            self._shared.stats.write_errors += 1
            self._shared.stats.dropped += len(batch)
            self._shared.stats.last_error = (
                f"{type(last_exc).__name__}: {last_exc}"
            )
        return conn

    def _progress(self, pending: int) -> tuple[int, int, str | None]:
        """返回（已落库条数, 丢弃条数, 错误说明），均不含被丢弃的批次。"""
        with self._shared.lock:
            return (
                self._shared.stats.persisted + pending,
                self._shared.stats.dropped,
                self._shared.stats.error_text(),
            )

    # --- 收尾 -----------------------------------------------------------
    def close(self) -> CollectorStats:
        """停止接收、排空队列并等最后一批落库；可重复调用。"""
        with self._close_lock:
            if not self._closed:
                self._closed = True
                with self._shared.lock:
                    self._shared.closing = True
                if self._thread is None:
                    return self.snapshot()
                deadline = time.monotonic() + settings.log_close_timeout_seconds
                while True:
                    try:
                        self._queue.put(_STOP, timeout=0.1)
                        break
                    except queue.Full:
                        if time.monotonic() >= deadline:
                            with self._shared.lock:
                                self._shared.stats.writer_alive_after_close = True
                                self._shared.stats.last_error = (
                                    "关闭时队列仍未排空"
                                )
                            break
                self._thread.join(
                    timeout=max(1.0, settings.log_close_timeout_seconds)
                )
                if self._thread.is_alive():
                    with self._shared.lock:
                        self._shared.stats.writer_alive_after_close = True
                        self._shared.stats.last_error = (
                            "日志写线程未在超时内退出"
                        )
        return self.snapshot()

    def snapshot(self) -> CollectorStats:
        with self._shared.lock:
            return CollectorStats(**vars(self._shared.stats))
