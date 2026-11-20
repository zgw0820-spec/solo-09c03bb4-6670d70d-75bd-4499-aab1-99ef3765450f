"""VACUUM VERBOSE 日志“边采集边保存”的端到端自测。

覆盖:
  1. 作业运行中即可按游标分页读取（≤100 条/页），末尾标明 collecting；
  2. 释放阻塞后从原游标续读，能读到完整日志且 seq 连续、无重复；
  3. 日志写入失败时 VACUUM 结论不受影响（以数据库实际执行结果为准），
     已保存的日志保留，状态标为 incomplete；
  4. 服务重启/作业中断后，已保存日志保留且可沿原游标续读；
  5. 旧作业内嵌的 VERBOSE 通知在启动时迁入分页入口；
  6. 游标/分页参数边界。

运行方式同 selftest.py。
"""
from __future__ import annotations

import os
import sys
import time

import psycopg
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import app  # noqa: E402
from app.config import settings  # noqa: E402

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
results: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    results.append((name, bool(cond), detail))
    print(f"  [{PASS if cond else FAIL}] {name}" + (f" — {detail}" if detail else ""))


def admin_conn() -> psycopg.Connection:
    return psycopg.connect(settings.database_url, autocommit=True)


def sqlname(name: str) -> str:
    return f'maintenance."{settings.log_table}"' if name == "log" else name


def make_partitioned_fixture(adm: psycopg.Connection, table: str, nparts: int) -> None:
    adm.execute(f"DROP TABLE IF EXISTS public.{table} CASCADE")
    adm.execute(f"CREATE TABLE public.{table} (id int, pad text) PARTITION BY RANGE (id)")
    for i in range(nparts):
        lo, hi = i * 100, (i + 1) * 100
        part = f"{table}_p{i:02d}"
        adm.execute(
            f"CREATE TABLE public.{part} PARTITION OF public.{table} "
            f"FOR VALUES FROM ({lo}) TO ({hi}) WITH (autovacuum_enabled=false)"
        )
        adm.execute(
            f"INSERT INTO public.{part} SELECT g, repeat('x', 200) "
            f"FROM generate_series({lo}, {hi - 1}) g"
        )
        adm.execute(f"DELETE FROM public.{part} WHERE id % 2 = 0")
    adm.execute(f"ANALYZE public.{table}")


def wait_until(pred, timeout: float = 15.0, interval: float = 0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        val = pred()
        if val:
            return val
        time.sleep(interval)
    return pred()


def read_all_pages(client: TestClient, job_id: int, after_seq: int = 0,
                   max_pages: int = 100) -> tuple[list[dict], dict, int]:
    items: list[dict] = []
    page: dict = {}
    pages = 0
    while pages < max_pages:
        r = client.get(f"/maintenance/jobs/{job_id}/logs",
                       params={"after_seq": after_seq, "limit": 100})
        assert r.status_code == 200, r.text
        page = r.json()
        pages += 1
        items.extend(page["items"])
        after_seq = page["next_seq"]
        if not page["has_more"]:
            break
    return items, page, after_seq


def main() -> int:
    print("DATABASE_URL =", settings.database_url)
    table = "log_fixture"
    with admin_conn() as adm:
        adm.execute("DELETE FROM maintenance.vacuum_jobs")
        adm.execute(f"DELETE FROM maintenance.{settings.log_table}")
        make_partitioned_fixture(adm, table, 60)
        # 锁住 p55：VACUUM 处理到这里会被确定性地卡住，从而能在
        # 作业运行中验证“边写边读”。
        locker = admin_conn()
        locker.execute(f"BEGIN; LOCK TABLE public.{table}_p55 IN SHARE ROW EXCLUSIVE MODE")

    try:
        with TestClient(app) as client:
            # --- 1/2. 运行中读取 + 续读 --------------------------------
            print("\n== 1. 作业运行中即可按游标读取 ==")
            r = client.post("/maintenance/vacuum",
                            json={"table_name": table, "wait": False})
            check("作业受理 202", r.status_code == 202, r.text)
            job_id = r.json()["job_id"]
            check("受理响应即给出日志分页入口",
                  r.json()["log_url"].startswith(
                      f"/maintenance/jobs/{job_id}/logs"))

            got = wait_until(lambda: (
                client.get(f"/maintenance/jobs/{job_id}").json()["logs"]["entries_saved"] > 0
            ), timeout=15)
            check("作业仍在运行时已有日志落库", bool(got))
            job = client.get(f"/maintenance/jobs/{job_id}").json()
            check("作业运行中 status=running 且 log state=collecting",
                  job["status"] == "running" and job["logs"]["state"] == "collecting",
                  str(job["logs"]))

            mid_items, mid_page, mid_cursor = read_all_pages(client, job_id)
            check("运行中可分页读到已保存日志", len(mid_items) > 0,
                  f"{len(mid_items)} 条")
            check("单页不超过 100 条",
                  len(client.get(f"/maintenance/jobs/{job_id}/logs",
                                 params={"after_seq": 0, "limit": 100})
                      .json()["items"]) <= 100)
            check("运行中翻到末尾时明确说明仍在采集",
                  mid_page["has_more"] is False
                  and mid_page["state"] == "collecting"
                  and "仍在采集" in mid_page["end_reason"],
                  mid_page["end_reason"])
            check("已保存条数与摘要一致",
                  mid_page["entries_saved"] >= len(mid_items))
            check("日志序号从 1 开始且连续",
                  [e["seq"] for e in mid_items]
                  == list(range(1, len(mid_items) + 1)),
                  f"first={mid_items[0]['seq'] if mid_items else None} "
                  f"last={mid_items[-1]['seq'] if mid_items else None}")
            page_after = client.get(f"/maintenance/jobs/{job_id}/logs",
                                    params={"after_seq": mid_cursor,
                                            "limit": 100}).json()
            seqs_after = [e["seq"] for e in page_after["items"]]
            check("同一游标续读不重复、不丢（严格接在游标之后）",
                  seqs_after == list(range(mid_cursor + 1,
                                           mid_cursor + 1 + len(seqs_after))),
                  f"{seqs_after[:3]}...")
            again = client.get(f"/maintenance/jobs/{job_id}/logs",
                               params={"after_seq": mid_cursor, "limit": 100}).json()
            again2 = client.get(f"/maintenance/jobs/{job_id}/logs",
                                params={"after_seq": mid_cursor, "limit": 100}).json()
            first_seqs = [e["seq"] for e in again["items"]]
            check("同一游标两次读取不重复（新写入只会追加在游标之后）",
                  [e["seq"] for e in again2["items"]][:len(first_seqs)]
                  == first_seqs)

            # 释放阻塞，等作业结束，从原游标续读剩余部分
            locker.rollback()
            locker.close()

            def done():
                j = client.get(f"/maintenance/jobs/{job_id}").json()
                return j if j["status"] != "running" else None

            job = wait_until(done, timeout=60)
            check("释放阻塞后作业成功", bool(job) and job["status"] == "succeeded",
                  (job or {}).get("error_message") or "")

            rest, last_page, _ = read_all_pages(client, job_id, after_seq=mid_cursor)
            all_items = mid_items + rest
            check("续读得到完整日志且状态 complete",
                  last_page["state"] == "complete"
                  and last_page["has_more"] is False
                  and "已全部收齐" in last_page["end_reason"],
                  last_page["end_reason"])
            check("全部日志 seq 连续无缺口、无重复",
                  [e["seq"] for e in all_items]
                  == list(range(1, len(all_items) + 1)),
                  f"共 {len(all_items)} 条")
            check("日志总条数不少于摘要统计",
                  len(all_items) >= job["logs"]["entries_saved"],
                  f"页面合计 {len(all_items)} / 摘要 {job['logs']['entries_saved']}")
            with admin_conn() as adm:
                n_db = adm.execute(
                    f"SELECT count(*) FROM maintenance.{settings.log_table} "
                    "WHERE job_id=%s", (job_id,)).fetchone()[0]
            check("数据库实际保存条数与分页读取一致",
                  n_db == len(all_items), f"{n_db} vs {len(all_items)}")
            check("日志中含 VACUUM VERBOSE 内容",
                  any("vacuuming" in e["message"] or "scanned" in e["message"]
                      for e in all_items))

            # --- 3. 日志写入失败不影响 VACUUM 结论 ---------------------
            print("\n== 2. 日志写入失败：已保存部分保留，VACUUM 结论照实 ==")
            fail_table = "log_fail_fixture"
            with admin_conn() as adm:
                make_partitioned_fixture(adm, fail_table, 40)
                # 分区表的统计记在各叶子分区上，父表 vacuum_count 恒为 0，
                # 因此按叶子求和作为“数据库实际执行”的证据。
                before_vc = adm.execute(
                    "SELECT coalesce(sum(vacuum_count), 0) "
                    "FROM pg_stat_user_tables WHERE relname LIKE %s",
                    (fail_table + "\\_p%",)).fetchone()[0]
            fail_locker = admin_conn()
            fail_locker.execute(
                f"BEGIN; LOCK TABLE public.{fail_table}_p35 "
                "IN SHARE ROW EXCLUSIVE MODE")
            try:
                r = client.post("/maintenance/vacuum",
                                json={"table_name": fail_table, "wait": False})
                fail_job = r.json()["job_id"]
                # 等第一批日志落库后，把日志表改名，使后续写入失败
                started = wait_until(lambda: client.get(
                    f"/maintenance/jobs/{fail_job}").json()["logs"]
                    ["entries_saved"] > 0, timeout=30)
                check("故障注入前已有日志落库", bool(started))
                saved_before = client.get(
                    f"/maintenance/jobs/{fail_job}").json()["logs"]["entries_saved"]
                renamed = _rename_log_table("away")
                check("日志表被移走（模拟写失败）", renamed)
            finally:
                fail_locker.rollback()
                fail_locker.close()

            def fail_done():
                j = client.get(f"/maintenance/jobs/{fail_job}").json()
                return j if j["status"] != "running" else None
            fjob = wait_until(fail_done, timeout=60)
            check("日志写入失败不改变 VACUUM 成败结论",
                  bool(fjob) and fjob["status"] == "succeeded",
                  (fjob or {}).get("error_message") or "")
            check("日志状态标记为 incomplete 并说明原因",
                  bool(fjob) and fjob["logs"]["state"] == "incomplete"
                  and fjob["logs"]["error"],
                  str((fjob or {}).get("logs")))
            check("已丢弃条数有记录",
                  bool(fjob) and fjob["logs"]["entries_dropped"] > 0,
                  str((fjob or {}).get("logs")))
            with admin_conn() as adm:
                after_vc = adm.execute(
                    "SELECT coalesce(sum(vacuum_count), 0) "
                    "FROM pg_stat_user_tables WHERE relname LIKE %s",
                    (fail_table + "\\_p%",)).fetchone()[0]
            check("数据库侧 vacuum_count 确实增加（实际执行结果为准）",
                  after_vc >= before_vc + 40, f"{before_vc} -> {after_vc}")

            # 写失败期间读日志页也应可用（读的是日志表，此时已被移走）：
            # 先恢复表名再验证已保存日志仍在。
            check("日志表恢复", _rename_log_table("back"))
            items, page, _ = read_all_pages(client, fail_job)
            check("失败前已保存的日志仍可完整读取",
                  len(items) >= saved_before
                  and len(items) == fjob["logs"]["entries_saved"]
                  and page["state"] == "incomplete"
                  and "采集不完整" in page["end_reason"],
                  f"{len(items)} 条（故障前至少 {saved_before}）/ "
                  f"摘要 {fjob['logs']['entries_saved']} / {page['end_reason']}")

            # --- 6. 参数边界 -----------------------------------------
            print("\n== 3. 分页参数边界 ==")
            check("limit=0 被拒绝", client.get(
                f"/maintenance/jobs/{job_id}/logs",
                params={"limit": 0}).status_code == 422)
            check("limit=101 被拒绝", client.get(
                f"/maintenance/jobs/{job_id}/logs",
                params={"limit": 101}).status_code == 422)
            check("负游标被拒绝", client.get(
                f"/maintenance/jobs/{job_id}/logs",
                params={"after_seq": -1}).status_code == 422)
            check("不存在的作业 404", client.get(
                "/maintenance/jobs/999999/logs").status_code == 404)
            check("旧作业无日志时返回空页", client.get(
                f"/maintenance/jobs/{fail_job}/logs",
                params={"after_seq": 10**9, "limit": 100}).json()["items"] == [])

    finally:
        try:
            locker.close()
        except Exception:
            pass

    # --- 4. 服务重启后中断作业: 日志保留 + 原游标续读 ---------------
    print("\n== 4. 重启/中断后日志保留并可沿原游标续读 ==")
    with admin_conn() as adm:
        orphan = adm.execute(
            "INSERT INTO maintenance.vacuum_jobs "
            "(table_schema, table_name, status, statement_timeout_ms, "
            " before_snapshot, log_state, log_entries) "
            "VALUES ('public', %s, 'running', 0, '{}'::jsonb, 'collecting', 3) "
            "RETURNING id", (table,)).fetchone()[0]
        for seq in (1, 2, 3):
            adm.execute(
                f"INSERT INTO maintenance.{settings.log_table} "
                "(job_id, seq, severity, message) VALUES (%s, %s, 'INFO', %s)",
                (orphan, seq, f"partial log {seq}"))

    with TestClient(app) as client2:
        body = client2.get(f"/maintenance/jobs/{orphan}").json()
        check("重启后作业标记 interrupted", body["status"] == "interrupted")
        check("日志状态变为 incomplete 且保留已保存条数",
              body["logs"]["state"] == "incomplete"
              and body["logs"]["entries_saved"] == 3,
              str(body["logs"]))
        first = client2.get(f"/maintenance/jobs/{orphan}/logs",
                            params={"after_seq": 0, "limit": 2}).json()
        check("中断作业的日志可读且分页正常",
              [e["message"] for e in first["items"]]
              == ["partial log 1", "partial log 2"]
              and first["has_more"] and first["next_seq"] == 2)
        rest = client2.get(f"/maintenance/jobs/{orphan}/logs",
                           params={"after_seq": 2, "limit": 100}).json()
        check("沿原游标续读拿到剩余部分并标明不完整",
              [e["message"] for e in rest["items"]] == ["partial log 3"]
              and rest["state"] == "incomplete"
              and "采集不完整" in rest["end_reason"],
              rest["end_reason"])

        # --- 5. 旧作业内嵌日志迁移 --------------------------------
        print("\n== 5. 旧作业内嵌日志从分页入口读 ==")
        with admin_conn() as adm:
            legacy = adm.execute(
                "INSERT INTO maintenance.vacuum_jobs "
                "(table_schema, table_name, status, statement_timeout_ms, "
                " before_snapshot, result, finished_at) "
                "VALUES ('public', %s, 'succeeded', 0, '{}'::jsonb, "
                " %s::jsonb, now()) RETURNING id",
                (table, '{"vacuum_verbose_notices": '
                        '["INFO: vacuuming \\"public.x\\"", '
                        '"INFO: finished vacuuming \\"public.x\\""], '
                        '"duration_ms": 1}')).fetchone()[0]

    with TestClient(app) as client3:
        body = client3.get(f"/maintenance/jobs/{legacy}").json()
        check("旧作业详情不再内嵌日志",
              "vacuum_verbose_notices" not in (body.get("result") or {}))
        check("旧作业日志摘要为 complete 且条数正确",
              body["logs"]["state"] == "complete"
              and body["logs"]["entries_saved"] == 2,
              str(body["logs"]))
        page = client3.get(f"/maintenance/jobs/{legacy}/logs",
                           params={"after_seq": 0, "limit": 100}).json()
        check("旧作业内嵌日志已迁入分页入口",
              [e["message"] for e in page["items"]]
              == ['vacuuming "public.x"',
                  'finished vacuuming "public.x"']
              and [e["severity"] for e in page["items"]] == ["INFO", "INFO"],
              str(page["items"]))

    print("\n" + "=" * 60)
    failed = [n for n, ok, _ in results if not ok]
    for n, ok, detail in results:
        print(f"  [{PASS if ok else FAIL}] {n}"
              + (f" — {detail}" if not ok and detail else ""))
    print(f"\n合计 {len(results)} 项, 通过 {len(results)-len(failed)}, 失败 {len(failed)}")
    return 1 if failed else 0


def _rename_log_table(direction: str) -> bool:
    """把日志表改名/改回，用于确定性地制造写入失败。"""
    src, dst = (settings.log_table, settings.log_table + "_away") \
        if direction == "away" else (settings.log_table + "_away", settings.log_table)
    try:
        with admin_conn() as adm:
            adm.execute(
                f'ALTER TABLE maintenance."{src}" RENAME TO "{dst}"')
        return True
    except Exception:
        return False


if __name__ == "__main__":
    sys.exit(main())
