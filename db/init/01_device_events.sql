-- 设备事件表（演示/自测用）。
--
-- 按 device_id 做 HASH 分区（32 个分区），贴近“分区多、VACUUM VERBOSE 通知多”
-- 的生产形态：一次 VACUUM 会逐分区产生大量 INFO 通知。
-- 注意 PG16 不允许在分区父表上设置存储参数，autovacuum_enabled=false
-- 必须逐个叶子分区设置（见下面的 DO 块）。
CREATE TABLE IF NOT EXISTS public.device_events (
    id          bigint GENERATED ALWAYS AS IDENTITY,
    device_id   integer NOT NULL,
    event_type  text NOT NULL,
    payload     jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, device_id)
) PARTITION BY HASH (device_id);

DO $$
DECLARE i int;
BEGIN
    FOR i IN 0..31 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS public.device_events_p%s '
            'PARTITION OF public.device_events '
            'FOR VALUES WITH (MODULUS 32, REMAINDER %s) '
            'WITH (autovacuum_enabled = false)',
            lpad(i::text, 2, '0'), i);
    END LOOP;
END $$;

INSERT INTO public.device_events (device_id, event_type, payload)
SELECT (g % 50) + 1,
       (ARRAY['boot', 'telemetry', 'alarm', 'shutdown'])[(g % 4) + 1],
       jsonb_build_object('seq', g)
  FROM generate_series(1, 100000) AS g;

-- 删除约 4 万行制造死元组（不立刻 VACUUM，留给维护 API 处理）
DELETE FROM public.device_events
 WHERE id IN (SELECT id FROM public.device_events
              WHERE event_type = 'telemetry' LIMIT 40000);

ANALYZE public.device_events;
