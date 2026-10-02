WITH observed AS (
 SELECT m.platform,m.job_group,m.slot_ts,
  coalesce(m.reported_total,CASE WHEN m.is_complete THEN m.items_unique END)::numeric AS inventory,
  '표시 총건수/전수 ID'::text AS definition FROM v_listing_scope m WHERE job_group<>'MIX' AND status='ok'
 UNION ALL
 SELECT m.platform,g.job_group,m.slot_ts,
  CASE WHEN m.is_complete THEN (SELECT count(DISTINCT s.posting_id) FROM v_snapshot_scope s
    WHERE s.run_key=m.run_key AND s.platform=m.platform AND s.job_group=g.job_group) END::numeric,
  '혼합 목록 전수의 직무 분류' FROM v_listing_scope m CROSS JOIN (VALUES('BE'),('DA'),('DE')) g(job_group)
 WHERE m.job_group='MIX' AND m.status='ok'
), grid AS (
 SELECT s.slot_ts,k.platform,k.job_group,o.inventory,o.definition
 FROM generate_series(timestamptz '2026-10-02 09:00+09',timestamptz '2026-10-02 20:00+09',interval '10 minutes') s(slot_ts)
 CROSS JOIN (SELECT DISTINCT platform,job_group FROM observed) k
 LEFT JOIN observed o USING(slot_ts,platform,job_group)
), w AS (
 SELECT *,inventory-lag(inventory) OVER(PARTITION BY platform,job_group ORDER BY slot_ts) AS delta_10min,
  CASE WHEN count(inventory) OVER(PARTITION BY platform,job_group ORDER BY slot_ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)=3
   THEN avg(inventory) OVER(PARTITION BY platform,job_group ORDER BY slot_ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) END AS ma30,
  first_value(inventory) OVER(PARTITION BY platform,job_group ORDER BY (inventory IS NULL),slot_ts) AS baseline FROM grid
) SELECT *,100*inventory/nullif(baseline,0) AS idx100 FROM w ORDER BY platform,job_group,slot_ts;
