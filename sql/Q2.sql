WITH fulls AS (
 SELECT *,lag(run_key) OVER(PARTITION BY platform,query_key ORDER BY slot_ts) AS prev_run,
  lag(slot_ts) OVER(PARTITION BY platform,query_key ORDER BY slot_ts) AS prev_slot
 FROM v_listing_scope WHERE is_complete
), counts AS (
 SELECT f.platform,f.query_key,f.job_group,f.slot_ts,f.prev_slot,
  (SELECT count(DISTINCT posting_id) FROM v_snapshot_scope s WHERE s.run_key=f.run_key AND s.platform=f.platform AND s.query_key=f.query_key) AS current_n,
  (SELECT count(DISTINCT posting_id) FROM v_snapshot_scope s WHERE s.run_key=f.prev_run AND s.platform=f.platform AND s.query_key=f.query_key) AS previous_n,
  CASE WHEN f.prev_run IS NOT NULL THEN (SELECT count(*) FROM
   (SELECT DISTINCT posting_id FROM v_snapshot_scope s WHERE s.run_key=f.run_key AND s.platform=f.platform AND s.query_key=f.query_key
    EXCEPT SELECT DISTINCT posting_id FROM v_snapshot_scope s WHERE s.run_key=f.prev_run AND s.platform=f.platform AND s.query_key=f.query_key) a) END AS added,
  CASE WHEN f.prev_run IS NOT NULL THEN (SELECT count(*) FROM
   (SELECT DISTINCT posting_id FROM v_snapshot_scope s WHERE s.run_key=f.prev_run AND s.platform=f.platform AND s.query_key=f.query_key
    EXCEPT SELECT DISTINCT posting_id FROM v_snapshot_scope s WHERE s.run_key=f.run_key AND s.platform=f.platform AND s.query_key=f.query_key) a) END AS removed
 FROM fulls f
) SELECT *,sum(added) OVER(PARTITION BY platform,query_key ORDER BY slot_ts) AS cum_added,
 sum(removed) OVER(PARTITION BY platform,query_key ORDER BY slot_ts) AS cum_removed FROM counts ORDER BY platform,query_key,slot_ts;
