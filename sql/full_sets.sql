SELECT DISTINCT m.run_key,m.platform,m.query_key,m.slot_ts,s.posting_id
 FROM v_listing_scope m JOIN v_snapshot_scope s USING(run_key,platform,query_key) WHERE m.is_complete ORDER BY 2,3,4,5;
