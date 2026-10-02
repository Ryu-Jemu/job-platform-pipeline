SELECT q.* FROM quality_check q JOIN v_run_scope r USING(run_key) ORDER BY r.slot_ts,q.check_name,q.platform;
