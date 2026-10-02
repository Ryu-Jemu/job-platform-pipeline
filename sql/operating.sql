SELECT platform,outcome,count(*) pages,sum(attempts) attempts,sum(octet_length(body)) bytes,
 min(fetched_at) first_fetch,max(fetched_at) last_fetch FROM raw_listing_page r JOIN v_run_scope s USING(run_key) GROUP BY 1,2 ORDER BY 1,2;
