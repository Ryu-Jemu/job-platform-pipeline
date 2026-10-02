SELECT platform,query_key,job_group,slot_ts,
 CASE WHEN row_number() OVER(PARTITION BY platform,query_key ORDER BY slot_ts)>1 THEN new_on_page1 END AS new_n
 FROM v_listing_scope ORDER BY platform,query_key,slot_ts;
