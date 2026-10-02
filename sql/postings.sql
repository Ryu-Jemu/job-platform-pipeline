SELECT a.* FROM v_posting_all a JOIN v_posting_scope p USING(platform,posting_id) ORDER BY platform,posting_id;
