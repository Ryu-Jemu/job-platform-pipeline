SELECT tag,count(*) n FROM v_posting_scope CROSS JOIN LATERAL unnest(tags_raw) tag GROUP BY tag ORDER BY n DESC LIMIT 80;
