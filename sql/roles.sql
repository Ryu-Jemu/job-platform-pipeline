SELECT DISTINCT x.canonical_id,r.job_group FROM posting_role r
 JOIN posting_canonical x USING(platform,posting_id) JOIN v_job_scope j USING(canonical_id) ORDER BY 1,2;
