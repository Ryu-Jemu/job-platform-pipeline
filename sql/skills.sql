SELECT DISTINCT x.canonical_id,r.job_group,s.skill,s.skill_group,s.source_field
 FROM posting_skill s JOIN posting_canonical x USING(platform,posting_id)
 JOIN posting_role r USING(platform,posting_id) JOIN v_job_scope j USING(canonical_id) ORDER BY 1,2,3;
