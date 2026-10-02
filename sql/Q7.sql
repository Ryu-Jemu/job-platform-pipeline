WITH r AS (SELECT DISTINCT x.canonical_id,r.job_group FROM posting_role r JOIN posting_canonical x USING(platform,posting_id) JOIN v_job_scope j USING(canonical_id)),
 s AS (SELECT DISTINCT x.canonical_id,sk.skill,sk.skill_group FROM posting_skill sk JOIN posting_canonical x USING(platform,posting_id) JOIN v_job_scope j USING(canonical_id)),
 freq AS (SELECT r.job_group,s.skill,s.skill_group,count(*) n FROM r JOIN s USING(canonical_id) GROUP BY 1,2,3),
 denom AS (SELECT job_group,count(*) n FROM r GROUP BY 1),overall AS (SELECT skill,count(*) n FROM s GROUP BY 1)
SELECT f.*,f.n::numeric/d.n AS share,(f.n::numeric/d.n)/(o.n::numeric/(SELECT count(*) FROM v_job_scope)) AS lift
FROM freq f JOIN denom d USING(job_group) JOIN overall o USING(skill) ORDER BY job_group,n DESC,skill;
