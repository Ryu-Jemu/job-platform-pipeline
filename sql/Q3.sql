WITH company AS (SELECT c.company_key,min(j.company_name) company_name,count(*) n
 FROM v_job_scope j JOIN job_canonical c USING(canonical_id) GROUP BY c.company_key)
SELECT *,dense_rank() OVER(ORDER BY n DESC) ranking, ntile(20) OVER(ORDER BY n DESC,company_key) bucket20,
 n::numeric/sum(n) OVER() AS share, sum(n) OVER(ORDER BY n DESC,company_key ROWS UNBOUNDED PRECEDING)::numeric/sum(n) OVER() AS cum_share
FROM company ORDER BY n DESC,company_key;
