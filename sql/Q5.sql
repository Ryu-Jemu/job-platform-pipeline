WITH rep AS (
 SELECT DISTINCT ON(x.canonical_id) x.canonical_id,p.* FROM v_posting_scope p
 JOIN posting_canonical x USING(platform,posting_id)
 ORDER BY x.canonical_id,array_position(ARRAY['second','minute','hour','day','none'],p.posted_precision),p.first_seen_at,p.platform,p.posting_id
), features AS (
 SELECT r.*,j.n_sources,j.sources,j.job_groups,
  (SELECT count(DISTINCT s.skill) FROM posting_skill s JOIN posting_canonical x USING(platform,posting_id)
    WHERE x.canonical_id=r.canonical_id AND s.skill_group='technology') AS skill_count,
  (SELECT count(*) FROM job_canonical c JOIN v_job_scope jj USING(canonical_id) WHERE c.company_key=r.company_key) AS company_postings,
  extract(epoch FROM r.deadline_at-(SELECT max(slot_ts) FROM v_run_scope))/86400 AS days_to_deadline,
  extract(epoch FROM (SELECT max(slot_ts) FROM v_run_scope)-r.posted_at)/86400 AS posted_age_days,
  length(r.title) AS title_length,(r.deadline_kind='always')::int AS always_open
 FROM rep r JOIN v_job_scope j USING(canonical_id)
)

, counts AS (SELECT (posted_at AT TIME ZONE 'Asia/Seoul')::date posted_day,count(*) n
 FROM features WHERE posted_at IS NOT NULL GROUP BY 1),calendar AS (
 SELECT d::date posted_day,coalesce(n,0) n FROM generate_series(
  (SELECT min(posted_day) FROM counts),(SELECT max(posted_day) FROM counts),interval '1 day') d LEFT JOIN counts ON counts.posted_day=d::date)
SELECT *,avg(n) OVER(ORDER BY posted_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) ma7 FROM calendar ORDER BY posted_day;
