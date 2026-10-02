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
SELECT canonical_id,career_min_yr,career_max_yr,skill_count,days_to_deadline,posted_age_days,n_sources,title_length,company_postings,always_open FROM features ORDER BY 1;
