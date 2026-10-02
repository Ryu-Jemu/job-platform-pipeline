SELECT platform,count(*) n,
 avg((posted_at IS NULL)::int)::float posted_at,avg((sido IS NULL)::int)::float sido,
 avg((career_min_yr IS NULL)::int)::float career_min_yr,avg((education IS NULL)::int)::float education,
 avg((deadline_kind='unknown')::int)::float deadline_unknown,
 avg((deadline_at IS NULL AND deadline_kind IN('always','until_filled'))::int)::float meaningful_deadline_null,
 avg((employment_type IS NULL)::int)::float employment_type
FROM v_posting_scope GROUP BY platform ORDER BY platform;
