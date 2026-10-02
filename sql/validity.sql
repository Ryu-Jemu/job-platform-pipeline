SELECT platform,count(*) n,count(*) FILTER(WHERE deadline_at < posted_at) deadline_before_posted,
 count(*) FILTER(WHERE career_min_yr > career_max_yr) invalid_career,
 count(*) FILTER(WHERE extract(year FROM deadline_at)>=2070) sentinel_leaks,
 count(*) FILTER(WHERE company_name IN('','(미상)') OR title='' OR url='') required_missing
 FROM v_posting_scope GROUP BY 1 ORDER BY 1;
