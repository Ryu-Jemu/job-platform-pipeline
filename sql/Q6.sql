SELECT sources,n_sources,count(*) n,count(*)::numeric/sum(count(*)) OVER() share
 FROM v_job_scope GROUP BY sources,n_sources ORDER BY n DESC;
