"""운영 원본의 replay·멱등·계보·중복 통합 증거. 전송 요청 없음."""
from pathlib import Path
import hashlib,json
from jobtrend import store,transform,dedup,quality

def fingerprint(conn):
 rows=conn.execute('SELECT platform,posting_id,canonical_id,match_method,similarity FROM posting_canonical ORDER BY platform,posting_id').fetchall()
 return hashlib.sha256(json.dumps(rows,default=str).encode()).hexdigest()

def verify_idempotency():
 with store.connect() as c:
  c.execute("SELECT pg_advisory_xact_lock(hashtext('jobtrend:transform'))")
  c.execute("SELECT pg_advisory_xact_lock(hashtext('jobtrend:dedup'))")
  # 외부 운영 트랜잭션과 교차하지 않도록 같은 연결의 트랜잭션에서 실제 정제 2회 시험
  k=c.execute("SELECT run_key FROM crawl_run WHERE run_kind='scheduled' AND status IN ('success','partial') ORDER BY slot_ts DESC LIMIT 1").fetchone()[0]
  names=['raw_listing_page','raw_posting_detail','job_posting','posting_role','posting_snapshot','listing_metric','posting_skill']
  # 진행 중인 다른 run의 수집을 멱등성 실패로 오인하지 않도록 원본은 시험 run만 센다.
  counts=lambda:{n:(c.execute(f'SELECT count(*) FROM {n} WHERE run_key=%s',(k,)).fetchone()[0]
                    if n.startswith('raw_') else c.execute(f'SELECT count(*) FROM {n}').fetchone()[0]) for n in names}
  transform.transform_run(c,k);before=counts();first=transform.transform_run(c,k);after1=counts();second=transform.transform_run(c,k);after2=counts()
  assert before==after1==after2,(before,after1,after2)
  # 別 run_key: 같은 원본을 새 실행에 붙이면 마스터는 증가하지 않고 스냅샷만 별 관측으로 저장됨. rollback으로 실험 데이터 제거.
  with c.transaction(force_rollback=True):
   new='nb__idempotency_probe'
   c.execute("INSERT INTO crawl_run(run_key,run_kind,slot_ts,code_version) SELECT %s,'notebook',slot_ts,code_version FROM crawl_run WHERE run_key=%s ON CONFLICT DO NOTHING",(new,k))
   c.execute("INSERT INTO raw_listing_page(run_key,platform,query_key,job_group,page_no,scan_kind,request_url,outcome,http_status,attempts,n_items,reported_total,has_next,item_ids,body_format,body,body_sha256,fetched_at) SELECT %s,platform,query_key,job_group,page_no,scan_kind,request_url,outcome,http_status,0,n_items,reported_total,has_next,item_ids,body_format,body,body_sha256,fetched_at FROM raw_listing_page WHERE run_key=%s",(new,k))
   oldmaster=c.execute('select count(*) from job_posting').fetchone()[0]
   transform.transform_run(c,new)
   assert c.execute('select count(*) from job_posting').fetchone()[0]==oldmaster
  result={'run_key':k,'before':before,'after_first':after1,'after_second':after2,'increase_second':{n:after2[n]-after1[n] for n in names},'new_run_master_growth':0,'experiment_rolled_back':True}
  # 두 해상도와 지문 조회 사이에도 정제·통합 잠금을 유지한다.
  dedup.resolve_duplicates(connection=c)
  hash1=fingerprint(c);n1=c.execute('select count(*) from job_canonical').fetchone()[0]
  dedup.resolve_duplicates(connection=c)
  hash2=fingerprint(c);n2=c.execute('select count(*) from job_canonical').fetchone()[0]
  assert hash1==hash2 and n1==n2
 result['dedup']={'canonical_rows':[n1,n2],'mapping_fingerprint_equal':hash1==hash2}
 Path('reports/idempotency.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));return result

def threshold_evidence():
 with store.connect() as c:
  summary=[]
  keys=[(r[0],r[1]) for r in c.execute('SELECT platform,posting_id FROM v_posting_scope')]
  for tau in [.4,.5,.55,.6,.7]:
   pairs=dedup.candidate_pairs(c,tau);uf=dedup._UnionFind()
   for key in keys:uf.find(key)
   for a,ia,b,ib,sim in pairs:uf.union((a,ia),(b,ib))
   groups={}
   for key in keys:groups.setdefault(uf.find(key),set()).add(key[0])
   multi=sum(len(s)>1 for s in groups.values())
   summary.append({'tau':tau,'candidate_pairs':len(pairs),'canonical_groups':len(groups),'cross_posted':multi,'cross_posted_ratio':multi/len(groups) if groups else 0})
  pairs=dedup.candidate_pairs(c,.55)
  ordered=sorted(pairs,key=lambda r:(float(r[4]),r[0],r[1],r[2],r[3]))
  sample=[ordered[i] for i in sorted(set(__import__('numpy').linspace(0,max(0,len(ordered)-1),min(20,len(ordered)),dtype=int)))]
  inspected=[]
  for a,ia,b,ib,sim in sample:
   ra=c.execute('SELECT company_name,title,deadline_at,deadline_kind FROM job_posting WHERE platform=%s AND posting_id=%s',(a,ia)).fetchone()
   rb=c.execute('SELECT company_name,title,deadline_at,deadline_kind FROM job_posting WHERE platform=%s AND posting_id=%s',(b,ib)).fetchone()
   inspected.append({'a':a,'id_a':ia,'company_a':ra[0],'title_a':ra[1],'deadline_a':ra[2],'b':b,'id_b':ib,'company_b':rb[0],'title_b':rb[1],'deadline_b':rb[2],'similarity':float(sim),'review':'未검토','label':None})
 result={'thresholds':summary,'sample':inspected,'sample_method':'유사도 순서의 균등 20쌍; 정답 라벨 아님'}
 Path('reports/dedup_sensitivity.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str));return result
if __name__=='__main__':
 from dotenv import load_dotenv
 load_dotenv('.env');print(verify_idempotency());print(threshold_evidence()['sample'])
