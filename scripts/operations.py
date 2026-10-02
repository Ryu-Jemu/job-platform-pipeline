"""가동 현황. 놓친 오전·미래·운영 중 결측을 구분하고 DB 재시작 정책을 바꾸지 않는다."""
from pathlib import Path
from datetime import datetime,timedelta
import json,subprocess
from dotenv import load_dotenv
from jobtrend import config,store

def command(args):
 return subprocess.run(args,capture_output=True,text=True,check=True).stdout.strip()
def meta(sql):
 return command(['docker','exec','job-platform-pipeline-postgres-1','psql','-U','airflow','-d','airflow','-At','-c',sql])
def state():
 inspected=json.loads(command(['docker','inspect','db-pg']))[0]
 db={'running':inspected['State']['Running'],'restart_policy':inspected['HostConfig']['RestartPolicy']['Name']}
 if not db['running']:raise RuntimeError('db-pg 중지: 사용자 확인 필요. 자동 재시작하지 않음')
 now=datetime.now(config.KST)
 with store.connect() as c:
  runs=c.execute("SELECT slot_ts,status FROM crawl_run WHERE run_kind='scheduled' ORDER BY slot_ts").fetchall()
  sources=c.execute('SELECT platform,reason,robots_status,robots_allowed FROM source_state ORDER BY platform').fetchall()
  volume=c.execute('SELECT pg_database_size(current_database()),sum(pg_total_relation_size(oid)) FROM pg_class WHERE relkind=\'r\' AND relnamespace=\'public\'::regnamespace').fetchone()
  requests=c.execute("SELECT platform,sum(attempts),count(*)filter(where attempts>1) FROM raw_listing_page GROUP BY 1 ORDER BY 1").fetchall()
  full_scans=c.execute("SELECT platform,query_key,max(slot_ts) FROM v_listing_scope WHERE is_complete AND status='ok' GROUP BY platform,query_key ORDER BY 1,2").fetchall()
 mapping=dict(runs);first=min(mapping) if mapping else None;slots=[]
 tick=config.WINDOW_START
 while tick<=config.WINDOW_END:
  status=mapping.get(tick)
  label=status.upper() if status else ('FUTURE' if tick>now else 'LEFT_TRUNCATED' if first and tick<first else 'GRACE' if tick>now-timedelta(minutes=5) else 'MISSING')
  slots.append({'slot_kst':tick.isoformat(),'state':label});tick+=timedelta(minutes=10)
 result={'checked_at':now.isoformat(),'db':db,'dag_paused':meta("select is_paused from dag where dag_id='jobtrend_snapshot_10min'")=='t',
   'import_errors':int(meta('select count(*) from import_error')),'slots':slots,
   'counts':{k:sum(s['state']==k for s in slots) for k in {s['state'] for s in slots}},
   'sources':[{'platform':p,'reason':r,'robots_status':s,'allowed':a} for p,r,s,a in sources],
   'database_bytes':int(volume[0]),'relation_bytes':int(volume[1] or 0),
   'full_scans':[{'platform':p,'query_key':q,'slot_kst':t.isoformat()} for p,q,t in full_scans],
   'requests':[{'platform':p,'attempts':n,'retried_pages':r} for p,n,r in requests]}
 Path('reports').mkdir(exist_ok=True)
 Path('reports/operations.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str))
 return result
if __name__=='__main__':
 load_dotenv(Path(__file__).resolve().parents[1]/'.env');print(json.dumps(state(),ensure_ascii=False,indent=2))
