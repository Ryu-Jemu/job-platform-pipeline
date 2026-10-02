"""로컬 단발 운영: 매시 점검 → 20:00 run 종료 → pause → replay → 새 커널 보고서 검증.

데이터를 추가로 수집하거나 빠진 과거 슬롯을 생성하지 않는다. db-pg 재시작은 하지 않는다.
"""
from pathlib import Path
from datetime import datetime,timedelta
import argparse,json,os,sys,time,traceback
import requests
from dotenv import load_dotenv
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'));os.chdir(ROOT);load_dotenv(ROOT/'.env')
from jobtrend import config,store,transform,dedup,quality,runlog
from operations import state
from run_notebook import run
API='http://localhost:8080/api/v2/dags/jobtrend_snapshot_10min'

def save(payload):
 payload={'updated_at':datetime.now(config.KST).isoformat(),'pid':os.getpid(),**payload}
 tmp=ROOT/'reports/finalization.tmp';tmp.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str));os.replace(tmp,ROOT/'reports/finalization.json')
 return payload

def ready():
 r=requests.get(API+'/dagRuns',params={'limit':100,'order_by':'-run_after'},timeout=15);r.raise_for_status()
 final=[d for d in r.json()['dag_runs'] if datetime.fromisoformat(d['run_after'].replace('Z','+00:00'))==config.WINDOW_END]
 if not final:return False
 return final[0]['state'] in ('success','failed')

def assert_final_coverage(ops):
 assert ops['dag_paused'] and ops['import_errors']==0
 assert len(ops['slots'])==67
 unfinished=[s for s in ops['slots'] if s['state'] not in ('SUCCESS','PARTIAL','LEFT_TRUNCATED')]
 assert not unfinished,{'unfinished_slots':unfinished,'counts':ops['counts']}

def rebuild(final=False):
 if final:
  assert datetime.now(config.KST)>=config.WINDOW_END
  deadline=config.WINDOW_END+timedelta(minutes=10)
  while not ready():
   if datetime.now(config.KST)>deadline:raise RuntimeError('20:00 Airflow run 종료 확인 실패')
   time.sleep(10)
  r=requests.patch(API,json={'is_paused':True},timeout=15);r.raise_for_status()
  assert requests.get(API,timeout=15).json()['is_paused']
 # 운영에서 이미 완료된 원본만 재생한다. running 실행은 변경하지 않는다.
 with store.connect() as c:keys=[r[0] for r in c.execute("SELECT run_key FROM crawl_run WHERE run_kind='scheduled' AND status<>'running' ORDER BY slot_ts")]
 for key in keys:transform.transform_load(config.dsn(),key)
 dedup.resolve_duplicates()
 for key in keys:
  quality.check_run(config.dsn(),key);runlog.finalize_run(config.dsn(),key)
 nb=run();ops=state()
 from build_web import build_web
 web=build_web(root=ROOT)
 validation=json.loads((ROOT/'reports/analysis_validation.json').read_text())
 result={'status':'complete' if final else 'preview_verified','notebook':nb,'analysis':validation,'web':web,
         'operations':{'counts':ops['counts'],'dag_paused':ops['dag_paused'],'import_errors':ops['import_errors']}}
 if final:
  assert validation['final_slot'],'최종 슬롯 success/partial 확인 실패'
  assert_final_coverage(ops)
 save(result);return result

def monitor():
 save({'status':'waiting_for_20_00','final_slot':config.WINDOW_END.isoformat()})
 last_hour=None
 while datetime.now(config.KST)<config.WINDOW_END:
  now=datetime.now(config.KST)
  if now.hour!=last_hour:
   try:
    ops=state();print('operating',now.isoformat(),ops['counts'],flush=True)
    save({'status':'waiting_for_20_00','final_slot':config.WINDOW_END.isoformat(),
          'last_hourly_check':ops['checked_at'],'coverage':ops['counts']})
   except Exception as exc:
    save({'status':'waiting_with_operating_error','error':str(exc),'final_slot':config.WINDOW_END.isoformat()})
    print('operating error',type(exc).__name__,str(exc),flush=True)
   last_hour=now.hour
  time.sleep(min(30,max(1,(config.WINDOW_END-datetime.now(config.KST)).total_seconds())))
 return rebuild(final=True)

if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--preview',action='store_true');parser.add_argument('--final-now',action='store_true');args=parser.parse_args()
 try:print(json.dumps(rebuild(False) if args.preview else rebuild(True) if args.final_now else monitor(),ensure_ascii=False,default=str),flush=True)
 except Exception as exc:
  save({'status':'failed','error':str(exc),'exception':type(exc).__name__});traceback.print_exc();sys.exit(1)
