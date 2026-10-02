"""DB·스키마·robots 근거 초기화. 비밀번호/본문 원문은 출력하지 않는다."""
from pathlib import Path
from datetime import datetime
from urllib.parse import urlsplit,urlunsplit
from urllib.robotparser import RobotFileParser
import json,os,subprocess,time
import psycopg,requests
from jobtrend import config,store
from jobtrend.sources import REGISTRY
ROOT=Path(__file__).resolve().parents[1]

def bootstrap():
 inspected=json.loads(subprocess.run(['docker','inspect','db-pg'],capture_output=True,text=True,check=True).stdout)[0]
 if not inspected['State']['Running']:raise RuntimeError('db-pg가 중지됨: 사용자 확인 후 기동 필요')
 parts=urlsplit(config.dsn());admin=urlunsplit(parts._replace(path='/postgres'))
 with psycopg.connect(admin,autocommit=True) as c:
  exists=c.execute("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname='job_platform_db')").fetchone()[0]
  if not exists:c.execute('CREATE DATABASE job_platform_db')
  c.execute("ALTER DATABASE job_platform_db SET timezone TO 'Asia/Seoul'")
 store.apply_schema()
 cached=ROOT/'reports/robots_checks.json'
 rows=json.loads(cached.read_text()) if cached.exists() else []
 done={r['platform'] for r in rows}
 for ad in REGISTRY.values():
  if ad.platform in done:continue
  time.sleep(ad.min_interval_s)
  r=requests.get('https://'+ad.host+'/robots.txt',headers={'User-Agent':config.USER_AGENT},timeout=20)
  rp=RobotFileParser();rp.parse(r.text.splitlines())
  req=ad.request(ad.queries()[0],1)
  allowed=r.status_code==200 and rp.can_fetch(config.USER_AGENT,req['url'])
  if hasattr(ad,'detail_request'):allowed=allowed and rp.can_fetch(config.USER_AGENT,ad.detail_request('1')['url'])
  rows.append({'platform':ad.platform,'url':r.url,'status':r.status_code,'can_fetch':allowed,'text':r.text[:5000],'checked_at':datetime.now(config.KST).isoformat()})
 cached.write_text(json.dumps(rows,ensure_ascii=False,indent=2))
 with store.connect() as c:
  for r in rows:
   c.execute('INSERT INTO source_state(platform,robots_status,robots_allowed,checked_at,reason) VALUES(%s,%s,%s,now(),%s) ON CONFLICT(platform) DO UPDATE SET robots_status=excluded.robots_status,robots_allowed=excluded.robots_allowed,reason=excluded.reason',
             (r['platform'],r.get('status'),r.get('can_fetch',False),'robots 직접 확인 (reports/robots_checks.json)'))
  for p in ('rocketpunch','alio'):
   c.execute("INSERT INTO source_state(platform,reason) VALUES(%s,%s) ON CONFLICT(platform) DO UPDATE SET reason=excluded.reason",(p,'웹 원천만 포함: 사용자 선택, API 키 미등록'))
 # root .env와 Docker용 .env는 독립. 로컬 DSN hostname만 Docker 호스트 주소로 바꾼다.
 if parts.hostname in ('localhost','127.0.0.1'):
  userinfo=parts.netloc.rpartition('@')[0]
  netloc=(userinfo+'@' if userinfo else '')+'host.docker.internal'+(':'+str(parts.port) if parts.port else '')
  docker_dsn=urlunsplit(parts._replace(netloc=netloc))
 else:docker_dsn=config.dsn()
 # 이미 실행 중인 스택의 env는 덮어쓰지 않는다. 초기 설치에서만 작성.
 env=ROOT/'airflow/.env'
 if not env.exists():
  env.write_text('JOBTREND_DSN='+docker_dsn+'\nROCKETPUNCH_API_KEY=\nALIO_SERVICE_KEY=\n');env.chmod(0o600)
 return {'database':'job_platform_db','timezone':'Asia/Seoul','created':not exists,'web_sources':list(REGISTRY)}
if __name__=='__main__':
 from dotenv import load_dotenv
 load_dotenv(ROOT/'.env');print(bootstrap())
