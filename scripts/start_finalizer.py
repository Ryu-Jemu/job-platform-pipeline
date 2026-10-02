"""부모 앱과 독립된 단발 마무리 프로세스를 시작하고 중복 실행을 막는다."""
from pathlib import Path
import json,os,subprocess,sys,time
ROOT=Path(__file__).resolve().parents[1]
pidfile=ROOT/'reports/finalizer.pid'
if pidfile.exists():
 try:
  pid=int(pidfile.read_text());os.kill(pid,0)
  print('existing finalizer',pid);sys.exit(0)
 except (ProcessLookupError,ValueError):pass
log=open(ROOT/'reports/finalizer.log','a')
p=subprocess.Popen([sys.executable,str(ROOT/'scripts/finalize.py')],cwd=ROOT,stdout=log,stderr=log,start_new_session=True,env={**os.environ,'PYTHONPATH':str(ROOT/'src'),'PYTHONUNBUFFERED':'1'})
pidfile.write_text(str(p.pid));time.sleep(1)
assert p.poll() is None,'finalizer failed to start'
print('finalizer pid',p.pid,'state',json.loads((ROOT/'reports/finalization.json').read_text())['status'])
