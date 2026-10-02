"""컴파일·새 프로세스 import·DAG 구조 시험 후, 실행이 없을 때 파일을 원자 교체한다."""
from pathlib import Path
import json,os,py_compile,subprocess,sys,tempfile
ROOT=Path(__file__).resolve().parents[1]
DAG='jobtrend_snapshot_10min'
def cmd(args):
    return subprocess.run(args,cwd=ROOT,check=True,capture_output=True,text=True).stdout.strip()
def meta(sql):
    return cmd(['docker','exec','job-platform-pipeline-postgres-1','psql','-U','airflow','-d','airflow','-At','-c',sql])
def deploy():
    for p in (ROOT/'src').rglob('*.py'):py_compile.compile(str(p),doraise=True)
    env={**os.environ,'PYTHONPATH':str(ROOT/'src')}
    subprocess.run([sys.executable,'-c','from jobtrend import collect,dedup,quality,transform,skills; import jobtrend_dag'],env=env,check=True,capture_output=True)
    if int(meta(f"select count(*) from dag_run where dag_id='{DAG}' and state in ('running','queued')")):
        raise RuntimeError('DAG 실행 중: 다음 유휴창에서 재시도')
    # package 파일부터 교체하고 DAG를 마지막에 공개한다. 유휴 상태를 두 번째로 확인한다.
    files=list((ROOT/'src/jobtrend').rglob('*.py'))+list((ROOT/'src/jobtrend').rglob('*.sql'))+[ROOT/'src/jobtrend_dag.py']
    for p in files:
        if '__pycache__' in p.parts:continue
        relative=p.relative_to(ROOT/'src');target=ROOT/'airflow/dags'/relative
        target.parent.mkdir(parents=True,exist_ok=True)
        fd,tmp=tempfile.mkstemp(dir=target.parent,prefix='.deploy-',suffix='.tmp')
        with os.fdopen(fd,'wb') as f:f.write(p.read_bytes())
        os.chmod(tmp,0o644);os.replace(tmp,target)
    (ROOT/'airflow/dags/.airflowignore').write_text('jobtrend/\n')
    from jobtrend import CODE_VERSION
    result={'files':len(files),'code_version':CODE_VERSION,'running_after':int(meta(f"select count(*) from dag_run where dag_id='{DAG}' and state in ('running','queued')"))}
    (ROOT/'reports/deployment.json').write_text(json.dumps(result,indent=2))
    print(result)
    return result
if __name__=='__main__':deploy()
