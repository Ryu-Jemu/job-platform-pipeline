"""ETL → EDA를 각각 새 커널에서 실행하고 두 결과를 검증 후 저장한다."""
from pathlib import Path
import os,json,time
import nbformat
from nbclient import NotebookClient
ROOT=Path(__file__).resolve().parents[1]
NAMES=('job_platform_etl.ipynb','job_platform_eda.ipynb')
def run():
    os.chdir(ROOT)
    from build_etl_notebook import build_etl
    from build_eda_notebook import build_eda
    originals={name:(ROOT/name).read_bytes() if (ROOT/name).exists() else None for name in NAMES}
    started=time.monotonic();executed={};results={}
    from dotenv import dotenv_values
    secrets=[(k,v) for p in (ROOT/'.env',ROOT/'airflow/.env') if p.exists()
             for k,v in dotenv_values(p).items() if v and len(v)>12]
    try:
        sources={name:ROOT/'reports'/name.replace('.ipynb','.generated.tmp') for name in NAMES}
        build_etl(output_path=sources[NAMES[0]]);build_eda(output_path=sources[NAMES[1]])
        for name in NAMES:
            nb=nbformat.read(sources[name],as_version=4);nbformat.validate(nb)
            for cell in nb.cells:
                if cell.cell_type=='code':cell.execution_count=None;cell.outputs=[]
            cell_started=time.monotonic()
            try:
                NotebookClient(nb,timeout=600,kernel_name='python3',resources={'metadata':{'path':str(ROOT)}}).execute()
            except Exception:
                nbformat.write(nb,ROOT/'reports'/name.replace('.ipynb','_failed.ipynb'));raise
            code=[c for c in nb.cells if c.cell_type=='code']
            errors=[o for c in code for o in c.outputs if o.output_type=='error']
            counts=[c.execution_count for c in code]
            images=sum('image/png' in o.get('data',{}) for c in code for o in c.outputs)
            notes=sum(any(s in str(o.get('data',{}).get('text/markdown','')) for s in ('✍️','해석:')) for c in code for o in c.outputs)
            assert not errors
            assert counts==list(range(1,len(counts)+1)),(name,counts)
            chart_cells=[c for c in code if c.metadata.get('analysis_chart')]
            assert chart_cells and all(any('image/png' in o.get('data',{}) for o in c.outputs) for c in chart_cells),name
            assert notes>=nb.metadata['jobtrend'].get('minimum_interpretations',4),(name,notes)
            serialized=nbformat.writes(nb)
            for k,v in secrets:assert v not in serialized,f'secret leakage detected: {k}'
            nb.metadata['jobtrend']['execution_verified']=True
            executed[name]=nb
            results[name]={'execution_verified':True,'code_cells':len(code),'execution_count_contiguous':True,
                           'errors':0,'png_figures':images,'interpretations':notes,'elapsed_s':round(time.monotonic()-cell_started,2)}
        for name,nb in executed.items():
            tmp=ROOT/'reports'/name.replace('.ipynb','.verified.tmp')
            nbformat.write(nb,tmp);os.replace(tmp,ROOT/name)
    except Exception:
        for name,content in originals.items():
            if content is not None:(ROOT/name).write_bytes(content)
            else:(ROOT/name).unlink(missing_ok=True)
        raise
    for p in sources.values():p.unlink(missing_ok=True)
    result={'execution_verified':True,'notebooks':results,'elapsed_s':round(time.monotonic()-started,2)}
    (ROOT/'reports/notebook_validation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps(result,ensure_ascii=False))
    return result
if __name__=='__main__':run()
