"""네트워크를 사용하지 않는 실패·동시성·정규화 회귀 시험과 실행 증거."""
import json, time, unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import requests
from jobtrend.http import HostGate, fetch_with_backoff
from jobtrend import normalize, skills
from jobtrend.collect import Task, run_task
from jobtrend.sources.base import Query, QuickParse

EVIDENCE = {}
class FakeSession:
    def __init__(self, statuses, delay=0):
        self.statuses, self.starts, self.delay = iter(statuses), [], delay
    def request(self, method, url, **kwargs):
        self.starts.append(time.monotonic()); time.sleep(self.delay)
        item = next(self.statuses); status, headers = item if isinstance(item, tuple) else (item,{})
        r = requests.Response(); r.status_code=status; r.headers=headers; r._content=b'fixture';r.url=url
        return r

class PipelineTests(unittest.TestCase):
    def test_retry(self):
        s=FakeSession([503,503,(429,{'Retry-After':'1'}),200]); g=HostGate(0)
        fr=fetch_with_backoff(s,{'url':'https://fixture.test/'},g,base=.2)
        self.assertEqual((fr.outcome,fr.attempts),('ok',4))
        gaps=[b-a for a,b in zip(s.starts,s.starts[1:])]
        waits=[x['wait_s'] for x in fr.trace]
        self.assertTrue(all(abs(a-b)<.1 for a,b in zip(gaps,waits)))
        self.assertTrue(all(a<=b for a,b in zip(waits,waits[1:])))
        EVIDENCE['retry']=[{'status':t['status'],'planned_s':t['wait_s'],'actual_s':round(a,3)} for t,a in zip(fr.trace,gaps)]
        for statuses,outcome,n in [([410],'http_error',1),([503]*4,'http_error',4)]:
            f=fetch_with_backoff(FakeSession(statuses),{'url':'https://fixture.test/'},HostGate(0),base=.01)
            self.assertEqual((f.outcome,f.attempts),(outcome,n))
    def test_circuit_race(self):
        s=FakeSession([403]*5,delay=.03);g=HostGate(.02)
        with ThreadPoolExecutor(5) as e:
            out=list(e.map(lambda _:fetch_with_backoff(s,{'url':'https://fixture.test/'},g),range(5)))
        self.assertEqual(len(s.starts),1)
        self.assertEqual(sum(x.outcome=='circuit_open' for x in out),4)
        EVIDENCE['circuit']={'requests':len(s.starts),'outcomes':[x.outcome for x in out]}
    def test_deadline(self):
        s=FakeSession([200]); f=fetch_with_backoff(s,{'url':'https://fixture.test/'},HostGate(0),deadline=time.monotonic()-1)
        self.assertEqual(f.attempts,0); self.assertEqual(s.starts,[])
    def test_cached_last_page(self):
        class Adapter:
            platform='fixture'; page_size=2
            def request(self,*args): raise AssertionError('완료 페이지 뒤의 요청')
        t=Task(Adapter(),Query('fixture','BE','BE'),'full','baseline')
        r=run_task(t,None,HostGate(0),time.monotonic()+1,lambda *a:None,
                   {1:QuickParse(['a','b'],2,False,'')})
        self.assertEqual(r['items'],2)
    def test_domain(self):
        from jobtrend.dedup import title_conflict
        from datetime import datetime
        from jobtrend.config import KST
        now=datetime(2026,10,2,13,tzinfo=KST)
        self.assertEqual(normalize.company_key('주식회사 ABC Co., Ltd.'),'abc')
        self.assertEqual(normalize.parse_deadline('2070-01-01',now),(None,'always'))
        self.assertEqual(normalize.parse_deadline('오늘마감',now)[0].day,2)
        self.assertEqual(normalize.parse_deadline('내일마감',now)[0].day,3)
        self.assertEqual(normalize.parse_career('경력 3~5년'),('exp',3,5))
        self.assertNotIn('DA',normalize.title_job_groups('mobile developer'))
        self.assertIn('DA',normalize.title_job_groups('bi analyst'))
        self.assertNotIn('Java',skills.extract_skills('JavaScript'))
        self.assertIn('Python',skills.extract_skills('파이썬, python'))
        self.assertTrue(title_conflict('백엔드 개발자','프론트엔드 개발자'))
        self.assertTrue(title_conflict('Development Leader','Development Engineer'))
        self.assertNotEqual(normalize.title_key('[캐시워크] 백엔드'),normalize.title_key('[킬로] 백엔드'))
    def test_concurrency(self):
        def bench(workers):
            gates={h:HostGate(.12) for h in ['A','B']};start=time.monotonic()
            def call(unit):
                h,i=unit;fr=fetch_with_backoff(FakeSession([200],delay=.04),{'url':'https://'+h},gates[h])
                return h,i,fr.status
            with ThreadPoolExecutor(workers) as e: result=set(e.map(call,[(h,i) for h in gates for i in range(3)]))
            elapsed=time.monotonic()-start
            violations=sum(b[0]-a[0]<.119 for g in gates.values() for a,b in zip(g.request_log,g.request_log[1:]))
            return elapsed,result,violations,{h:[(a-start,b-start) for a,b in g.request_log] for h,g in gates.items()}
        sequential,a,v1,l1=bench(1); concurrent,b,v2,l2=bench(6)
        self.assertEqual(a,b);self.assertEqual(v1+v2,0);self.assertLess(concurrent,sequential)
        EVIDENCE['concurrency']={'sequential_s':sequential,'concurrent_s':concurrent,'equal_results':a==b,
          'interval_violations':v1+v2,'min_interval_s':.12,'transport':'고정 응답 가짜 전송; 운영은 2초',
          'sequential_log':l1,'concurrent_log':l2}

    def test_structured_detail(self):
        from jobtrend.sources.linkareer import Linkareer
        graph={'Activity:1':{'createdAt':1790823026000,'detailText':{'__ref':'ActivityText:1'},
               'recruitType':'ASAP','managerEmail':'private@example.test'},
               'ActivityText:1':{'text':'<p>Python 협업</p>'}}
        state={'props':{'pageProps':{'__APOLLO_STATE__':graph}}}
        ld={'@type':'JobPosting','educationRequirements':{'credentialCategory':'학사'}}
        text='<script id="__NEXT_DATA__" type="application/json">'+json.dumps(state)+'</script>'
        text+='<script type="application/ld+json">'+json.dumps(ld)+'</script>'
        detail=Linkareer().parse_detail(text)
        self.assertEqual(detail['education'],'학사')
        self.assertIn('Python',detail['skills_text'])
        self.assertNotIn('managerEmail',detail)

def run_suite():
    r=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(PipelineTests))
    EVIDENCE['tests']={'run':r.testsRun,'failures':len(r.failures),'errors':len(r.errors)}
    Path('reports').mkdir(exist_ok=True);Path('reports/test_evidence.json').write_text(json.dumps(EVIDENCE,ensure_ascii=False,indent=2))
    assert r.wasSuccessful()
    return EVIDENCE
if __name__=='__main__':run_suite()
