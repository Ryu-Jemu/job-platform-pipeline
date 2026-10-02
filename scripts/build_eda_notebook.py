"""검증된 DB 관측값으로 질문·SQL·그림·해석만 담는 EDA 노트북."""
from pathlib import Path
import nbformat as nbf
ROOT=Path(__file__).resolve().parents[1]

def build_eda(output_path=None):
    cells=[]
    def md(s):cells.append(nbf.v4.new_markdown_cell(s.strip()))
    def code(s,chart=False):
        cell=nbf.v4.new_code_cell(s.strip())
        if chart:cell.metadata['analysis_chart']=True
        cells.append(cell)
    md('''# 직무별 채용 공고 EDA

**분석 질문:** BE·DA·DE의 관측 공고 구성, 기술 키워드와 원천 간 중복은 어떻게 다른가?

사람인·잡코리아·인크루트·링커리어의 실제 저장 자료를 분석한다. [ETL 노트북](job_platform_etl.ipynb)은 수집·재시도·원본 분리·멱등성을 검증한다. 이 파일은 공고 구성·변화·기술과 결측을 집계하고 실제 관측값을 시각화한다.

2026-10-02 09:00~20:00 KST가 계획 범위이며 실제 수집은 12:50부터 시작했다. 오전 23틱은 좌절단이다. 20:00 전 출력은 부분 관측이고 빠진 시간의 값을 채우지 않는다.

분석 단위는 **통합 공고**다. 대표 조건은 게시 시각 정밀도·첫 관측·원천 순으로 한 공고를 선택한다. 기술·역량은 출처 합집합, 직무별 분모는 해당 직무의 통합 공고 수다. 복수 직무를 포함하므로 직무 합계는 전체 합계와 다를 수 있다. 중복 통합은 유사도에 따른 추정이며 실제 고용 인원·전체 시장 규모를 뜻하지 않는다.''')
    code('''from pathlib import Path
import os,sys,json
ROOT=Path.cwd()
assert (ROOT/'src/jobtrend').is_dir(), '프로젝트 루트에서 실행하세요'
sys.path.insert(0,str(ROOT/'src'))
from dotenv import load_dotenv
load_dotenv(ROOT/'.env')
from IPython.display import display,Markdown
import pandas as pd
from jobtrend import analysis as eda,store
pd.set_option('display.max_colwidth',90)
eda.setup_plots()
snapshot=eda.load_snapshot() # 모든 쿼리가 동일한 READ ONLY REPEATABLE READ 스냅샷
validation=eda.validate(snapshot)
catalog_evidence=eda.export_catalog(snapshot)
print('분석 기준 슬롯:',snapshot['asof'])
display(pd.DataFrame([catalog_evidence]))
print('SQL/pandas 독립 검증:',validation)
display(snapshot['catalog'].head(5)[['company_name','title','job_groups','sources','is_open']])''')
    md('''## 1. 결측과 유효성부터 확인

**목적:** 원천이 제공하지 않는 값과 파싱 오류를 구분하고 비교 가능한 변수만 해석한다. 결측률은 플랫폼 공고 단위이며 통합 대표 조건의 결측률과 다르다. 상시·채용시 마감은 실제 날짜가 없으므로 의미 있는 NULL로 분리한다.''')
    code('''display(snapshot['missingness'].round(4))
display(snapshot['validity'])
assert snapshot['validity'].sentinel_leaks.sum()==0
assert snapshot['validity'].required_missing.sum()==0
assert not ((snapshot['quality'].check_name=='privacy_patterns') & (snapshot['quality'].metric.astype(float)>0)).any()
display(Markdown(eda.chart_quality(snapshot)))''',True)
    md('''## 2. Q1·Q2 관측 공고의 변화

**목적:** 표시 총건수의 증감과 실제 목록 ID의 등장·이탈을 구분한다. Q1은 `LAG`·3슬롯 `AVG OVER`로 변화와 30분 평균을 계산한다. 링커리어는 혼합 IT 전수 목록에서 분류된 수이며 다른 플랫폼의 표시 총건수와 정의가 다르다. Q2는 연속 완료 전수의 `EXCEPT`로 유입·이탈을 계산한다. 첫 관측은 기준선으로만 사용한다.

차트는 실제 관측 구간만 표시하며 원천별 최초 관측=100 지수로 변화 크기를 비교한다. 오전 공백과 이후 예정 시간은 집계 표의 결측으로 유지한다. 목록 ID의 이탈은 채용 성사를 뜻하지 않는다.''')
    code(r'''for name in ('Q1','Q2'):
    display(Markdown('**'+name+' 집계 SQL**\n```sql\n'+eda.QUERIES[name].strip()+'\n```'))
q1=snapshot['Q1'];observed=q1[q1.inventory.notna()]
display(observed.groupby(['platform','job_group']).tail(1)[['platform','job_group','slot_ts','inventory','delta_10min','ma30']])
display(snapshot['Q2'][snapshot['Q2'].prev_slot.notna()].tail(12))''')
    code('''display(Markdown(eda.chart_q1(snapshot)))
display(Markdown(eda.chart_q2(snapshot)))''',True)
    md('''## 3. Q3·Q4 기업 집중도와 지원 조건

**목적:** 소수 기업의 다수 공고가 전체 수요를 부풀리는지 살펴보고 직무별 경력 문턱·지역·마감 구성을 비교한다. 회사명 정규화 후 `DENSE_RANK`·`NTILE`·누적 점유율로 집중도를 집계한다. 경력의 `PERCENTILE_CONT`는 결측을 제외한다. 대표 조건이 서로 다른 플랫폼에서 선택될 수 있으므로 원천별 결측을 함께 읽는다.''')
    code(r'''display(Markdown('```sql\n'+eda.QUERIES['Q3'].strip()+'\n```'))
display(snapshot['Q3'].head(10)[['company_name','n','ranking','share','cum_share']])
display(snapshot['Q4'])
display(Markdown(eda.chart_q3(snapshot)))
display(Markdown(eda.chart_q4(snapshot)))''',True)
    md('''## 4. Q5 게시일 분포와 Q6 중복 게시

**목적:** 현재 목록에 남아 있는 오래된 공고의 영향을 확인하고 플랫폼 중복을 통합한 효과를 측정한다. 게시일 분포는 과거 채용 수요의 시계열이 아니다. 상대 시각은 최초 관측 기준으로 고정하고 수정 표기와 구분한다.

중복 기준은 회사·제목 유사도 0.55·호환 마감·지역과 직무 충돌 제외다. [ETL](job_platform_etl.ipynb)의 임계값 민감도와 표본 검토가 이 추정의 한계를 설명한다. 모든 원천 링크를 보존하여 직접 검토할 수 있다.''')
    code('''display(snapshot['features'].posted_precision.value_counts(dropna=False).rename('통합 공고 수').to_frame())
display(snapshot['Q6'])
display(Markdown(eda.chart_q5(snapshot)))
display(Markdown(eda.chart_q6(snapshot)))''',True)
    md('''## 5. Q7 기술·역량과 직무별 특이성

**목적:** 통합 공고별 키워드를 한 번만 세어 원천 중복으로 빈도가 부풀려지는 것을 막는다. 직무 점유율을 전체 점유율로 나눈 Lift는 해당 직무에서 상대적으로 자주 검출된 기술을 보여준다. 사전 별칭과 경계를 사용하며 JavaScript를 Java로 세지 않는 시험은 ETL에 있다.

목록 태그·제목·읽을 수 있는 상세 텍스트만 사용한다. 이미지 공고와 사전 밖 표현은 검출하지 못하므로 키워드 미검출을 요구 없음으로 해석하지 않는다. 의미 없는 명사 후보 나열과 자동 사전 확대는 제외했다.''')
    code(r'''display(Markdown('```sql\n'+eda.QUERIES['Q7'].strip()+'\n```'))
display(snapshot['Q7'][snapshot['Q7'].skill_group.eq('technology')].groupby('job_group').head(5)[['job_group','skill','n','share','lift']])
display(Markdown(eda.chart_q7(snapshot)))''',True)
    md('''## 6. Q8 상관과 공출현

**목적:** 경력·키워드 수·게시 경과일 등의 동반 변화를 탐색한다. Spearman은 유효 쌍 30개 이상과 비상수 변수에만 계산한다. 상위 기술 공출현은 통합 공고의 이진 지표로 Phi를 계산한다. 범주 검정은 단일 직무 공고만 사용하고 기대빈도 5 미만이면 p값을 보류한다.

상관 쌍의 n·p·BH FDR 보정 q값을 CSV에 보존한다. 시차 상관은 연속 변화량 표본이 충분할 때만 그림을 만든다. 검정은 탐색적이며 인과관계나 직무 효과를 확정하지 않는다.''')
    code('''stats8=eda.stats_q8(snapshot)
display(stats8['associations'].round(4))
pair_rows=[]
for i,a in enumerate(stats8['rho'].columns):
    for b in stats8['rho'].columns[i+1:]:
        rho=stats8['rho'].loc[a,b]
        if pd.notna(rho):pair_rows.append({'A':a,'B':b,'n':int(stats8['n'].loc[a,b]),'rho':rho,'p':stats8['p'].loc[a,b],'q_BH':stats8['qvalue'].loc[a,b]})
pairs=pd.DataFrame(pair_rows)
if not pairs.empty:display(pairs.assign(abs_rho=pairs.rho.abs()).sort_values('abs_rho',ascending=False).drop(columns='abs_rho').head(10).round(4))
print('유효 시차 결과:',int(stats8['lags'].rho.notna().sum()), '개 · 0개이면 시차 그림 제외')
display(Markdown(eda.chart_q8(snapshot,stats8)))''',True)
    md('''## 7. Q9 분포와 이상치 판단

**목적:** 기업당 공고 수·최소 경력·마감까지 일수 등의 긴 꼬리를 확인한다. IQR 1.5배 울타리와 표준편차 3배 기준을 비교하되 이상치를 자동 삭제하지 않는다. 기업 다직무 공고는 실제 분포의 꼬리일 수 있고 마감이 게시보다 빠른 값은 별도 유효성 문제다. IQR=0인 희소 신규 수·키워드는 작은 양수도 이상치가 될 수 있다.''')
    code('''stats9=eda.stats_q9(snapshot)
display(stats9[0].round(3))
display(Markdown(eda.chart_q9(snapshot,stats9)))''',True)
    md('''## 분석 결과와 재실행

[전체 공고 웹 화면](web/index.html) · [운영 문서](docs/RUNBOOK.md)

저장 자료로 다시 분석하려면 프로젝트 폴더에서 `python scripts/run_notebook.py`를 실행한다. 통합·원천 공고, 상관 통계와 IQR 경계 CSV는 `reports/`에 생성된다. 20:00 최종 실행이 종료되면 자동 마무리 프로세스가 세 노트북과 웹 자료를 갱신한다.

이 자료는 하루의 부분 관측·플랫폼 직무 코드의 범위 차이·목록 광고와 끌어올리기·상세 수집 대기·살아남은 공고의 편향을 포함한다. 기업별 공고 수는 고용 인원이 아니며 중복 통합과 키워드 검출은 추정이다.''')
    code('''with store.connect() as conn:
    final_slot=conn.execute("SELECT status FROM crawl_run WHERE run_kind='scheduled' AND slot_ts=timestamptz '2026-10-02 20:00+09'").fetchone()
finished=bool(final_slot and final_slot[0] in ('success','partial'))
checks={'SQL/pandas 교차 검증':all(v is True for v in validation.values() if isinstance(v,bool)),
        '원천 링크 누락 0':catalog_evidence['source_links']==len(snapshot['postings']),
        '필수 값 누락 0':int(snapshot['validity'].required_missing.sum())==0,
        '2070 센티널 잔존 0':int(snapshot['validity'].sentinel_leaks.sum())==0}
assert all(checks.values()),checks
display(pd.DataFrame(checks.items(),columns=['검증','통과']))
display(Markdown('**'+('최종 수집 자료' if finished else '부분 수집 자료 · 20:00 최종 실행 대기')+'** · 기준 슬롯 '+snapshot['asof']))
report={'asof':snapshot['asof'],'final_slot':finished,'validation':validation,'catalog':catalog_evidence,'checks':checks}
Path('reports/analysis_validation.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))''')
    nb=nbf.v4.new_notebook(cells=cells,metadata={
        'kernelspec':{'display_name':'Python 3 (ipykernel)','language':'python','name':'python3'},
        'language_info':{'name':'python','version':'3.12'},
        'jobtrend':{'date':'2026-10-02','timezone':'Asia/Seoul','scope':'scheduled','kind':'EDA',
                    'generated_by':'scripts/build_eda_notebook.py','minimum_interpretations':10}})
    nbf.write(nb,Path(output_path) if output_path else ROOT/'job_platform_eda.ipynb')
    return {'notebook':'job_platform_eda.ipynb','cells':len(cells)}

if __name__=='__main__':print(build_eda())
