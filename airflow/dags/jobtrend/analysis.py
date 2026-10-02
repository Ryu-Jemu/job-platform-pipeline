"""제출용 EDA 질의·검증·도표. 모든 분석은 scheduled 범위 뷰를 읽는다."""
from pathlib import Path
import json,math
import numpy as np
import pandas as pd
from . import store

COLORS={'BE':'#0072B2','DA':'#D55E00','DE':'#009E73'}
LABELS={'saramin':'사람인','jobkorea':'잡코리아','incruit':'인크루트','linkareer':'링커리어'}
REP="""
WITH rep AS (
 SELECT DISTINCT ON(x.canonical_id) x.canonical_id,p.* FROM v_posting_scope p
 JOIN posting_canonical x USING(platform,posting_id)
 ORDER BY x.canonical_id,array_position(ARRAY['second','minute','hour','day','none'],p.posted_precision),p.first_seen_at,p.platform,p.posting_id
), features AS (
 SELECT r.*,j.n_sources,j.sources,j.job_groups,
  (SELECT count(DISTINCT s.skill) FROM posting_skill s JOIN posting_canonical x USING(platform,posting_id)
    WHERE x.canonical_id=r.canonical_id AND s.skill_group='technology') AS skill_count,
  (SELECT count(*) FROM job_canonical c JOIN v_job_scope jj USING(canonical_id) WHERE c.company_key=r.company_key) AS company_postings,
  extract(epoch FROM r.deadline_at-(SELECT max(slot_ts) FROM v_run_scope))/86400 AS days_to_deadline,
  extract(epoch FROM (SELECT max(slot_ts) FROM v_run_scope)-r.posted_at)/86400 AS posted_age_days,
  length(r.title) AS title_length,(r.deadline_kind='always')::int AS always_open
 FROM rep r JOIN v_job_scope j USING(canonical_id)
)
"""
QUERIES={
'features':REP+'SELECT * FROM features ORDER BY canonical_id',
'catalog':"WITH st AS (SELECT canonical_id,CASE WHEN bool_or(s.is_open) THEN true WHEN bool_or(s.is_open IS NULL) THEN NULL ELSE false END is_open FROM posting_canonical x JOIN v_posting_status s USING(platform,posting_id) GROUP BY canonical_id) SELECT j.*,st.is_open FROM v_job_scope j LEFT JOIN st USING(canonical_id) ORDER BY canonical_id",
'postings':"SELECT a.* FROM v_posting_all a JOIN v_posting_scope p USING(platform,posting_id) ORDER BY platform,posting_id",
'runs':"SELECT * FROM v_run_scope ORDER BY slot_ts",
'metrics':"SELECT * FROM v_listing_scope ORDER BY slot_ts,platform,query_key",
'quality':"SELECT q.* FROM quality_check q JOIN v_run_scope r USING(run_key) ORDER BY r.slot_ts,q.check_name,q.platform",
'skills':"""SELECT DISTINCT x.canonical_id,r.job_group,s.skill,s.skill_group,s.source_field
 FROM posting_skill s JOIN posting_canonical x USING(platform,posting_id)
 JOIN posting_role r USING(platform,posting_id) JOIN v_job_scope j USING(canonical_id) ORDER BY 1,2,3""",
'roles':"""SELECT DISTINCT x.canonical_id,r.job_group FROM posting_role r
 JOIN posting_canonical x USING(platform,posting_id) JOIN v_job_scope j USING(canonical_id) ORDER BY 1,2""",
'Q1':"""
WITH observed AS (
 SELECT m.platform,m.job_group,m.slot_ts,
  coalesce(m.reported_total,CASE WHEN m.is_complete THEN m.items_unique END)::numeric AS inventory,
  '표시 총건수/전수 ID'::text AS definition FROM v_listing_scope m WHERE job_group<>'MIX' AND status='ok'
 UNION ALL
 SELECT m.platform,g.job_group,m.slot_ts,
  CASE WHEN m.is_complete THEN (SELECT count(DISTINCT s.posting_id) FROM v_snapshot_scope s
    WHERE s.run_key=m.run_key AND s.platform=m.platform AND s.job_group=g.job_group) END::numeric,
  '혼합 목록 전수의 직무 분류' FROM v_listing_scope m CROSS JOIN (VALUES('BE'),('DA'),('DE')) g(job_group)
 WHERE m.job_group='MIX' AND m.status='ok'
), grid AS (
 SELECT s.slot_ts,k.platform,k.job_group,o.inventory,o.definition
 FROM generate_series(timestamptz '2026-10-02 09:00+09',timestamptz '2026-10-02 20:00+09',interval '10 minutes') s(slot_ts)
 CROSS JOIN (SELECT DISTINCT platform,job_group FROM observed) k
 LEFT JOIN observed o USING(slot_ts,platform,job_group)
), w AS (
 SELECT *,inventory-lag(inventory) OVER(PARTITION BY platform,job_group ORDER BY slot_ts) AS delta_10min,
  CASE WHEN count(inventory) OVER(PARTITION BY platform,job_group ORDER BY slot_ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)=3
   THEN avg(inventory) OVER(PARTITION BY platform,job_group ORDER BY slot_ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) END AS ma30,
  first_value(inventory) OVER(PARTITION BY platform,job_group ORDER BY (inventory IS NULL),slot_ts) AS baseline FROM grid
) SELECT *,100*inventory/nullif(baseline,0) AS idx100 FROM w ORDER BY platform,job_group,slot_ts
""",
'Q2':"""
WITH fulls AS (
 SELECT *,lag(run_key) OVER(PARTITION BY platform,query_key ORDER BY slot_ts) AS prev_run,
  lag(slot_ts) OVER(PARTITION BY platform,query_key ORDER BY slot_ts) AS prev_slot
 FROM v_listing_scope WHERE is_complete
), counts AS (
 SELECT f.platform,f.query_key,f.job_group,f.slot_ts,f.prev_slot,
  (SELECT count(DISTINCT posting_id) FROM v_snapshot_scope s WHERE s.run_key=f.run_key AND s.platform=f.platform AND s.query_key=f.query_key) AS current_n,
  (SELECT count(DISTINCT posting_id) FROM v_snapshot_scope s WHERE s.run_key=f.prev_run AND s.platform=f.platform AND s.query_key=f.query_key) AS previous_n,
  CASE WHEN f.prev_run IS NOT NULL THEN (SELECT count(*) FROM
   (SELECT DISTINCT posting_id FROM v_snapshot_scope s WHERE s.run_key=f.run_key AND s.platform=f.platform AND s.query_key=f.query_key
    EXCEPT SELECT DISTINCT posting_id FROM v_snapshot_scope s WHERE s.run_key=f.prev_run AND s.platform=f.platform AND s.query_key=f.query_key) a) END AS added,
  CASE WHEN f.prev_run IS NOT NULL THEN (SELECT count(*) FROM
   (SELECT DISTINCT posting_id FROM v_snapshot_scope s WHERE s.run_key=f.prev_run AND s.platform=f.platform AND s.query_key=f.query_key
    EXCEPT SELECT DISTINCT posting_id FROM v_snapshot_scope s WHERE s.run_key=f.run_key AND s.platform=f.platform AND s.query_key=f.query_key) a) END AS removed
 FROM fulls f
) SELECT *,sum(added) OVER(PARTITION BY platform,query_key ORDER BY slot_ts) AS cum_added,
 sum(removed) OVER(PARTITION BY platform,query_key ORDER BY slot_ts) AS cum_removed FROM counts ORDER BY platform,query_key,slot_ts
""",
'Q3':"""
WITH company AS (SELECT c.company_key,min(j.company_name) company_name,count(*) n
 FROM v_job_scope j JOIN job_canonical c USING(canonical_id) GROUP BY c.company_key)
SELECT *,dense_rank() OVER(ORDER BY n DESC) ranking, ntile(20) OVER(ORDER BY n DESC,company_key) bucket20,
 n::numeric/sum(n) OVER() AS share, sum(n) OVER(ORDER BY n DESC,company_key ROWS UNBOUNDED PRECEDING)::numeric/sum(n) OVER() AS cum_share
FROM company ORDER BY n DESC,company_key
""",
'Q4':REP+"""
SELECT g.job_group,f.career_type,count(*) n,count(*)::numeric/sum(count(*)) OVER(PARTITION BY g.job_group) AS share,
 percentile_cont(.5) WITHIN GROUP(ORDER BY f.career_min_yr) AS median_min_years
FROM features f CROSS JOIN LATERAL unnest(f.job_groups) g(job_group) GROUP BY 1,2 ORDER BY 1,2
""",
'Q5':REP+"""
, counts AS (SELECT (posted_at AT TIME ZONE 'Asia/Seoul')::date posted_day,count(*) n
 FROM features WHERE posted_at IS NOT NULL GROUP BY 1),calendar AS (
 SELECT d::date posted_day,coalesce(n,0) n FROM generate_series(
  (SELECT min(posted_day) FROM counts),(SELECT max(posted_day) FROM counts),interval '1 day') d LEFT JOIN counts ON counts.posted_day=d::date)
SELECT *,avg(n) OVER(ORDER BY posted_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) ma7 FROM calendar ORDER BY posted_day
""",
'Q6':"""SELECT sources,n_sources,count(*) n,count(*)::numeric/sum(count(*)) OVER() share
 FROM v_job_scope GROUP BY sources,n_sources ORDER BY n DESC""",
'Q7':"""
WITH r AS (SELECT DISTINCT x.canonical_id,r.job_group FROM posting_role r JOIN posting_canonical x USING(platform,posting_id) JOIN v_job_scope j USING(canonical_id)),
 s AS (SELECT DISTINCT x.canonical_id,sk.skill,sk.skill_group FROM posting_skill sk JOIN posting_canonical x USING(platform,posting_id) JOIN v_job_scope j USING(canonical_id)),
 freq AS (SELECT r.job_group,s.skill,s.skill_group,count(*) n FROM r JOIN s USING(canonical_id) GROUP BY 1,2,3),
 denom AS (SELECT job_group,count(*) n FROM r GROUP BY 1),overall AS (SELECT skill,count(*) n FROM s GROUP BY 1)
SELECT f.*,f.n::numeric/d.n AS share,(f.n::numeric/d.n)/(o.n::numeric/(SELECT count(*) FROM v_job_scope)) AS lift
FROM freq f JOIN denom d USING(job_group) JOIN overall o USING(skill) ORDER BY job_group,n DESC,skill
""",
'Q8':REP+'SELECT canonical_id,career_min_yr,career_max_yr,skill_count,days_to_deadline,posted_age_days,n_sources,title_length,company_postings,always_open FROM features ORDER BY 1',
'Q9':REP+"""SELECT g.job_group,percentile_cont(.25) WITHIN GROUP(ORDER BY career_min_yr) q1,
 percentile_cont(.75) WITHIN GROUP(ORDER BY career_min_yr) q3,stddev_samp(career_min_yr) sd,
 count(career_min_yr) n FROM features CROSS JOIN LATERAL unnest(job_groups) g(job_group) GROUP BY 1 ORDER BY 1""",
'missingness':"""SELECT platform,count(*) n,
 avg((posted_at IS NULL)::int)::float posted_at,avg((sido IS NULL)::int)::float sido,
 avg((career_min_yr IS NULL)::int)::float career_min_yr,avg((education IS NULL)::int)::float education,
 avg((deadline_kind='unknown')::int)::float deadline_unknown,
 avg((deadline_at IS NULL AND deadline_kind IN('always','until_filled'))::int)::float meaningful_deadline_null,
 avg((employment_type IS NULL)::int)::float employment_type
FROM v_posting_scope GROUP BY platform ORDER BY platform""",
'source_status':"SELECT * FROM source_state ORDER BY platform",
'operating':"""SELECT platform,outcome,count(*) pages,sum(attempts) attempts,sum(octet_length(body)) bytes,
 min(fetched_at) first_fetch,max(fetched_at) last_fetch FROM raw_listing_page r JOIN v_run_scope s USING(run_key) GROUP BY 1,2 ORDER BY 1,2""",
'new10':"""SELECT platform,query_key,job_group,slot_ts,
 CASE WHEN row_number() OVER(PARTITION BY platform,query_key ORDER BY slot_ts)>1 THEN new_on_page1 END AS new_n
 FROM v_listing_scope ORDER BY platform,query_key,slot_ts""",
'full_sets':"""SELECT DISTINCT m.run_key,m.platform,m.query_key,m.slot_ts,s.posting_id
 FROM v_listing_scope m JOIN v_snapshot_scope s USING(run_key,platform,query_key) WHERE m.is_complete ORDER BY 2,3,4,5""",
'validity':"""SELECT platform,count(*) n,count(*) FILTER(WHERE deadline_at < posted_at) deadline_before_posted,
 count(*) FILTER(WHERE career_min_yr > career_max_yr) invalid_career,
 count(*) FILTER(WHERE extract(year FROM deadline_at)>=2070) sentinel_leaks,
 count(*) FILTER(WHERE company_name IN('','(미상)') OR title='' OR url='') required_missing
 FROM v_posting_scope GROUP BY 1 ORDER BY 1""",
'raw_tags':"SELECT tag,count(*) n FROM v_posting_scope CROSS JOIN LATERAL unnest(tags_raw) tag GROUP BY tag ORDER BY n DESC LIMIT 80",
'company_variants':"SELECT company_key,count(DISTINCT company_name) variants,array_agg(DISTINCT company_name) names FROM v_posting_scope GROUP BY company_key HAVING count(DISTINCT company_name)>1 ORDER BY variants DESC LIMIT 20",
}

def frame(conn,sql):
    cur=conn.execute(sql)
    return pd.DataFrame(cur.fetchall(),columns=[d.name for d in cur.description])

def load_snapshot():
    with store.connect() as conn:
        conn.commit();conn.execute('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
        data={k:frame(conn,q) for k,q in QUERIES.items()}
        data['asof']=str(conn.execute('SELECT max(slot_ts) FROM v_run_scope').fetchone()[0])
        data['db_bytes']=conn.execute('SELECT pg_database_size(current_database())').fetchone()[0]
    for key,q in QUERIES.items():
        Path('sql').mkdir(exist_ok=True);Path('sql',key+'.sql').write_text(q.strip()+';\n')
    return data

def validate(data):
    checks={}
    f,c,r=data['features'],data['catalog'],data['roles']
    assert f.canonical_id.is_unique and c.canonical_id.is_unique
    assert set(f.canonical_id)==set(c.canonical_id)
    assert sum(c.n_postings)==len(data['postings'])
    checks['canonical_coverage']=len(c);checks['platform_postings']=len(data['postings'])
    # SQL 회사 집계와 pandas 독립 집계 일치
    actual=f.groupby('company_key').size().sort_index()
    expected=data['Q3'].set_index('company_key').n.sort_index()
    pd.testing.assert_series_equal(actual,expected,check_names=False,check_dtype=False)
    checks['company_sql_pandas']=True
    # 총건수 window LAG·30분 이동평균을 별도 경로로 계산
    ts=data['Q1'].copy()
    for _,g in ts.groupby(['platform','job_group']):
        y=pd.to_numeric(g.inventory,errors='coerce').astype(float)
        np.testing.assert_allclose(pd.to_numeric(g.delta_10min).astype(float),y.diff(),equal_nan=True)
        np.testing.assert_allclose(pd.to_numeric(g.ma30).astype(float),y.rolling(3,min_periods=3).mean(),equal_nan=True)
    checks['window_sql_pandas']=True
    q=data['Q2'];valid=q.prev_slot.notna()
    assert ((q.loc[valid,'added']-q.loc[valid,'removed'])==(q.loc[valid,'current_n']-q.loc[valid,'previous_n'])).all()
    # 실제 집합 차집합으로 SQL 유입·이탈 수를 교차 검증
    sets=data['full_sets']
    for _,row in q[valid].iterrows():
        ss=sets[(sets.platform==row.platform)&(sets.query_key==row.query_key)]
        cur=set(ss.loc[ss.slot_ts==row.slot_ts,'posting_id']);prev=set(ss.loc[ss.slot_ts==row.prev_slot,'posting_id'])
        assert len(cur-prev)==row.added and len(prev-cur)==row.removed
    checks['flow_sql_set_pandas']=True
    # canonical 기술 빈도는 원천별 역할 목록의 합집합과 결합: 플랫폼 중복으로 부풀리지 않는다
    sk=data['skills'][['canonical_id','skill','skill_group']].drop_duplicates()
    freq=r.merge(sk,on='canonical_id').groupby(['job_group','skill','skill_group']).size().sort_index()
    expected=data['Q7'].set_index(['job_group','skill','skill_group']).n.sort_index()
    pd.testing.assert_series_equal(freq,expected,check_names=False,check_dtype=False)
    checks['skill_sql_pandas']=True
    return checks

def setup_plots():
    import matplotlib.pyplot as plt
    from matplotlib import font_manager
    font=Path('/System/Library/Fonts/AppleSDGothicNeo.ttc')
    font_manager.fontManager.addfont(str(font))
    plt.rcParams.update({'font.family':font_manager.FontProperties(fname=str(font)).get_name(),
      'axes.unicode_minus':False,'figure.dpi':120,'font.size':10,'axes.titlesize':12,'axes.spines.top':False,'axes.spines.right':False})
    return str(font)

def savefig(fig,name):
    import matplotlib.pyplot as plt
    fig.tight_layout();Path('reports/figures').mkdir(parents=True,exist_ok=True)
    fig.savefig(Path('reports/figures',name+'.png'),dpi=150,bbox_inches='tight')
    plt.show();plt.close(fig)


def export_catalog(data):
    fields=['canonical_id','platform','location_raw','sido','career_raw','career_type','career_min_yr',
            'career_max_yr','education','employment_type','deadline_at','deadline_kind','posted_precision']
    conditions=data['features'][fields].rename(columns={'platform':'representative_platform','location_raw':'region'})
    c=data['catalog'].merge(conditions,on='canonical_id',validate='one_to_one')
    # 기술 UI의 빈도·필터에 역량 키워드를 섞지 않고 각각 보존한다.
    for group,column in [('technology','skills'),('competency','competencies')]:
        selected=data['skills'][data['skills'].skill_group.eq(group)][['canonical_id','skill']].drop_duplicates()
        by_id=selected.sort_values('skill').groupby('canonical_id').skill.agg(list)
        c[column]=c.canonical_id.map(by_id).map(lambda v:v if isinstance(v,list) else [])
    c['asof']=data['asof']
    c.to_csv('reports/all_jobs.csv',index=False,encoding='utf-8-sig')
    data['postings'].to_csv('reports/all_platform_postings.csv',index=False,encoding='utf-8-sig')
    records=json.loads(c.to_json(orient='records',date_format='iso',force_ascii=False))
    assert sum(len(r['source_links']) for r in records)==len(data['postings'])
    return {'canonical_rows':len(c),'platform_rows':len(data['postings']),'source_links':sum(len(r['source_links']) for r in records)}

def chart_q1(data):
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    fig,axes=plt.subplots(3,2,figsize=(15,10),sharex=True)
    styles=['-','--',':','-.']
    for row,role in zip(axes,COLORS):
        for i,(platform,g) in enumerate(data['Q1'][data['Q1'].job_group==role].groupby('platform')):
            x=pd.to_datetime(g.slot_ts,utc=True).dt.tz_convert('Asia/Seoul')
            for ax,field in zip(row,('inventory','idx100')):
                ax.plot(x,pd.to_numeric(g[field]),label=LABELS[platform],color=COLORS[role],linestyle=styles[i],marker='.',ms=3)
        row[0].set_title(role+' · 표시 총건수/완료 전수');row[0].set_ylabel('공고 수 (건)')
        row[1].set_title(role+' · 원천별 최초 관측=100');row[1].set_ylabel('지수');row[1].axhline(100,color='grey',alpha=.3,lw=.7)
        for ax in row:ax.grid(alpha=.2)
    axes[0,0].legend(ncol=2,fontsize=8);axes[0,1].legend(ncol=2,fontsize=8)
    ticks=pd.to_datetime(data['runs'].slot_ts,utc=True).dt.tz_convert('Asia/Seoul')
    for ax in axes[-1]:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M',tz=__import__('zoneinfo').ZoneInfo('Asia/Seoul')))
        ax.set_xlim(ticks.min()-pd.Timedelta(minutes=3),ticks.max()+pd.Timedelta(minutes=3))
        ax.set_xlabel('2026-10-02 KST · 실제 관측 구간')
    savefig(fig,'Q1_inventory')
    n=int(data['Q1'].inventory.notna().sum())
    return f'✍️ 확인된 원천·직무·슬롯 관측값은 {n:,}개다. 오전 가동 전과 아직 도래하지 않은 슬롯은 결측이며, 링커리어는 전수 스캔 시점의 분류 건수만 표시한다. 원천 간 총건수 정의가 달라 크기를 직접 시장 점유율로 해석할 수 없다.'

def chart_q2(data):
    import matplotlib.pyplot as plt
    import seaborn as sns
    q=data['Q2'];valid=q[q.prev_slot.notna()].copy()
    fig,axes=plt.subplots(1,2,figsize=(14,5))
    if len(valid):
        grouped=valid.groupby(['platform','query_key'])[['added','removed']].sum()
        x=np.arange(len(grouped));axes[0].bar(x,grouped.added,label='유입',color='#0072B2');axes[0].bar(x,-grouped.removed,label='이탈',color='#D55E00')
        axes[0].set_xticks(x,[f'{LABELS[p]} {g}' for p,g in grouped.index],rotation=40,ha='right');axes[0].legend();axes[0].axhline(0,color='#333',lw=.7)
    else:axes[0].text(.5,.5,'연속 완료 전수 스캔이 아직 없음',ha='center',transform=axes[0].transAxes)
    n=data['new10'].copy();n['hour']=pd.to_datetime(n.slot_ts,utc=True).dt.tz_convert('Asia/Seoul').dt.hour
    heat=n[n.new_n.notna()&n.job_group.ne('MIX')].pivot_table(index='job_group',columns='hour',values='new_n',aggfunc='sum').reindex(list(COLORS))
    if heat.size:sns.heatmap(heat,ax=axes[1],annot=True,fmt='.0f',cmap='Blues',cbar_kws={'label':'관측 신규 ID 수'})
    axes[0].set_title('연속 완료 전수 간 유입·이탈');axes[0].set_ylabel('공고 수 (건)');axes[1].set_title('첫 기준선 제외 · 첫 페이지 신규 ID');axes[1].set_xlabel('KST 시각');axes[1].set_ylabel('직무')
    savefig(fig,'Q2_flow')
    return f'✍️ 비교 가능한 전수 간격 {len(valid):,}개에서 유입 {int(valid.added.sum()):,}건, 이탈 {int(valid.removed.sum()):,}건을 관측했다. 이는 최초 게시·실제 채용 성사 건수가 아니라 목록 ID의 등장·부재다. 같은 공고의 직무 중복과 시간 간격 차이가 있으므로 각 원천·질의 안에서 비교한다.'

def chart_q3(data):
    import matplotlib.pyplot as plt
    q=data['Q3'];x=np.sort(q.n.to_numpy(dtype=float));n=len(x)
    gini=(2*np.dot(np.arange(1,n+1),x)/(n*x.sum())-(n+1)/n) if n and x.sum() else 0
    curve=np.r_[0,np.cumsum(x)/x.sum()];share=np.arange(n+1)/n
    gini2=1-2*np.trapezoid(curve,share);assert abs(gini-gini2)<1e-10
    top5=q.n.iloc[:max(1,math.ceil(n*.05))].sum()/q.n.sum();top10=q.n.iloc[:max(1,math.ceil(n*.1))].sum()/q.n.sum()
    fig,axes=plt.subplots(1,2,figsize=(14,6));top=q.head(15).iloc[::-1]
    axes[0].barh(top.company_name,top.n,color='#0072B2');axes[0].set_title('통합 공고 수 상위 15개 기업');axes[0].set_xlabel('공고 수 (건)')
    axes[1].plot(share,curve,label=f'Gini={gini:.3f}',color='#0072B2');axes[1].plot([0,1],[0,1],'--',color='grey',label='균등 분포');axes[1].set_title('기업별 공고 수 로렌츠 곡선');axes[1].set_xlabel('기업 누적 비율');axes[1].set_ylabel('공고 누적 비율');axes[1].legend()
    savefig(fig,'Q3_concentration')
    return f'✍️ 정규화 기업 {n:,}개 중 상위 5%의 공고 점유율은 {top5:.1%}, 상위 10%는 {top10:.1%}, Gini는 {gini:.3f}다. 헤드헌팅 업체와 다직무 공고가 포함돼 기업 공고 수가 고용 인원을 뜻하지 않는다.'

def chart_q4(data):
    import matplotlib.pyplot as plt
    q=data['Q4'].copy();q['share']=pd.to_numeric(q.share)
    fig,axes=plt.subplots(1,3,figsize=(15,5))
    pivot=q.pivot(index='job_group',columns='career_type',values='share').fillna(0).reindex(list(COLORS))
    pivot.plot.bar(stacked=True,ax=axes[0],colormap='tab20');axes[0].set_title('직무별 경력 유형');axes[0].set_ylabel('직무 내 비율');axes[0].legend(fontsize=8)
    f=data['roles'].merge(data['features'],on='canonical_id')
    for ax,field,title in [(axes[1],'sido','근무 시도'),(axes[2],'deadline_kind','마감 유형')]:
        x=pd.crosstab(f.job_group,f[field].fillna('미상'),normalize='index').reindex(list(COLORS))
        x.plot.bar(stacked=True,ax=ax,colormap='tab20');ax.set_title(title);ax.set_ylabel('직무 내 비율');ax.legend(fontsize=7)
    for ax in axes:ax.set_xlabel('직무');ax.tick_params(axis='x',rotation=0)
    savefig(fig,'Q4_conditions')
    shares=q[q.career_type.isin(['new','new_or_exp'])].groupby('job_group').share.sum()
    capital=f.sido.isin(['서울','경기','인천']).mean()
    return '✍️ 신입을 포함하는 공고 비율은 '+', '.join(f'{g} {shares.get(g,0):.1%}' for g in COLORS)+f'다. 대표 공고 기준 수도권 비율은 {capital:.1%}이며 지역 결측도 분모에 포함했다. 직무가 여러 개인 통합 공고는 각 직무 안에서 한 번씩 집계한다.'

def chart_q5(data):
    import matplotlib.pyplot as plt
    q=data['Q5'];fig,axes=plt.subplots(1,2,figsize=(14,4))
    axes[0].plot(pd.to_datetime(q.posted_day),q.n,label='일별',color='#999');axes[0].plot(pd.to_datetime(q.posted_day),pd.to_numeric(q.ma7),label='7일 이동평균',color='#0072B2');axes[0].legend();axes[0].set_title('관측 중인 공고의 게시일 분포');axes[0].set_xlabel('게시일 KST');axes[0].set_ylabel('통합 공고 수 (건)')
    age=pd.to_numeric(data['features'].posted_age_days,errors='coerce').dropna();axes[1].hist(age.clip(lower=0),bins=40,color='#0072B2');axes[1].set_title('대표 게시일의 경과일 분포');axes[1].set_xlabel('관측 슬롯 대비 경과일');axes[1].set_ylabel('통합 공고 수 (건)')
    savefig(fig,'Q5_posted_dates')
    missing=data['features'].posted_at.isna().mean()
    return f'✍️ 대표 게시 시각 결측률은 {missing:.1%}다. 게시일 분포는 이번에 목록에 남아 있던 공고의 분포이며 과거 일별 채용 수요 추이가 아니다. 상대 시각은 첫 관측 값으로 고정했고, 등록과 수정 표기는 구분했다.'

def chart_q6(data):
    import matplotlib.pyplot as plt
    import seaborn as sns
    c=data['catalog'];fig,axes=plt.subplots(1,3,figsize=(16,5))
    counts=c.n_sources.value_counts().sort_index();axes[0].bar(counts.index,counts.values,color='#0072B2');axes[0].set_title('통합 공고별 원천 수');axes[0].set_xlabel('원천 수');axes[0].set_ylabel('통합 공고 수 (건)')
    combos=data['Q6'].head(8).iloc[::-1];axes[1].barh(combos.sources.map(lambda s:' + '.join(LABELS[p] for p in s)),combos.n,color='#0072B2');axes[1].set_title('중복 게시 원천 조합');axes[1].set_xlabel('통합 공고 수 (건)')
    platforms=sorted({p for s in c.sources for p in s});sets={p:set(c.loc[c.sources.map(lambda s:p in s),'canonical_id']) for p in platforms}
    mat=pd.DataFrame([[len(sets[a]&sets[b])/len(sets[a]|sets[b]) if sets[a]|sets[b] else np.nan for b in platforms] for a in platforms],index=[LABELS[p] for p in platforms],columns=[LABELS[p] for p in platforms])
    sns.heatmap(mat,annot=True,fmt='.3f',cmap='Blues',vmin=0,vmax=1,ax=axes[2]);axes[2].set_title('원천 쌍 Jaccard')
    savefig(fig,'Q6_overlap')
    rate=c.n_sources.gt(1).mean()
    return f'✍️ 현재 판정에서 여러 원천에 게시된 통합 공고는 {int(c.n_sources.gt(1).sum()):,}건({rate:.1%})이다. 회사·제목·마감이 호환되는 후보를 묶은 추정치이며, 임계값별 민감도와 표본 검토를 함께 읽어야 한다. 링크 {len(data["postings"]):,}개를 전체 목록에 보존했다.'

def chart_q7(data):
    import matplotlib.pyplot as plt
    import seaborn as sns
    from wordcloud import WordCloud
    font=setup_plots();q=data['Q7'];tech=q[q.skill_group=='technology'];comp=q[q.skill_group=='competency']
    fig,axes=plt.subplots(2,3,figsize=(15,10))
    for i,g in enumerate(COLORS):
        x=tech[tech.job_group==g].head(20).iloc[::-1]
        axes[0,i].barh(x.skill,x.n,color=COLORS[g]);axes[0,i].set_title(g+' · 기술 상위 20');axes[0,i].set_xlabel('통합 공고 수 (건)')
        freq=dict(zip(x.skill,x.n.astype(int)))
        if freq:
            wc=WordCloud(font_path=font,width=700,height=350,background_color='white',random_state=42,collocations=False).generate_from_frequencies(freq)
            axes[1,i].imshow(wc,interpolation='bilinear')
        else:axes[1,i].text(.5,.5,'관측 기술 키워드 없음',ha='center')
        axes[1,i].axis('off');axes[1,i].set_title(g+' · 동일 공고 1회')
    savefig(fig,'Q7_technologies')
    fig,axes=plt.subplots(1,2,figsize=(14,5))
    # role union 중복을 제거한 competency canonical 수
    s=data['skills'];freq=s[s.skill_group=='competency'][['canonical_id','skill']].drop_duplicates().groupby('skill').size()
    if len(freq):
        wc=WordCloud(font_path=font,width=700,height=350,background_color='white',random_state=42,collocations=False).generate_from_frequencies(freq.to_dict());axes[0].imshow(wc)
    else:axes[0].text(.5,.5,'관측 역량 키워드 없음',ha='center')
    axes[0].axis('off');axes[0].set_title('역량 · 통합 공고 단위')
    top=tech.groupby('skill').n.sum().nlargest(18).index
    pivot=tech[tech.skill.isin(top)].pivot(index='skill',columns='job_group',values='lift').reindex(columns=list(COLORS)).fillna(0).astype(float)
    if pivot.size:sns.heatmap(pivot,annot=True,fmt='.1f',cmap='Blues',ax=axes[1]);axes[1].set_title('기술 Lift · 직무 점유율 / 전체 점유율')
    savefig(fig,'Q7_competencies_lift')
    coverage=data['features'].skill_count.gt(0).mean()
    return f'✍️ 기술 키워드가 한 개 이상 검출된 통합 공고는 {coverage:.1%}다. 제목·목록 태그와 읽을 수 있는 상세 텍스트를 사전으로 검색한 결과다. 이미지로만 제공된 자격요건과 사전 밖 표현은 관측하지 못하므로 미검출이 요구 없음으로 해석되지 않는다. 워드클라우드의 크기는 수요 인원이 아니라 공고 빈도다.'

def stats_q8(data):
    from scipy import stats
    f=data['Q8'].set_index('canonical_id').apply(pd.to_numeric,errors='coerce').astype(float)
    columns=list(f);corr=pd.DataFrame(np.nan,index=columns,columns=columns);p=corr.copy();n=corr.copy()
    for a in columns:
        for b in columns:
            v=f[[a,b]].dropna() if a!=b else f[[a]].dropna();n.loc[a,b]=len(v)
            if len(v)>=30 and v.iloc[:,0].nunique()>1 and v.iloc[:,-1].nunique()>1:
                if a==b:corr.loc[a,b]=1;p.loc[a,b]=0
                else:res=stats.spearmanr(v[a],v[b]);corr.loc[a,b]=res.statistic;p.loc[a,b]=res.pvalue
    # 상위 기술 공출현 phi: canonical 한 번, 이진 상관의 Pearson과 동일
    sk=data['skills'];tech=sk[sk.skill_group=='technology'][['canonical_id','skill']].drop_duplicates()
    top=tech.skill.value_counts().head(15).index
    binary=pd.crosstab(tech.canonical_id,tech.skill).reindex(index=data['catalog'].canonical_id,columns=top,fill_value=0).gt(0).astype(int)
    phi=binary.corr()
    qvalue=p.copy()*np.nan
    valid_pairs=[(a,b) for i,a in enumerate(columns) for b in columns[i+1:] if pd.notna(p.loc[a,b])]
    if valid_pairs:
        raw=np.array([p.loc[a,b] for a,b in valid_pairs]);order=np.argsort(raw)
        adjusted=np.minimum.accumulate((raw[order]*len(raw)/np.arange(1,len(raw)+1))[::-1])[::-1].clip(0,1)
        for i,value in zip(order,adjusted):
            a,b=valid_pairs[i];qvalue.loc[a,b]=qvalue.loc[b,a]=value
    # 중복 직무 공고를 제외하여 독립 관측 단위를 유지
    ff=data['features'];exclusive=ff[ff.job_groups.map(len).eq(1)].copy();exclusive['role']=exclusive.job_groups.map(lambda x:x[0]);exclusive['cross_posted']=exclusive.n_sources.gt(1)
    exclusive['region']=exclusive.sido.fillna('미상').where(exclusive.sido.isin(['서울','경기','인천']), '기타/미상')
    rows=[]
    for a,b in [('role','career_type'),('role','region'),('platform','career_type'),('cross_posted','role')]:
        table=pd.crosstab(exclusive[a],exclusive[b]);nn=table.to_numpy().sum()
        if min(table.shape)>=2:
            chi,pp,dof,expected=stats.chi2_contingency(table,correction=False)
            v=math.sqrt(chi/(nn*(min(table.shape)-1)))
            reliable=bool((expected>=5).all())
            rows.append({'variables':a+' × '+b,'n':nn,'chi2':chi,'p':pp if reliable else np.nan,'cramers_v':v,'min_expected':expected.min(),'p_valid':reliable})
    associations=pd.DataFrame(rows)
    # 10분 변화량의 동시/시차 상관: 완성된 연속 슬롯 12개 미만이면 보류
    q=data['Q1'];lags=[]
    for role in COLORS:
        t=q[q.job_group==role].pivot(index='slot_ts',columns='platform',values='delta_10min').astype(float)
        for i,a in enumerate(t.columns):
            for b in t.columns[i+1:]:
                for lag in [-2,-1,0,1,2]:
                    pair=pd.concat([t[a],t[b].shift(lag)],axis=1).dropna()
                    # 누락을 건너뛴 관측을 연속 표본으로 오인하지 않는다.
                    if len(pair):
                        breaks=pd.Series(pd.DatetimeIndex(pair.index).to_series().diff().ne(pd.Timedelta(minutes=10)).to_numpy(),index=pair.index).cumsum()
                        pair=max((g for _,g in pair.groupby(breaks)),key=len)
                    nn=len(pair)
                    value=pair.iloc[:,0].corr(pair.iloc[:,1],method='spearman') if nn>=12 and (pair.nunique()>1).all() else np.nan
                    lags.append({'role':role,'a':a,'b':b,'lag_10min':lag,'n':nn,'rho':value})
    results={'rho':corr,'p':p,'qvalue':qvalue,'n':n,'phi':phi,'associations':associations,'lags':pd.DataFrame(lags),'excluded_multirole':len(ff)-len(exclusive)}
    for key in ('rho','p','qvalue','n','phi','associations','lags'):results[key].to_csv('reports/Q8_'+key+'.csv',encoding='utf-8-sig')
    return results

def chart_q8(data,results):
    import matplotlib.pyplot as plt
    import seaborn as sns
    fig,axes=plt.subplots(1,2,figsize=(16,7))
    for ax,key,title in [(axes[0],'rho','공고 특성 Spearman · 최소 30쌍'),(axes[1],'phi','상위 기술 공출현 Phi')]:
        mat=results[key]
        if mat.notna().to_numpy().any():sns.heatmap(mat,mask=mat.isna(),annot=True,fmt='.2f',vmin=-1,vmax=1,center=0,cmap='RdBu_r',ax=ax,annot_kws={'fontsize':7})
        else:ax.text(.5,.5,'분산/표본 부족으로 계산 보류',ha='center',transform=ax.transAxes)
        ax.set_title(title)
    savefig(fig,'Q8_correlations')
    lag=results['lags'];finite=lag[lag.rho.notna()]
    if len(finite):
        # 직무·원천 쌍을 섞어 평균하지 않고 각 시계열의 탐색 결과를 보존한다.
        pairs=list(finite.groupby(['role','a','b']))
        fig,axes=plt.subplots(len(pairs),1,figsize=(10,3*len(pairs)),squeeze=False)
        for ax,((role,a,b),rows) in zip(axes.flat,pairs):
            ax.plot(rows.lag_10min,rows.rho,marker='o',color=COLORS[role]);ax.set_ylim(-1,1)
            ax.set_title(f'{role} · {LABELS[a]} / {LABELS[b]}');ax.set_ylabel('Spearman rho')
        axes[-1,0].set_xlabel('시차 (10분; 양수는 B의 과거 값)');savefig(fig,'Q8_lag')
    else:
        Path('reports/figures/Q8_lag.png').unlink(missing_ok=True)
    return f'✍️ 상관은 인과관계를 뜻하지 않는다. p값·유효 쌍 수·상관 쌍의 BH FDR 보정 q값을 CSV에 보존하고 결측을 쌍별로 제외했다. 범주 검정은 단일 직무 공고만 사용해 복수 직무 {results["excluded_multirole"]:,}건을 제외했으며 기대빈도 5 미만인 검정의 p값은 보류한다. 범주 검정은 보정 전 탐색 통계이며, 어느 결과도 확증적 인과 결론으로 사용하지 않는다.'

def stats_q9(data):
    from scipy import stats
    f=data['features'];rows=[];outliers=[]
    values={'기업당 공고 수':data['Q3'].n,'마감까지 일수':pd.to_numeric(f.days_to_deadline),
      '최소 경력':pd.to_numeric(f.career_min_yr),'기술 수':f.skill_count,
      '10분 신규 수':pd.to_numeric(data['new10'].new_n)}
    for name,series in values.items():
        s=series.dropna().astype(float);q1,q3=s.quantile([.25,.75]);iqr=q3-q1;lo=q1-1.5*iqr;hi=q3+1.5*iqr
        mask=(s<lo)|(s>hi);sd=s.std();stdmask=(s<s.mean()-3*sd)|(s>s.mean()+3*sd)
        rows.append({'변수':name,'n':len(s),'q1':q1,'q3':q3,'IQR':iqr,'lower':lo,'upper':hi,'IQR_outliers':int(mask.sum()),'std_outliers':int(stdmask.sum()),'skew':s.skew(),'kurtosis':s.kurt()})
        for idx,value in s[mask].head(30).items():outliers.append({'variable':name,'row':idx,'value':value,'lower':lo,'upper':hi})
    exclusive=f[f.job_groups.map(len).eq(1)]
    groups=[pd.to_numeric(exclusive.loc[exclusive.job_groups.map(lambda x:x[0]==g),'career_min_yr']).dropna().astype(float) for g in COLORS]
    if all(len(g)>=5 for g in groups) and len(set(np.concatenate(groups)))>1:
        result=stats.kruskal(*groups);kruskal={'H':result.statistic,'p':result.pvalue,'n':[len(g) for g in groups]}
    else:kruskal={'status':'표본/분산 부족으로 보류','n':[len(g) for g in groups]}
    bounds=pd.DataFrame(rows);bounds.to_csv('reports/Q9_bounds.csv',index=False,encoding='utf-8-sig');pd.DataFrame(outliers).to_csv('reports/Q9_outliers.csv',index=False,encoding='utf-8-sig')
    return bounds,pd.DataFrame(outliers),kruskal

def chart_q9(data,results):
    import matplotlib.pyplot as plt
    import seaborn as sns
    bounds,outliers,kw=results
    f=data['roles'].merge(data['features'],on='canonical_id');fig,axes=plt.subplots(1,3,figsize=(15,5))
    sns.boxplot(data=f,x='job_group',y='career_min_yr',order=list(COLORS),hue='job_group',palette=COLORS,legend=False,ax=axes[0],whis=1.5);axes[0].set_title('최소 경력 · IQR 울타리 1.5');axes[0].set_xlabel('직무');axes[0].set_ylabel('년')
    axes[1].hist(data['Q3'].n,bins=np.geomspace(1,max(2,data['Q3'].n.max()),25),color='#0072B2');axes[1].set_xscale('log');axes[1].set_title('기업당 공고 수 · 로그 축');axes[1].set_xlabel('기업당 통합 공고 수 (건)');axes[1].set_ylabel('기업 수')
    dd=pd.to_numeric(data['features'].days_to_deadline).dropna();axes[2].boxplot(dd,whis=1.5);axes[2].set_title('마감까지 일수 · IQR 울타리 1.5');axes[2].set_ylabel('일');axes[2].set_xticks([])
    savefig(fig,'Q9_outliers')
    total=int(bounds.IQR_outliers.sum())
    return f'✍️ 변수별 IQR 규칙은 총 {total:,}개 관측값을 표시했다(동일 공고의 변수 중복 포함). IQR=0인 희소 키워드·신규 수에서는 양수가 모두 이상치가 될 수 있어 오류 판정으로 쓰지 않는다. 상시 마감의 NULL과 2070 센티널은 실제 마감까지 일수로 계산하지 않는다. Kruskal-Wallis 결과 {kw}도 탐색적이며 직무 분류·결측의 차이를 함께 고려해야 한다.'

def chart_quality(data):
    import matplotlib.pyplot as plt
    import seaborn as sns
    m=data['missingness'].set_index('platform').drop(columns=['n']);fig,ax=plt.subplots(figsize=(12,5))
    sns.heatmap(m,annot=True,fmt='.1%',vmin=0,vmax=1,cmap='Oranges',ax=ax);ax.set_title('원천별 결측률 · 의미 있는 마감 NULL 별도');ax.set_xlabel('필드');ax.set_ylabel('원천');savefig(fig,'quality_missingness')
    return '✍️ 목록이 제공하지 않는 학력·고용형태는 구조적 결측, 상시·채용시 마감일은 의미 있는 NULL, 상세 수집 대기와 수정 시각은 부분 결측, 양의 표시 총건수인데 파싱 0건인 경우는 파싱 실패로 구분한다. 매칭에 필요한 회사·제목·URL과 유효성 규칙을 별도로 점검했다.'
