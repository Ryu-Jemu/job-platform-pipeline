"""링커리어 SSR 목록. Apollo 그래프를 화이트리스트 JSON으로 축소해 저장한다."""
import json
from datetime import datetime
from bs4 import BeautifulSoup
from .base import Query, QuickParse, SourceAdapter
from .. import normalize
from ..config import KST

def state(text):
    script = BeautifulSoup(text, 'lxml').select_one('script#__NEXT_DATA__')
    if script is None: raise ValueError('NEXT_DATA 누락')
    return json.loads(script.string)['props']['pageProps']['__APOLLO_STATE__']

def timestamp(value):
    return datetime.fromtimestamp(float(value) / 1000, KST) if value else None

def groups(item):
    cats, result = set(item.get('category_ids', [])), set()
    if '103002' in cats: result.add('BE')
    if '103006' in cats: result.add('DA')
    return result | normalize.title_job_groups(item.get('title'))

class Linkareer(SourceAdapter):
    platform, label, host = 'linkareer', '링커리어', 'linkareer.com'
    body_format, page_size = 'json', 20

    def queries(self):
        return [Query(self.platform, 'IT', 'MIX', {'category': '100003'})]

    def request(self, q, page_no):
        return {'url': 'https://linkareer.com/list/recruit', 'params': {
            'filterBy_activityTypeID': 5, 'filterBy_categoryIDs': q.params['category'],
            'filterBy_status': 'OPEN', 'orderBy_field': 'RECENT', 'orderBy_direction': 'DESC', 'page': page_no}}

    def quick_parse(self, text, q, page_no):
        graph = state(text)
        cs = [v for v in graph.get('ROOT_QUERY', {}).values()
              if isinstance(v, dict) and v.get('__typename') == 'ActivityConnection']
        if len(cs) != 1: raise ValueError('목록 연결을 유일하게 찾지 못함')
        c, rows = cs[0], []
        for ref in c['nodes']:
            a = graph[ref['__ref']]
            rows.append({'id': str(a['id']), 'title': a['title'], 'company': a.get('organizationName'),
                         'jobTypes': a.get('jobTypes', []), 'recruitType': a.get('recruitType'),
                         'recruitCloseAt': a.get('recruitCloseAt'), 'viewCount': a.get('viewCount'),
                         'category_ids': [x['__ref'].split(':')[1] for x in a.get('categories', [])],
                         'categories': [graph[x['__ref']].get('name', '') for x in a.get('categories', [])],
                         'regions': [x.get('name', '') for x in a.get('regions', [])]})
        total = int(c['totalCount'])
        return QuickParse([a['id'] for a in rows], total, page_no * self.page_size < total,
                          json.dumps({'items': rows, 'totalCount': total}, ensure_ascii=False))

    def parse_items(self, region, q):
        items = []
        for a in json.loads(region).get('items', []):
            kind = 'until_filled' if a.get('recruitType') == 'ASAP' else 'date'
            career = {'NEW': '신입', 'EXPERIENCED': '경력', 'INTERN': '신입', 'IRRELEVANT': '경력무관'}
            items.append({'posting_id': a['id'], 'company_name': a.get('company'), 'title': a['title'],
                          'url': 'https://linkareer.com/activity/' + a['id'], 'job_groups': sorted(groups(a)),
                          'location_raw': ' '.join(a.get('regions', [])),
                          'career_raw': ' · '.join(dict.fromkeys(career.get(x, '') for x in a.get('jobTypes', []))),
                          'employment_type': '인턴' if 'INTERN' in a.get('jobTypes', []) else None,
                          'deadline_at': timestamp(a.get('recruitCloseAt')) if kind == 'date' else None,
                          'deadline_kind': kind, 'tags': a.get('categories', []),
                          'extra': {'view_count': a.get('viewCount'), 'category_ids': a.get('category_ids', [])}})
        return items

    def detail_request(self, pid):
        return {'url': 'https://linkareer.com/activity/' + str(pid)}

    def parse_detail(self, text):
        graph = state(text)
        acts = [a for k, a in graph.items() if k.startswith('Activity:') and 'createdAt' in a]
        if len(acts) != 1: raise ValueError('상세 공고를 유일하게 찾지 못함')
        a = acts[0]
        detail = a.get('detailText') or ''
        if isinstance(detail,dict):
            detail = graph.get(detail.get('__ref'),{}).get('text','')
        d = {'posted_at': timestamp(a.get('createdAt')), 'posted_precision': 'second',
             'skills_text': BeautifulSoup(detail, 'lxml').get_text(' ', strip=True)}
        if a.get('recruitType') == 'ASAP': d['deadline_kind'] = 'until_filled'
        elif a.get('recruitCloseAt'): d.update(deadline_at=timestamp(a['recruitCloseAt']), deadline_kind='date')
        for sc in BeautifulSoup(text, 'lxml').select('script[type="application/ld+json"]'):
            ld = json.loads(sc.string or '{}')
            if isinstance(ld, dict) and ld.get('@type') == 'JobPosting':
                edu = ld.get('educationRequirements')
                if isinstance(edu,dict): edu = edu.get('credentialCategory') or edu.get('educationalLevel')
                if isinstance(edu,(dict,list)): edu = json.dumps(edu,ensure_ascii=False)
                d['education'] = edu
        return d
