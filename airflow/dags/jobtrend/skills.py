"""본문은 메모리에서만 처리하고 기술·역량 키워드만 반환한다."""
import re

ALIASES = {
    'Python': r'\bpython\b|파이썬', 'Java': r'\bjava\b|자바(?!스크립트)',
    'JavaScript': r'\bjavascript\b|자바스크립트', 'TypeScript': r'\btypescript\b|타입스크립트',
    'Spring': r'\bspring(?:\s*boot)?\b|스프링', 'Kotlin': r'\bkotlin\b|코틀린',
    'Go': r'\bgolang\b|\bgo\s*(?:언어|개발|developer)', 'Node.js': r'\bnode\.?js\b',
    'SQL': r'\bsql\b', 'MySQL': r'\bmysql\b', 'PostgreSQL': r'\bpostgres(?:ql)?\b',
    'Oracle': r'\boracle\b|오라클', 'MongoDB': r'\bmongodb\b', 'Redis': r'\bredis\b',
    'Spark': r'\bspark\b|스파크', 'Airflow': r'\bairflow\b', 'Kafka': r'\bkafka\b|카프카',
    'Hadoop': r'\bhadoop\b|하둡', 'Hive': r'\bhive\b', 'Flink': r'\bflink\b',
    'dbt': r'\bdbt\b', 'Snowflake': r'\bsnowflake\b', 'BigQuery': r'\bbigquery\b',
    'AWS': r'\baws\b|아마존\s*웹', 'GCP': r'\bgcp\b|구글\s*클라우드', 'Azure': r'\bazure\b',
    'Docker': r'\bdocker\b|도커', 'Kubernetes': r'\bkubernetes\b|\bk8s\b|쿠버네티스',
    'Tableau': r'\btableau\b|태블로', 'Power BI': r'\bpower\s*bi\b',
    'pandas': r'\bpandas\b|판다스', 'NumPy': r'\bnumpy\b',
    'PyTorch': r'\bpytorch\b|파이토치', 'TensorFlow': r'\btensorflow\b|텐서플로',
    'scikit-learn': r'\bscikit[ -]learn\b|\bsklearn\b', 'R': r'(?<![\w])R(?![\w&])',
    'Django': r'\bdjango\b|장고', 'FastAPI': r'\bfastapi\b', 'Flask': r'\bflask\b',
    'Linux': r'\blinux\b|리눅스', 'Git': r'\bgit(?:hub|lab)?\b',
    'Elasticsearch': r'\belasticsearch\b|엘라스틱서치', 'C++': r'(?<!\w)c\+\+(?!\w)',
    'C#': r'(?<!\w)c#(?!\w)', 'Scala': r'\bscala\b', 'React': r'\breact\b|리액트',
}
COMPETENCIES = {
    '협업': r'협업|\bcollaboration\b|팀워크', '커뮤니케이션': r'커뮤니케이션|의사소통|소통|\bcommunication\b',
    '문제해결': r'문제\s*해결|problem[ -]solving', '대용량 처리': r'대용량|대규모\s*(?:데이터|트래픽)',
    '데이터 모델링': r'데이터\s*모델링|data\s*modeling', 'A/B 테스트': r'a\s*/\s*b\s*(?:테스트|test)',
    '통계': r'통계|\bstatistic', '머신러닝': r'머신\s*러닝|machine\s*learning|\bML\b',
    '딥러닝': r'딥\s*러닝|deep\s*learning', '데이터 시각화': r'시각화|visualization',
    'ETL': r'\betl\b|\belt\b', '데이터 파이프라인': r'데이터\s*파이프라인|data\s*pipeline',
    'API 설계': r'api\s*(?:설계|개발)|restful', '분산 시스템': r'분산\s*(?:시스템|처리)|distributed',
    '실험 설계': r'실험\s*설계|experiment\s*design', '비즈니스 이해': r'비즈니스\s*(?:이해|분석)',
}
_PATTERNS = {k: re.compile(v, 0 if k == 'R' else re.I) for k, v in ALIASES.items()}
_COMP = {k: re.compile(v, re.I) for k, v in COMPETENCIES.items()}

def extract_skills(text):
    return {k: 'technology' for k, p in _PATTERNS.items() if p.search(text or '')}

def extract_competencies(text):
    return {k for k, p in _COMP.items() if p.search(text or '')}

def extract_all(tags, title, detail=None):
    found = {}
    for field, text in [('tag', ' '.join(tags or [])), ('title', title or ''), ('detail', detail or '')]:
        for k, group in {**extract_skills(text), **{k: 'competency' for k in extract_competencies(text)}}.items():
            found.setdefault(k, (k, group, field))
    return list(found.values())
