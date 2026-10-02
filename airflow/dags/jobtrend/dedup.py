"""플랫폼 간 같은 공고 판정(엔티티 해상도) — job_canonical·posting_canonical을 한 트랜잭션에서 통째로 다시 만듭니다.

규칙(17번 엔티티 해상도 실습의 회사명 정규화를 확장):
1. 같은 company_key 안에서, 서로 다른 플랫폼의 공고끼리만 후보 쌍을 만듭니다.
2. 제목 유사도 ≥ τ, 지역·직무가 호환되고 마감이 모두 NULL(종류 일치) 또는 차이 ≤ 3일인 후보를 묶습니다.
3. union-find로 묶은 덩어리 하나가 canonical 1건입니다. 짝이 없는 공고는 singleton입니다.
결과는 추정이므로 EDA에서는 τ별 쌍 수와 표본 점검으로 범위를 함께 보고합니다.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import re

from . import store

PAIR_SQL = """
SELECT a.platform, a.posting_id, b.platform, b.posting_id, similarity(a.title_key, b.title_key) AS sim, a.title, b.title
FROM job_posting a
JOIN job_posting b ON a.company_key = b.company_key AND a.platform < b.platform
WHERE similarity(a.title_key, b.title_key) >= %(tau)s
  AND length(a.title_key) >= 4 AND length(b.title_key) >= 4
  AND a.company_name <> '(미상)' AND b.company_name <> '(미상)'
  AND (a.sido IS NULL OR b.sido IS NULL OR a.sido=b.sido
       OR a.sido IN ('전국','재택') OR b.sido IN ('전국','재택'))
  AND ((a.deadline_at IS NULL AND b.deadline_at IS NULL AND a.deadline_kind=b.deadline_kind)
       OR (a.deadline_at IS NOT NULL AND b.deadline_at IS NOT NULL
           AND abs(extract(epoch FROM a.deadline_at - b.deadline_at)) <= %(days)s * 86400))
"""


class _UnionFind:
    def __init__(self):
        self.parent: dict = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


def title_conflict(a, b):
    patterns = {'front':r'프론트[ -]?엔드|front[ -]?end', 'back':r'백엔드|back[ -]?end',
                'ml':r'\b(?:ml|llm)\b|머신러닝', 'de':r'데이터\s*엔지니어|data\s*engineer',
                'da':r'데이터\s*분석|data\s*analyst'}
    sa={k for k,v in patterns.items() if re.search(v,a,re.I)}
    sb={k for k,v in patterns.items() if re.search(v,b,re.I)}
    if sa and sb and sa.isdisjoint(sb):return True
    lead=r'\blead(?:er)?\b|팀장|리드'
    if bool(re.search(lead,a,re.I)) != bool(re.search(lead,b,re.I)):
        if re.search(r'engineer|엔지니어|개발자',a+' '+b,re.I):return True
    return False

def candidate_pairs(conn, tau: float, days: int = 3) -> list[tuple]:
    return [r[:5] for r in conn.execute(PAIR_SQL, {"tau": tau, "days": days}).fetchall()
            if not title_conflict(r[5],r[6])]


def resolve_duplicates(dsn: str | None = None, tau: float = 0.55, days: int = 3, connection=None) -> dict:
    from contextlib import nullcontext
    with (nullcontext(connection) if connection is not None else store.connect(dsn)) as conn:
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(hashtext('jobtrend:dedup'))")
            postings = conn.execute("SELECT platform, posting_id, company_key, company_name, title, title_key, "
                                    "first_seen_at FROM job_posting ORDER BY platform,posting_id").fetchall()
            pairs = candidate_pairs(conn, tau, days)
            uf, best = _UnionFind(), defaultdict(float)
            for p in postings:
                uf.find((p[0], p[1]))
            for pa, ia, pb, ib, sim in pairs:
                uf.union((pa, ia), (pb, ib))
                best[(pa, ia)] = max(best[(pa, ia)], float(sim))
                best[(pb, ib)] = max(best[(pb, ib)], float(sim))
            members = defaultdict(list)
            info = {(p[0], p[1]): p for p in postings}
            for key in info:
                members[uf.find(key)].append(key)

            conn.execute("TRUNCATE posting_canonical, job_canonical RESTART IDENTITY")
            canon_rows, map_rows = [], []
            for root, keys in sorted(members.items()):
                rows = [info[k] for k in keys]
                rep = min(rows, key=lambda r: (r[6], r[0], r[1]))              # 가장 먼저 관측된 공고를 대표로
                names = Counter(r[3] for r in rows)
                canon_rows.append((rep[2], names.most_common(1)[0][0], rep[4], rep[5], len(rows),
                                   len({r[0] for r in rows}), keys))
            with conn.cursor() as cur:
                for company_key, company_name, title, tkey, n_post, n_src, keys in canon_rows:
                    cid = cur.execute("INSERT INTO job_canonical (company_key, company_name, title, title_key, n_postings, "
                                      "n_sources) VALUES (%s,%s,%s,%s,%s,%s) RETURNING canonical_id",
                                      (company_key, company_name, title, tkey, n_post, n_src)).fetchone()[0]
                    for k in keys:
                        sim = best.get(k)
                        method = "singleton" if n_post == 1 or sim is None else ("exact" if sim >= 0.999 else "trgm")
                        map_rows.append((k[0], k[1], cid, method, round(sim, 3) if sim is not None else None))
                cur.executemany("INSERT INTO posting_canonical (platform, posting_id, canonical_id, match_method, similarity) "
                                "VALUES (%s,%s,%s,%s,%s)", map_rows)
    multi = [c for c in canon_rows if c[5] > 1]
    return {"postings": len(postings), "pairs": len(pairs), "canonicals": len(canon_rows),
            "cross_posted_canonicals": len(multi), "tau": tau}


def threshold_table(dsn: str | None = None, taus=(0.4, 0.5, 0.55, 0.6, 0.7, 0.8, 1.0), days: int = 3) -> list[dict]:
    """τ별 후보 쌍 수 — 노트북의 τ 보정표(쌍이 급감하는 지점과 표본 점검으로 τ를 정함)."""
    with store.connect(dsn) as conn:
        return [{"tau": t, "pairs": len(candidate_pairs(conn, t, days))} for t in taus]
