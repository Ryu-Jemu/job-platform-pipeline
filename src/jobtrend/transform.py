"""정제 — 원본(raw_listing_page)을 어댑터로 파싱·정규화해 정제·관측 테이블에 적재합니다.

- job_posting   : upsert(first_seen=LEAST, last_seen=GREATEST). 게시 시각은 첫 관측 값을 고정하고 더 정밀한 값만 덮어씀.
                  속성은 더 늦은 관측이면서 내용이 바뀐 경우에만 갱신 → 적재 순서와 상관없이 결과가 같음.
- posting_role  : 공고↔직무(요청한 코드 또는 제목 분류). title_match로 직무 순도를 기록.
- posting_snapshot·listing_metric : run 단위 교체(DELETE 후 INSERT) → 파서를 고친 뒤 다시 정제(replay)하면 반영.
- posting_skill : 태그·제목(·상세)에서 기술·역량 키워드.
"""
from __future__ import annotations

import json
import math
from collections import OrderedDict
from datetime import datetime

from psycopg.types.json import Jsonb

from . import normalize, store
from .sources import REGISTRY

from . import skills as _skills

_ATTR_COLS = ["company_name", "company_key", "title", "title_key", "url", "location_raw", "sido", "career_raw",
              "career_type", "career_min_yr", "career_max_yr", "education", "employment_type", "apply_start_date",
              "deadline_at", "deadline_raw", "deadline_kind", "tags_raw", "extra", "attrs_sha", "last_raw_id"]
_POSTED_COLS = ["posted_at", "posted_lo", "posted_hi", "posted_precision"]
_ALL_COLS = ["platform", "posting_id"] + _ATTR_COLS + _POSTED_COLS + ["first_seen_at", "last_seen_at"]

_RANK = "array_position(ARRAY['second','minute','hour','day','none']::text[], {})"
_NEWER = "(EXCLUDED.last_seen_at >= job_posting.last_seen_at AND EXCLUDED.attrs_sha IS DISTINCT FROM job_posting.attrs_sha)"
_FINER = (f"(job_posting.posted_at IS NULL OR "
          f"{_RANK.format('EXCLUDED.posted_precision')} < {_RANK.format('job_posting.posted_precision')})")

UPSERT_POSTING = (
    f"INSERT INTO job_posting ({', '.join(_ALL_COLS)}) VALUES ({', '.join('%(' + c + ')s' for c in _ALL_COLS)}) "
    "ON CONFLICT (platform, posting_id) DO UPDATE SET "
    + ", ".join(f"{c} = CASE WHEN {_NEWER} THEN EXCLUDED.{c} ELSE job_posting.{c} END" for c in _ATTR_COLS) + ", "
    + ", ".join(f"{c} = CASE WHEN {_FINER} AND EXCLUDED.posted_at IS NOT NULL THEN EXCLUDED.{c} ELSE job_posting.{c} END"
                for c in _POSTED_COLS) + ", "
    "first_seen_at = LEAST(job_posting.first_seen_at, EXCLUDED.first_seen_at), "
    "last_seen_at = GREATEST(job_posting.last_seen_at, EXCLUDED.last_seen_at)"
)
UPSERT_ROLE = ("INSERT INTO posting_role (platform, posting_id, job_group, assign_method, title_match, first_seen_at) "
               "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (platform, posting_id, job_group) DO UPDATE SET "
               "first_seen_at = LEAST(posting_role.first_seen_at, EXCLUDED.first_seen_at)")
INSERT_SNAPSHOT = ("INSERT INTO posting_snapshot (run_key, platform, job_group, posting_id, query_key, page_no, "
                   "rank_in_list, raw_id, observed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING")
UPSERT_SKILL = ("INSERT INTO posting_skill (platform, posting_id, skill, skill_group, source_field) "
                "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (platform, posting_id, skill) DO NOTHING")


def _queries(platform: str) -> dict:
    return {q.query_key: q for q in REGISTRY[platform].queries()}


def transform_run(conn, run_key: str) -> dict:
    """한 run의 원본을 정제합니다(호출자가 트랜잭션·잠금을 잡음)."""
    raws = conn.execute(
        "SELECT raw_id, platform, query_key, page_no, scan_kind, fetched_at, outcome, reported_total, has_next, "
        "item_ids, body FROM raw_listing_page WHERE run_key=%s ORDER BY platform, query_key, page_no", (run_key,)
    ).fetchall()
    conn.execute("DELETE FROM posting_snapshot WHERE run_key=%s", (run_key,))
    conn.execute("DELETE FROM listing_metric WHERE run_key=%s", (run_key,))

    groups: OrderedDict = OrderedDict()
    for r in raws:
        groups.setdefault((r[1], r[2]), []).append(r)

    totals = {"postings": 0, "snapshots": 0, "skipped_unclassified": 0, "parse_errors": 0}
    for (platform, query_key), rows in groups.items():
        if platform not in REGISTRY:
            continue
        ad = REGISTRY[platform]
        q = _queries(platform).get(query_key)
        if q is None:
            continue
        known = {r[0] for r in conn.execute(
            "SELECT DISTINCT unnest(p.item_ids) FROM raw_listing_page p JOIN crawl_run r USING(run_key) "
            "WHERE p.platform=%s AND p.query_key=%s AND p.outcome='ok' AND "
            "r.slot_ts < (SELECT slot_ts FROM crawl_run WHERE run_key=%s)", (platform,query_key,run_key))}
        details = {r[0]: json.loads(r[1]) for r in conn.execute(
            "SELECT posting_id, body FROM raw_posting_detail WHERE platform=%s AND outcome='ok' AND body IS NOT NULL",
            (platform,))}
        posting_rows, role_rows, snap_rows, skill_rows = [], [], [], []
        seen_ids, parsed, new_on_page1 = set(), 0, None
        pages_ok = pages_failed = 0
        parse_failed = False
        page1 = rows[0]
        for raw_id, _p, _q, page_no, scan_kind, fetched_at, outcome, reported_total, has_next, item_ids, body in rows:
            if outcome not in ("ok", "empty"):
                pages_failed += 1
                continue
            pages_ok += 1
            try:
                items = ad.parse_items(body or "", q) if body else []
            except Exception:  # noqa: BLE001 — 원본은 남아 있으므로 파서를 고친 뒤 replay
                totals["parse_errors"] += 1
                parse_failed = True
                continue
            for rank, it in enumerate(items, start=1):
                job_groups = it.get("job_groups")
                job_groups = set(job_groups) if job_groups is not None else {q.job_group}
                job_groups &= {"BE", "DA", "DE"}
                if not job_groups:
                    totals["skipped_unclassified"] += 1
                    continue
                d = details.get(str(it["posting_id"]))
                if d:                                   # 상세의 정확한 값으로 보강(게시 시각·학력·마감)
                    it = {**it, **{k: d[k] for k in ("posted_at", "posted_precision", "deadline_at", "deadline_kind")
                                   if k in d}}
                    if d.get("posted_at"):
                        it["posted_at"] = datetime.fromisoformat(d["posted_at"])
                    if d.get("deadline_at"):
                        it["deadline_at"] = datetime.fromisoformat(d["deadline_at"])
                    education = d.get('education')
                    if isinstance(education,dict):
                        education = education.get('credentialCategory') or education.get('educationalLevel') or None
                    if isinstance(education,(dict,list)):
                        education = json.dumps(education,ensure_ascii=False)
                    it["education"] = it.get("education") or education
                rec = normalize.build_record(platform, it, fetched_at)
                rec.update(first_seen_at=fetched_at, last_seen_at=fetched_at, last_raw_id=raw_id,
                           extra=Jsonb(rec["extra"]))
                posting_rows.append(rec)
                parsed += 1
                seen_ids.add(rec["posting_id"])
                title_groups = normalize.title_job_groups(rec["title"])
                method = "keyword" if q.job_group == "MIX" else "code"
                for g in sorted(job_groups):
                    role_rows.append((platform, rec["posting_id"], g, method, g in title_groups, fetched_at))
                    snap_rows.append((run_key, platform, g, rec["posting_id"], query_key, page_no, rank, raw_id,
                                      fetched_at))
                if _skills is not None:
                    for skill, group, field in _skills.extract_all(rec["tags_raw"], rec["title"], None):
                        skill_rows.append((platform, rec["posting_id"], skill, group, field))
                for skill, group in (d or {}).get("skills", []):
                    skill_rows.append((platform, rec["posting_id"], skill, group, "detail"))
                for comp in (d or {}).get("competencies", []):
                    skill_rows.append((platform, rec["posting_id"], comp, "competency", "detail"))
            if page_no == 1:
                new_on_page1 = sum(1 for i in (item_ids or []) if i not in known)

        with conn.cursor() as cur:
            if posting_rows:
                cur.executemany(UPSERT_POSTING, posting_rows)
            if role_rows:
                cur.executemany(UPSERT_ROLE, role_rows)
            if snap_rows:
                cur.executemany(INSERT_SNAPSHOT, snap_rows)
            if skill_rows:
                cur.executemany(UPSERT_SKILL, skill_rows)

        last = rows[-1]
        reported = page1[7]
        pages_needed = math.ceil(reported / ad.page_size) if reported else None
        scan_kind = page1[4]
        complete = (scan_kind == "full" and pages_failed == 0 and not parse_failed and last[8] is False and pages_ok == last[3])
        status = "ok" if pages_failed == 0 and not parse_failed else ("partial" if pages_ok else "failed")
        conn.execute(
            "INSERT INTO listing_metric (run_key, platform, query_key, job_group, scan_kind, observed_at, reported_total, "
            "pages_ok, pages_failed, pages_needed, is_complete, items_parsed, items_unique, new_on_page1, status) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (run_key, platform, query_key, q.job_group, scan_kind, page1[5], reported, pages_ok, pages_failed,
             pages_needed, complete, parsed, len(seen_ids), new_on_page1, status))
        totals["postings"] += len(posting_rows)
        totals["snapshots"] += len(snap_rows)
    return totals


def transform_load(dsn: str, run_key: str) -> dict:
    """DAG transform_load의 진입점. DAG와 노트북이 동시에 정제하지 않도록 advisory lock을 잡습니다."""
    with store.connect(dsn) as conn:
        with conn.transaction():
            conn.execute("SELECT pg_advisory_xact_lock(hashtext('jobtrend:transform'))")
            totals = transform_run(conn, run_key)
    return {"run_key": run_key, **totals}


def replay(dsn: str, run_kind: str = "scheduled") -> list[dict]:
    """범위 안 run을 slot 순서대로 다시 정제합니다(파서·정규화를 고친 뒤 원본에서 재생성)."""
    with store.connect(dsn) as conn:
        keys = [r[0] for r in conn.execute("SELECT run_key FROM crawl_run WHERE run_kind=%s ORDER BY slot_ts",
                                           (run_kind,))]
    return [transform_load(dsn, k) for k in keys]


__all__ = ["transform_load", "transform_run", "replay", "UPSERT_POSTING", "json"]
