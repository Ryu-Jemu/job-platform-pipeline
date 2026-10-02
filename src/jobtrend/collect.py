"""수집 실행 — 계획(light/full·포화 이어받기)과 동시 수집(ThreadPoolExecutor).

동시성 단위는 (원천, 질의) 작업입니다. 같은 호스트 요청은 HostGate가 간격을 지키며 줄 세우고,
서로 다른 호스트를 동시에 기다리는 데서 수집 시간이 줄어듭니다(I/O 바운드 → 스레드).
"""
from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime

import requests

from . import config, store
from .http import HostGate, fetch_with_backoff
from .sources import REGISTRY, QuickParse

from . import skills as _skills


@dataclass
class Task:
    adapter: object
    query: object
    scan_kind: str              # 'light' | 'full'
    reason: str                 # baseline | hourly | always_full | light
    known_ids: frozenset = frozenset()


def enabled_adapters(sources=None) -> list:
    names = sources or config.ENABLED_SOURCES
    return [REGISTRY[n] for n in names if n in REGISTRY and REGISTRY[n].available()]


def plan_run(conn, run_key: str, slot_ts: datetime, adapters, force_scan: str | None = None) -> list[Task]:
    """원천·질의마다 이번 실행의 스캔 종류를 정합니다.

    - full: 원천별 첫 완료 전수가 아직 없음(기준선) / 매 정각 / 페이지가 적어 매번 전수인 원천
    - light: 최신순 1페이지(+ 1페이지가 전부 처음 보는 공고면 아는 공고가 나올 때까지 이어받기)
    """
    tasks = []
    for ad in adapters:
        for q in ad.queries():
            if force_scan:
                kind, reason = force_scan, f"forced_{force_scan}"
            elif ad.always_full:
                kind, reason = "full", "always_full"
            elif not store.has_complete_full(conn, ad.platform, q.query_key, run_key):
                kind, reason = "full", "baseline"
            elif slot_ts.astimezone(config.KST).minute == 0:
                kind, reason = "full", "hourly"
            else:
                kind, reason = "light", "light"
            known = store.known_ids(conn, ad.platform, q.query_key, run_key) if kind == "light" else frozenset()
            tasks.append(Task(ad, q, kind, reason, known))
    return tasks


def run_task(task: Task, session, gate: HostGate, deadline: float, sink, skip_pages=None) -> dict:
    """작업 하나: 페이지를 차례로 받아 sink(원본 기록 함수)에 넘깁니다. 실패는 예외 대신 결과로 남깁니다."""
    ad, q = task.adapter, task.query
    page, outcomes, n_items = 1, [], 0
    while True:
        if skip_pages and page in skip_pages:
            qp = skip_pages[page]
            outcome = "ok"                 # 마지막/포화 신호도 재사용해 완료 페이지를 다시 요청하지 않는다
        else:
            req = ad.request(q, page)
            fr = fetch_with_backoff(session, req, gate, deadline=deadline)
            outcome, qp = fr.outcome, None
            if fr.outcome == "ok":
                try:
                    qp = ad.quick_parse(fr.text or "", q, page)
                    if not qp.item_ids:
                        outcome = "empty" if not qp.reported_total else "parse_error"
                except Exception as exc:
                    outcome, fr.error = "parse_error", f"{type(exc).__name__}: {exc}"[:300]
            sink(task, page, req, fr, outcome, qp)
        outcomes.append(outcome)
        if outcome != "ok":
            break
        n_items += len(qp.item_ids)
        if not qp.has_next:
            break
        if task.scan_kind == "full":
            if page >= config.FULL_PAGE_CAP:
                break
        else:
            saturated = not any(i in task.known_ids for i in qp.item_ids)
            if not saturated or page >= config.FULL_PAGE_CAP:
                break
        if time.monotonic() > deadline:
            break
        page += 1
    return {"platform": ad.platform, "query_key": q.query_key, "scan_kind": task.scan_kind,
            "pages": len(outcomes), "outcomes": outcomes, "items": n_items}


def execute(tasks: list[Task], sink, *, max_workers: int | None = None, deadline_s: float = config.RUN_DEADLINE_S,
            gates: dict | None = None, skip=None) -> tuple[list[dict], dict]:
    """작업들을 ThreadPoolExecutor로 동시에 돌립니다. max_workers=1이면 같은 함수로 순차 기준선이 됩니다."""
    gates = gates or {}
    for t in tasks:
        gates.setdefault(t.adapter.host, HostGate(t.adapter.min_interval_s))
    deadline = time.monotonic() + deadline_s

    def _one(t: Task) -> dict:
        try:
            with requests.Session() as s:
                s.headers["User-Agent"] = config.USER_AGENT
                skip_pages = skip(t) if skip else None
                return run_task(t, s, gates[t.adapter.host], deadline, sink, skip_pages)
        except Exception as exc:  # noqa: BLE001 — 한 원천의 실패가 다른 원천을 멈추지 않게
            return {"platform": t.adapter.platform, "query_key": t.query.query_key, "error": repr(exc)[:300],
                    "outcomes": ["task_error"], "pages": 0, "items": 0}

    workers = max_workers or max(1, min(8, len(tasks)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(_one, tasks))           # 입력(작업) 순서대로 결과를 돌려줌
    return results, gates


def collect_run(dsn: str, run_key: str, *, sources=None, force_scan: str | None = None,
                max_workers: int | None = None, deadline_s: float = config.RUN_DEADLINE_S) -> dict:
    """DAG extract_raw의 진입점: 계획 → 동시 수집 → 원본 기록. 모든 원천에서 ok가 0이면 예외(Airflow 재시도)."""
    adapters = enabled_adapters(sources)
    with store.connect(dsn) as conn:
        slot_ts = conn.execute("SELECT slot_ts FROM crawl_run WHERE run_key=%s", (run_key,)).fetchone()[0]
        tasks = plan_run(conn, run_key, slot_ts, adapters, force_scan)
        plan = {f"{t.adapter.platform}:{t.query.query_key}": {"scan": t.scan_kind, "reason": t.reason,
                                                              "known": len(t.known_ids)} for t in tasks}
        conn.execute("UPDATE crawl_run SET plan=%s WHERE run_key=%s", (json.dumps(plan), run_key))
        states = {r[0]: r[1:] for r in conn.execute(
            "SELECT platform,robots_allowed,blocked_until > now() FROM source_state")}
    gates = {}
    for ad in adapters:
        gate = gates.setdefault(ad.host, HostGate(ad.min_interval_s))
        allowed, cooling = states.get(ad.platform, (False, False))
        if not allowed or cooling:
            gate.open_circuit()

    def make_sink():
        conns: dict = {}

        def sink(task, page, req, fr, outcome, qp):
            key = id(task)
            if key not in conns:
                conns[key] = store.connect(dsn)
            store.insert_raw(conns[key], run_key, task.adapter, task.query, page, task.scan_kind, req, fr, outcome, qp)

        return sink, conns

    sink, conns = make_sink()

    def skip(t: Task) -> dict:
        with store.connect(dsn) as c:
            rows = c.execute("SELECT page_no,item_ids,reported_total,has_next FROM raw_listing_page "
                             "WHERE run_key=%s AND platform=%s AND query_key=%s AND outcome='ok'",
                             (run_key,t.adapter.platform,t.query.query_key)).fetchall()
            return {n: QuickParse(ids,total,nxt,'') for n,ids,total,nxt in rows}

    t0 = time.monotonic()
    try:
        results, gates = execute(tasks, sink, max_workers=max_workers, deadline_s=deadline_s, skip=skip, gates=gates)
    finally:
        for c in conns.values():
            c.close()
    details = collect_details(dsn, run_key, adapters, gates, deadline_s=max(0.0, deadline_s - (time.monotonic() - t0)))
    with store.connect(dsn) as conn:
        for ad in adapters:
            blocked = conn.execute("SELECT EXISTS(SELECT 1 FROM raw_listing_page WHERE run_key=%s AND platform=%s "
                                   "AND outcome IN ('blocked','rate_limited')) OR EXISTS(SELECT 1 FROM "
                                   "raw_posting_detail WHERE run_key=%s AND platform=%s AND outcome='blocked')",
                                   (run_key,ad.platform,run_key,ad.platform)).fetchone()[0]
            if blocked:
                conn.execute("UPDATE source_state SET blocked_until=now()+interval '30 minutes' WHERE platform=%s",
                             (ad.platform,))
    ok = sum(o == "ok" for r in results for o in r["outcomes"])
    summary = {"run_key": run_key, "elapsed_s": round(time.monotonic() - t0, 1), "ok_pages": ok,
               "tasks": [{k: r[k] for k in ("platform", "query_key", "pages", "items") if k in r}
                         | {"scan": r.get("scan_kind"), "last": r["outcomes"][-1] if r["outcomes"] else None}
                         for r in results], "details": details}
    if ok == 0:
        raise RuntimeError(f"모든 원천에서 ok 0건 — {summary}")
    return summary


def _detail_candidates(conn, run_key: str, adapter, limit: int) -> list[str]:
    """이번 run 목록에 나온 공고 중 상세를 아직 받지 않은 것(최신 페이지 순). 혼합 목록은 3직무로 분류된 공고만."""
    rows = conn.execute("SELECT DISTINCT ON(query_key,page_no) query_key,body FROM raw_listing_page "
                        "WHERE platform=%s AND outcome='ok' ORDER BY query_key,page_no,fetched_at DESC",
                        (adapter.platform,)).fetchall()
    done = {r[0] for r in conn.execute("SELECT posting_id FROM raw_posting_detail WHERE platform=%s AND outcome='ok'",
                                       (adapter.platform,))}
    queries = {q.query_key: q for q in adapter.queries()}
    picked: list[str] = []
    for query_key, body in rows:
        q = queries.get(query_key)
        if q is None or not body:
            continue
        for it in adapter.parse_items(body, q):
            pid = str(it["posting_id"])
            groups = it.get("job_groups")
            if q.job_group == "MIX" and not groups:
                continue
            if pid not in done and pid not in picked:
                picked.append(pid)
            if len(picked) >= limit:
                return picked
    return picked


def collect_details(dsn: str, run_key: str, adapters, gates: dict, deadline_s: float) -> dict:
    """신규 공고 상세를 호스트별 상한 안에서 받아, 화이트리스트 필드와 추출한 키워드만 raw_posting_detail에 남깁니다."""
    out: dict = {}
    deadline = time.monotonic() + deadline_s
    for ad in adapters:
        if not hasattr(ad, "detail_request") or deadline_s <= 0:
            continue
        with store.connect(dsn) as conn:
            used = conn.execute('SELECT count(*) FROM raw_posting_detail WHERE platform=%s AND run_key=%s',
                                (ad.platform,run_key)).fetchone()[0]
            remaining = max(0, config.DETAIL_PER_RUN-used)
            ids = _detail_candidates(conn, run_key, ad, remaining) if remaining else []
            gate = gates.setdefault(ad.host, HostGate(ad.min_interval_s))
            n_ok = 0
            with requests.Session() as s:
                s.headers["User-Agent"] = config.USER_AGENT
                for pid in ids:
                    if time.monotonic() > deadline or gate.circuit_open:
                        break
                    req = ad.detail_request(pid)
                    fr = fetch_with_backoff(s, req, gate, deadline=deadline)
                    body, outcome, err = None, fr.outcome, fr.error
                    if fr.outcome == "ok":
                        try:
                            d = ad.parse_detail(fr.text or "")
                            text = d.pop("skills_text", None) or ""
                            if _skills is not None and text:
                                d["skills"] = [[k, g] for k, g in _skills.extract_skills(text).items()]
                                d["competencies"] = sorted(_skills.extract_competencies(text))
                            body = json.dumps(d, ensure_ascii=False, default=str)
                            n_ok += 1
                        except Exception as exc:  # noqa: BLE001
                            outcome, err = "parse_error", f"{type(exc).__name__}: {exc}"[:300]
                    conn.execute(
                        "INSERT INTO raw_posting_detail (platform, posting_id, run_key, request_url, outcome, http_status, "
                        "body_format, body, body_sha256, error) VALUES (%s,%s,%s,%s,%s,%s,'json',%s,%s,%s) "
                        "ON CONFLICT (platform, posting_id) DO UPDATE SET run_key=EXCLUDED.run_key, outcome=EXCLUDED.outcome, "
                        "http_status=EXCLUDED.http_status, fetched_at=now(), body=EXCLUDED.body, "
                        "body_sha256=EXCLUDED.body_sha256, error=EXCLUDED.error WHERE raw_posting_detail.outcome <> 'ok'",
                        (ad.platform, pid, run_key, ad.redact(fr.url or req["url"]), outcome, fr.status, body,
                         None if body is None else __import__("hashlib").sha256(body.encode()).hexdigest(), err))
                    conn.commit()
        out[ad.platform] = {"candidates": len(ids), "ok": n_ok}
    return out
