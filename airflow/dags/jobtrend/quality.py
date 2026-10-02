"""품질 점검 — DAG의 check_quality와 노트북 EDA가 같은 규칙을 씁니다.

임계값 근거: 17번 overall_verdict(완전성 5%·유효성 3%), 14번 '직전 대비 30% 변화'.
critical은 시스템 문제(전 원천 실패, 개인정보 패턴)에만 쓰고, 원천 하나의 문제는 warning으로 둡니다
→ 멀쩡한 원천의 데이터까지 분석에서 빠지지 않게.
"""
from __future__ import annotations

import json
import re

from . import store

_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_PHONE = re.compile(r"(?<!\d)0\d{1,2}[-.\s]?\d{3,4}[-.\s]?\d{4}(?!\d)")
_ORDER = {"ok": 0, "warning": 1, "critical": 2}


def _check(name, dimension, metric, threshold, severity, platform="*", **detail):
    return {"check_name": name, "platform": platform, "dimension": dimension, "metric": metric,
            "threshold": threshold, "severity": severity, "detail": detail}


def run_checks(conn, run_key: str) -> list[dict]:
    checks = []
    # coverage — 계획한 요청 단위 중 ok 비율(원천별)
    for platform, n, ok in conn.execute(
            "SELECT platform, COUNT(*), COUNT(*) FILTER (WHERE outcome IN ('ok','empty')) FROM raw_listing_page "
            "WHERE run_key=%s GROUP BY platform", (run_key,)):
        ratio = ok / n if n else 0.0
        checks.append(_check("coverage_ok_ratio", "coverage", round(ratio, 3), 0.9,
                             "ok" if ratio >= 0.9 else "warning", platform, units=n, ok=ok))
    total_ok = sum(c["detail"]["ok"] for c in checks)
    checks.append(_check("coverage_any_ok", "coverage", total_ok, 1, "ok" if total_ok else "critical"))
    # format — 200인데 행이 0(총건수는 >0): 응답 구조 변경 신호
    for platform, n in conn.execute("SELECT platform, COUNT(*) FROM raw_listing_page WHERE run_key=%s AND "
                                    "outcome='parse_error' GROUP BY platform", (run_key,)):
        checks.append(_check("format_parse_error", "format", n, 0, "warning", platform))
    # privacy — 저장된 원본에 이메일·전화 패턴이 남았는가(scrub 실패)
    n_priv = 0
    for (body,) in conn.execute("SELECT body FROM raw_listing_page WHERE run_key=%s AND body IS NOT NULL UNION ALL "
                                "SELECT body FROM raw_posting_detail WHERE run_key=%s AND body IS NOT NULL", (run_key,run_key)):
        n_priv += len(_EMAIL.findall(body)) + len(_PHONE.findall(body))
    checks.append(_check("privacy_patterns", "privacy", n_priv, 0, "ok" if n_priv == 0 else "critical"))
    # change — 사이트 총건수가 직전 run 대비 ±30% 이상 변했는가
    for platform, qk, cur, prev in conn.execute(
            """
            SELECT m.platform, m.query_key, m.reported_total,
                   (SELECT p.reported_total FROM listing_metric p JOIN crawl_run r ON r.run_key = p.run_key
                    WHERE p.platform = m.platform AND p.query_key = m.query_key AND p.run_key <> m.run_key
                      AND r.slot_ts < (SELECT slot_ts FROM crawl_run WHERE run_key = m.run_key)
                      AND p.reported_total IS NOT NULL ORDER BY r.slot_ts DESC LIMIT 1)
            FROM listing_metric m WHERE m.run_key=%s AND m.reported_total IS NOT NULL""", (run_key,)):
        if prev:
            change = abs(cur - prev) / prev
            checks.append(_check("change_reported_total", "change", round(change, 3), 0.3,
                                 "warning" if change > 0.3 else "ok", f"{platform}:{qk}", prev=prev, cur=cur))
    # completeness — 이번 run에서 관측한 공고의 필수 값 결측
    n_missing = conn.execute(
        """SELECT COUNT(*) FROM job_posting p WHERE EXISTS (SELECT 1 FROM posting_snapshot s WHERE s.run_key=%s
           AND s.platform=p.platform AND s.posting_id=p.posting_id)
           AND (p.company_name IN ('', '(미상)') OR p.title = '' OR p.url = '')""", (run_key,)).fetchone()[0]
    checks.append(_check("completeness_required", "completeness", n_missing, 0, "ok" if n_missing == 0 else "warning"))
    # timeliness — 슬롯 대비 시작 지연
    lag = conn.execute("SELECT EXTRACT(EPOCH FROM started_at - slot_ts) / 60 FROM crawl_run WHERE run_key=%s",
                       (run_key,)).fetchone()
    if lag and lag[0] is not None:
        checks.append(_check("timeliness_lag_min", "timeliness", round(float(lag[0]), 2), 9,
                             "ok" if lag[0] <= 9 else "warning"))
    # integrity — 원본에서 읽은 ID 수와 관측(스냅샷) 행 수(직무 중복 제외)
    raw_ids, snap = conn.execute(
        """SELECT (SELECT COUNT(DISTINCT (platform,job_group,pid)) FROM raw_listing_page,
                    LATERAL unnest(item_ids) AS pid WHERE run_key=%s AND outcome='ok' AND job_group <> 'MIX'),
                  (SELECT COUNT(DISTINCT (s.platform,s.job_group,s.posting_id)) FROM posting_snapshot s
                    JOIN raw_listing_page r ON r.run_key=s.run_key AND r.platform=s.platform AND r.job_group=s.job_group
                      AND s.posting_id=ANY(r.item_ids)
                    WHERE s.run_key=%s AND r.job_group <> 'MIX')""", (run_key, run_key)).fetchone()
    checks.append(_check("integrity_raw_vs_snapshot", "integrity", int(snap) - int(raw_ids), 0,
                         "ok" if int(snap) == int(raw_ids) else "warning", raw_ids=int(raw_ids), snapshot=int(snap)))
    metrics = conn.execute("SELECT COUNT(*),COUNT(*) FILTER(WHERE status <> 'failed') FROM listing_metric WHERE run_key=%s",
                           (run_key,)).fetchone()
    checks.append(_check('transform_available','integrity',int(metrics[1]),1,'ok' if metrics[1] else 'critical'))
    plan = conn.execute('SELECT plan FROM crawl_run WHERE run_key=%s',(run_key,)).fetchone()[0]
    actual = {r[0] for r in conn.execute("SELECT platform||':'||query_key FROM listing_metric WHERE run_key=%s",(run_key,))}
    missing = sorted(set(plan)-actual)
    checks.append(_check('planned_queries_missing','coverage',len(missing),0,'warning' if missing else 'ok',missing=missing))
    partial=conn.execute("SELECT count(*) FROM listing_metric WHERE run_key=%s AND status<>'ok'",(run_key,)).fetchone()[0]
    checks.append(_check('transform_partial_queries','integrity',partial,0,'warning' if partial else 'ok'))
    for platform,n,bad in conn.execute("SELECT p.platform,COUNT(*),COUNT(*) FILTER(WHERE "
            "(career_min_yr > career_max_yr) OR (deadline_at < posted_at)) FROM job_posting p "
            "WHERE EXISTS(SELECT 1 FROM posting_snapshot s WHERE s.run_key=%s AND s.platform=p.platform "
            "AND s.posting_id=p.posting_id) GROUP BY 1",(run_key,)):
        ratio=bad/n if n else 0
        checks.append(_check('validity_ratio','validity',round(ratio,4),.03,'warning' if ratio>.03 else 'ok',platform,bad=bad,total=n))
    return checks


def check_run(dsn: str, run_key: str) -> dict:
    with store.connect(dsn) as conn:
        checks = run_checks(conn, run_key)
        conn.execute("DELETE FROM quality_check WHERE run_key=%s", (run_key,))
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO quality_check (run_key, check_name, platform, dimension, metric, threshold, severity, detail) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                [(run_key, c["check_name"], c["platform"], c["dimension"], c["metric"], c["threshold"], c["severity"],
                  json.dumps(c["detail"], ensure_ascii=False)) for c in checks])
    worst = max((c["severity"] for c in checks), key=_ORDER.get, default="ok")
    failed = [f"{c['check_name']}@{c['platform']}" for c in checks if c["severity"] != "ok"]
    return {"severity": worst, "failed": failed, "n_checks": len(checks)}
