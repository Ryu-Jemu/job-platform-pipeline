"""실행 로그 — crawl_run 열기·마감, 멈춘 실행 정리, 노트북 실제 요청용 유휴창 판정."""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from . import config, store


def floor_slot(ts: datetime) -> datetime:
    ts = ts.astimezone(config.KST)
    return ts.replace(minute=ts.minute - ts.minute % 10, second=0, microsecond=0)


def sweep_stale(conn, older_than_min: int = 15) -> int:
    """dagrun_timeout 등으로 끝나지 못하고 running에 남은 실행을 failed로 정리합니다."""
    cur = conn.execute("UPDATE crawl_run SET status='failed', finished_at=now(), "
                       "note=concat_ws('; ', note, 'sweep_stale') "
                       "WHERE status='running' AND started_at < now() - make_interval(mins => %s)", (older_than_min,))
    return cur.rowcount


def open_run(dsn: str, run_key: str, slot_ts: datetime, run_kind: str, code_version: str) -> dict:
    with store.connect(dsn) as conn:
        swept = sweep_stale(conn)
        conn.execute(
            """
            INSERT INTO crawl_run (run_key, run_kind, slot_ts, code_version)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (run_key) DO UPDATE SET status='running', started_at=now(), finished_at=NULL,
                   code_version=EXCLUDED.code_version
            """, (run_key, run_kind, slot_ts, code_version))
        db, tz = conn.execute("SELECT current_database(), current_setting('TimeZone')").fetchone()
    return {"run_key": run_key, "slot_ts": slot_ts.isoformat(), "run_kind": run_kind, "swept": swept,
            "database": db, "timezone": tz, "code_version": code_version}


def finalize_run(dsn: str, run_key: str, force: str | None = None) -> dict:
    """원본 결과로 상태를 정합니다: 전부 ok/empty → success, 일부 → partial, ok 0 → failed."""
    with store.connect(dsn) as conn:
        rows = conn.execute("SELECT platform, outcome, COUNT(*), SUM(attempts) FROM raw_listing_page "
                            "WHERE run_key=%s GROUP BY 1, 2", (run_key,)).fetchall()
        by_outcome: dict[str, int] = {}
        for _, outcome, n, _attempts in rows:
            by_outcome[outcome] = by_outcome.get(outcome, 0) + n
        good = by_outcome.get("ok", 0) + by_outcome.get("empty", 0)
        bad = sum(by_outcome.values()) - good
        status = force or ("failed" if good == 0 else "partial" if bad else "success")
        by_platform = {}
        for p,o,n,a in rows:
            v=by_platform.setdefault(p,{'by_outcome':{},'n':0,'attempts':0})
            v['by_outcome'][o]=n;v['n']+=n;v['attempts']+=int(a or 0)
        summary = {"by_outcome": by_outcome,"by_platform":by_platform}
        critical = conn.execute("SELECT EXISTS(SELECT 1 FROM quality_check WHERE run_key=%s AND severity='critical')",
                                (run_key,)).fetchone()[0]
        if critical and not force: status='failed'
        elif not force:
            incomplete=conn.execute("SELECT EXISTS(SELECT 1 FROM listing_metric WHERE run_key=%s AND status<>'ok') "
                "OR EXISTS(SELECT 1 FROM quality_check WHERE run_key=%s AND check_name='planned_queries_missing' AND metric>0)",
                (run_key,run_key)).fetchone()[0]
            if incomplete and status=='success':status='partial'
        conn.execute("UPDATE crawl_run SET status=%s, finished_at=now(), summary=%s WHERE run_key=%s",
                     (status, json.dumps(summary, ensure_ascii=False), run_key))
    return {"run_key": run_key, "status": status, **summary}


def mark_failed(dsn: str, run_key: str, note: str) -> None:
    with store.connect(dsn) as conn:
        conn.execute("UPDATE crawl_run SET status='failed', finished_at=now(), note=concat_ws('; ', note, %s::text) "
                     "WHERE run_key=%s", (note, run_key))


def is_idle(dsn: str, now: datetime | None = None) -> bool:
    """노트북의 실제 요청 셀이 DAG와 겹치지 않을 때인가: running 중인 scheduled 실행이 없고, 분 % 10이 3~8."""
    now = (now or datetime.now(config.KST)).astimezone(config.KST)
    with store.connect(dsn) as conn:
        running = conn.execute("SELECT COUNT(*) FROM crawl_run WHERE status='running' AND run_kind='scheduled' "
                               "AND started_at > now() - interval '15 minutes'").fetchone()[0]
    return running == 0 and 3 <= now.minute % 10 <= 8


def slot_for(run_after: datetime | None, run_kind: str) -> datetime:
    base = run_after or datetime.now(config.KST)
    return base.astimezone(config.KST) if run_kind == "scheduled" else floor_slot(base)


__all__ = ["open_run", "finalize_run", "mark_failed", "sweep_stale", "is_idle", "floor_slot", "slot_for",
           "timedelta"]
