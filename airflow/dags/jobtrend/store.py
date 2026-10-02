"""적재 — DB 연결, 스키마 적용, 원본 기록, 계획에 필요한 조회."""
from __future__ import annotations

import hashlib
import json
import pathlib

import psycopg

from . import config

SCHEMA_SQL = pathlib.Path(__file__).with_name("schema.sql")


def connect(dsn: str | None = None) -> psycopg.Connection:
    conn = psycopg.connect(dsn or config.dsn(), connect_timeout=10)
    conn.execute("SET TIME ZONE 'Asia/Seoul'")
    return conn


def apply_schema(dsn: str | None = None) -> None:
    with connect(dsn) as conn:
        conn.execute(SCHEMA_SQL.read_text(encoding="utf-8"))


def table_counts(dsn: str | None = None) -> dict[str, int]:
    names = ["crawl_run", "raw_listing_page", "raw_posting_detail", "job_posting", "posting_role",
             "posting_snapshot", "listing_metric", "job_canonical", "posting_canonical", "posting_skill",
             "quality_check"]
    with connect(dsn) as conn:
        return {n: conn.execute(f"SELECT COUNT(*) FROM {n}").fetchone()[0] for n in names}


def insert_raw(conn, run_key: str, adapter, q, page_no: int, scan_kind: str, req: dict, fr, outcome: str,
               qp) -> int:
    """목록 요청 1건을 원본 테이블에 1행으로 남깁니다(실패도 기록).

    같은 (run_key, 원천, 질의, 페이지)가 이미 ok면 덮어쓰지 않고, 실패 행만 새 결과로 바꿉니다(재시도 이어받기).
    """
    body = qp.region if (qp is not None and outcome in ("ok", "empty", "parse_error")) else None
    sha = hashlib.sha256(body.encode("utf-8")).hexdigest() if body else None
    url = adapter.redact(fr.url or req["url"])
    row = conn.execute(
        """
        INSERT INTO raw_listing_page (run_key, platform, query_key, job_group, page_no, scan_kind, request_url,
               outcome, http_status, attempts, retry_trace, waited_ms, elapsed_ms, n_items, reported_total,
               has_next, item_ids, body_format, body, body_sha256, error)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT ON CONSTRAINT uq_raw_unit DO UPDATE SET
               scan_kind = EXCLUDED.scan_kind, request_url = EXCLUDED.request_url, outcome = EXCLUDED.outcome,
               http_status = EXCLUDED.http_status, attempts = raw_listing_page.attempts + EXCLUDED.attempts,
               retry_trace = EXCLUDED.retry_trace, waited_ms = EXCLUDED.waited_ms, elapsed_ms = EXCLUDED.elapsed_ms,
               fetched_at = now(), n_items = EXCLUDED.n_items, reported_total = EXCLUDED.reported_total,
               has_next = EXCLUDED.has_next, item_ids = EXCLUDED.item_ids, body_format = EXCLUDED.body_format,
               body = EXCLUDED.body, body_sha256 = EXCLUDED.body_sha256, error = EXCLUDED.error
        WHERE raw_listing_page.outcome <> 'ok'
        RETURNING raw_id
        """,
        (run_key, adapter.platform, q.query_key, q.job_group, page_no, scan_kind, url, outcome, fr.status,
         fr.attempts, json.dumps(fr.trace), fr.waited_ms, fr.elapsed_ms,
         len(qp.item_ids) if qp else None, qp.reported_total if qp else None, qp.has_next if qp else None,
         qp.item_ids if qp else None, adapter.body_format if body else None, body, sha,
         (fr.error or None) if outcome != "ok" else None),
    ).fetchone()
    conn.commit()
    return row[0] if row else -1


def ok_pages(conn, run_key: str, platform: str, query_key: str) -> set[int]:
    """이미 ok로 받은 페이지 — Airflow 재시도 때 다시 요청하지 않습니다(예산 보호)."""
    rows = conn.execute("SELECT page_no FROM raw_listing_page WHERE run_key=%s AND platform=%s AND query_key=%s "
                        "AND outcome='ok'", (run_key, platform, query_key)).fetchall()
    return {r[0] for r in rows}


def has_complete_full(conn, platform: str, query_key: str, exclude_run: str) -> bool:
    """이 (원천, 질의)에 완료된 full 스캔(전 페이지 ok, 마지막 페이지 has_next=false)이 있었는가 — 없으면 기준선 full."""
    return conn.execute(
        """
        SELECT EXISTS (
          SELECT 1 FROM raw_listing_page
          WHERE platform=%s AND query_key=%s AND scan_kind='full' AND run_key <> %s
          GROUP BY run_key
          HAVING bool_and(outcome='ok') AND bool_or(has_next IS FALSE) AND COUNT(*) = MAX(page_no))
        """, (platform, query_key, exclude_run)).fetchone()[0]


def known_ids(conn, platform: str, query_key: str, exclude_run: str) -> frozenset:
    rows = conn.execute("SELECT DISTINCT unnest(item_ids) FROM raw_listing_page WHERE platform=%s AND query_key=%s "
                        "AND outcome='ok' AND run_key <> %s", (platform, query_key, exclude_run)).fetchall()
    return frozenset(r[0] for r in rows)
