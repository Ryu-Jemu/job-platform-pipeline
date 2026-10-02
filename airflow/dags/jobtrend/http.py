"""HTTP 수집 — 호스트별 예의(HostGate)와 지수 백오프 재시도(fetch_with_backoff)."""
from __future__ import annotations

import email.utils
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests

from . import config


class HostGate:
    """한 호스트로 가는 요청을 줄 세웁니다.

    - 요청 시작 간격 ≥ min_interval, 동시 요청 ≤ max_inflight(시작 시각을 '예약'해 스레드 경쟁 없이 간격 보장)
    - 429/503의 Retry-After는 penalize()로 호스트 전체를 함께 쉬게 합니다
    - 403(차단 신호)이면 open_circuit()으로 이번 실행에서 그 호스트에 더 요청하지 않습니다
    """

    def __init__(self, min_interval: float, max_inflight: int = 1):
        self.min_interval = min_interval
        self._sem = threading.BoundedSemaphore(max_inflight)
        self._lock = threading.Lock()
        self._next_at = 0.0
        self._local = threading.local()
        self.circuit_open = False
        self.request_log: list[tuple[float, float]] = []   # (시작, 끝) monotonic — 간격 검증·Gantt용

    def __enter__(self):
        self._sem.acquire()
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next_at)
            self._next_at = start + self.min_interval
        time.sleep(max(0.0, start - now))
        self._local.t0 = time.monotonic()
        with self._lock:
            self._next_at = max(self._next_at, self._local.t0 + self.min_interval)
        return self

    def __exit__(self, *exc):
        with self._lock:
            self.request_log.append((self._local.t0, time.monotonic()))
        self._sem.release()
        return False

    def penalize(self, seconds: float) -> None:
        with self._lock:
            self._next_at = max(self._next_at, time.monotonic() + seconds)

    def open_circuit(self) -> None:
        self.circuit_open = True


@dataclass
class FetchResult:
    outcome: str            # ok | http_error | network_error | rate_limited | blocked | circuit_open | skipped_deadline
    status: int | None
    attempts: int           # 실제로 보낸 요청 수(예산 단위)
    trace: list = field(default_factory=list)   # [{attempt, status, wait_s}]
    waited_ms: int = 0
    elapsed_ms: int = 0
    text: str | None = None
    url: str = ""
    error: str | None = None


def parse_retry_after(resp) -> float | None:
    """Retry-After 헤더(초 또는 HTTP-date)를 초로 바꿉니다. 없으면 None."""
    value = resp.headers.get("Retry-After") if resp is not None else None
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def fetch_with_backoff(session, req: dict, gate: HostGate, *, max_attempts: int = 4, base: float = 1.0,
                       cap: float = 30.0, max_retry_after: float = config.MAX_RETRY_AFTER_S,
                       deadline: float | None = None, sleep=time.sleep) -> FetchResult:
    """요청 하나를 보내고, 일시적인 실패면 대기 시간을 늘려 가며 다시 보냅니다.

    - 재시도: 연결 오류·타임아웃, 408·500·502·503·504, 429(Retry-After가 있으면 그 값만큼 호스트 전체 냉각)
    - 즉시 중단: 400·401·404·410·422 등 4xx(다시 보내도 결과가 같음), 403(차단 → 회로 개방, 다시 두드리지 않음)
    - 대기: equal jitter — d = min(cap, base·2^n), wait = d/2 + U(0, d/2). 다음 대기 구간이 항상 이전보다 깁니다.
    - 마감: 다음 대기가 실행 내부 마감(deadline, monotonic)을 넘기면 skipped_deadline으로 끝냅니다.
    """
    url = req["url"]
    trace: list[dict] = []
    waited = 0.0
    t_start = time.monotonic()
    status: int | None = None
    error: str | None = None

    def _result(outcome, attempts, text=None, final_url=url):
        return FetchResult(outcome, status, attempts, trace, int(waited * 1000),
                           int((time.monotonic() - t_start) * 1000), text, final_url, error)

    for attempt in range(max_attempts):
        if deadline is not None and time.monotonic() >= deadline:
            return _result("skipped_deadline", attempt)
        if gate.circuit_open:
            return _result("circuit_open", attempt)
        resp = None
        try:
            with gate:
                if gate.circuit_open:
                    return _result("circuit_open", attempt)
                if deadline is not None and time.monotonic() >= deadline:
                    return _result("skipped_deadline", attempt)
                resp = session.request(req.get("method", "GET"), url, params=req.get("params"),
                                       data=req.get("data"), json=req.get("json"),
                                       headers=req.get("headers"), timeout=(5, min(20, max(0.1, deadline-time.monotonic())) if deadline else 20))
                # semaphore를 놓기 전에 회로/냉각을 반영해 대기 중인 스레드도 즉시 따른다.
                if resp.status_code == 403:
                    gate.open_circuit()
                if resp.status_code in (429,503):
                    cooldown = parse_retry_after(resp)
                    if cooldown is not None:
                        gate.penalize(cooldown)
            status, error = resp.status_code, None
        except (requests.ConnectionError, requests.Timeout) as exc:
            status, error = None, f"{type(exc).__name__}: {exc}"[:300]

        if status == 200:
            encoding = req.get("encoding")
            text = resp.content.decode(encoding, errors="replace") if encoding else resp.text
            return _result("ok", attempt + 1, text, resp.url)
        if status == 403:
            gate.open_circuit()
            return _result("blocked", attempt + 1)
        if status is not None and 400 <= status < 500 and status not in (408, 429):
            return _result("http_error", attempt + 1)

        retry_after = parse_retry_after(resp) if status in (429, 503) else None
        if retry_after is not None and retry_after > max_retry_after:
            gate.open_circuit()
            return _result("rate_limited", attempt + 1)
        d = min(cap, base * 2 ** attempt)
        wait = retry_after if retry_after is not None else d / 2 + random.uniform(0, d / 2)
        last = attempt == max_attempts - 1
        trace.append({"attempt": attempt + 1, "status": status, "wait_s": 0.0 if last else round(wait, 3)})
        if last:
            break
        if deadline is not None and time.monotonic() + wait > deadline:
            return _result("skipped_deadline", attempt + 1)
        if retry_after is not None:
            gate.penalize(retry_after)      # 다음 요청이 게이트에서 대기 — 같은 호스트의 다른 스레드도 함께
            waited += retry_after
        else:
            sleep(wait)
            waited += wait

    outcome = "rate_limited" if status == 429 else ("network_error" if status is None else "http_error")
    return _result(outcome, max_attempts)
