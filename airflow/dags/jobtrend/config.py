"""수집 설정 — 직무, 수집 날짜, 호스트 예의, 예산 상수와 DSN 로딩."""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

# 한국은 서머타임이 없으므로 고정 오프셋으로 충분합니다(컨테이너에 tzdata가 없어도 동작).
KST = timezone(timedelta(hours=9), "KST")
COLLECTION_DATE = date(2026, 10, 2)
WINDOW_START = datetime(2026, 10, 2, 9, 0, tzinfo=KST)
WINDOW_END = datetime(2026, 10, 2, 20, 0, tzinfo=KST)

JOB_GROUPS = {"BE": "백엔드 개발자", "DA": "데이터 분석가·사이언티스트", "DE": "데이터 엔지니어"}

# 정직한 UA — 교육 목적을 밝히고, 브라우저로 위장하지 않습니다.
USER_AGENT = "Mozilla/5.0 (Educational use - job-platform-pipeline data pipeline assignment)"

WEB_MIN_INTERVAL_S = 2.0      # 같은 웹 호스트 요청 시작 간격(동시 1개)
API_MIN_INTERVAL_S = 1.0      # 공식 API 호스트 요청 시작 간격
RUN_DEADLINE_S = 240          # 수집 내부 마감: dagrun_timeout(9분) − 정제·점검 여유
MAX_RETRY_AFTER_S = 60.0      # 이보다 긴 Retry-After는 이번 실행에서 그 호스트를 멈춤
LIGHT_MAX_PAGES = 5           # light 스캔에서 1페이지가 전부 신규(포화)일 때 이어받을 최대 페이지
FULL_PAGE_CAP = 60            # full 스캔 페이지 상한(넘으면 is_complete=false로 기록)
DETAIL_PER_RUN = 20           # 상세 페이지: 실행당 호스트별 상한(신규 공고만 1회)

# 어댑터가 완성·검증되는 대로 추가합니다(가동 순서: 사람인·잡코리아 먼저).
ENABLED_SOURCES = ["saramin", "jobkorea", "incruit", "linkareer"]


def dsn() -> str:
    """JOBTREND_DSN을 읽고, 시연 DB에 잘못 적재하지 않도록 DB 이름을 확인합니다."""
    value = os.environ.get("JOBTREND_DSN", "").strip()
    if not value:
        raise RuntimeError("JOBTREND_DSN이 비어 있습니다 — 노트북은 .env, 컨테이너는 airflow/.env를 확인하세요")
    if not value.split("?")[0].rstrip("/").endswith("/job_platform_db"):
        raise RuntimeError("JOBTREND_DSN이 job_platform_db를 가리키지 않습니다 — 다른 DB에 적재하지 않도록 멈춥니다")
    return value
