"""원천 어댑터 계약 — 원천마다 다른 것(요청 모양·파싱)만 어댑터에 두고, 수집·저장 흐름은 공통으로 씁니다."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from .. import config


@dataclass(frozen=True)
class Query:
    platform: str
    query_key: str      # 'BE' | 'DA' | 'DE' | 'IT'(여러 직무가 섞인 목록 — 파서가 제목·분류로 직무를 나눔)
    job_group: str      # 'BE' | 'DA' | 'DE' | 'MIX'
    params: dict = field(default_factory=dict)


@dataclass
class QuickParse:
    """수집 시점에 바로 읽는 최소 정보 — 다음 페이지를 받을지 정하고, 원본 행의 메타로 남깁니다."""
    item_ids: list[str]
    reported_total: int | None
    has_next: bool | None
    region: str         # 저장할 목록 영역(개인정보성 속성은 scrub 뒤)


class SourceAdapter:
    platform = ""
    label = ""
    host = ""
    body_format = "html"
    page_size = 20
    min_interval_s = config.WEB_MIN_INTERVAL_S
    always_full = False         # 페이지가 적어 매 실행 전수로 도는 원천

    def available(self) -> bool:
        """키가 필요한 원천은 키가 있을 때만 True."""
        return True

    def queries(self) -> list[Query]:
        raise NotImplementedError

    def request(self, q: Query, page_no: int) -> dict:
        """{"method", "url", "params"|"data"|"json", "headers", "encoding"} — 최신순 정렬을 항상 강제합니다."""
        raise NotImplementedError

    def quick_parse(self, text: str, q: Query, page_no: int) -> QuickParse:
        raise NotImplementedError

    def parse_items(self, region: str, q: Query) -> list[dict]:
        """원본 영역 → 공고 dict 목록(순수 함수라 원본에서 언제든 다시 정제(replay)할 수 있음).

        키: posting_id, company_name, title, url, location_raw, career_raw, education, employment_type,
            posted_raw, deadline_raw, apply_start_date, tags(list), extra(dict), job_groups(set|None)
        """
        raise NotImplementedError

    def redact(self, url: str) -> str:
        """요청 URL에서 인증키를 지웁니다(원본 테이블에 키가 남지 않게)."""
        return re.sub(r"(serviceKey|access-key|api[_-]?key)=[^&]+", r"\1=***", url, flags=re.I)


def text_of(node, selector: str | None = None) -> str | None:
    """BeautifulSoup 노드(또는 그 하위 선택자)의 공백 정리 텍스트. 없으면 None."""
    target = node.select_one(selector) if selector else node
    if target is None:
        return None
    value = " ".join(target.get_text(" ", strip=True).split())
    return value or None


_ATTR_SCRUB = re.compile(r'\s(?:data-info|data-gainfo|data-mem-sys|data-mem-id)="[^"]*"')
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_PHONE = re.compile(r"(?<!\d)0\d{1,2}[-.\s]?\d{3,4}[-.\s]?\d{4}(?!\d)")


def scrub_html(html: str) -> str:
    """원본에 남기지 않을 값 — 회원 ID성 속성, 이메일, 전화번호 패턴 — 을 지웁니다."""
    html = _ATTR_SCRUB.sub("", html)
    html = _EMAIL.sub("[email]", html)
    return _PHONE.sub("[phone]", html)
