"""사람인 웹 — 직업별 채용정보 목록(SSR HTML) 어댑터.

실측(2026-10-02): /zf_user/jobs/list/job-category?cat_kewd=…&sort=RD&page=n&page_count=100
- robots(*)가 목록·상세 경로를 허용. 1페이지 최대 100건, sort=RD = 등록일순(최신순)
- 총건수: .list_total_count .total_count em (없으면 페이지 안 패널 JSON의 KEWD_CD_NO별 COUNT)
- 등록 시각: '.support_detail .deadlines'의 'N분/시간/일 전 등록' 상대 표기(수정되면 '… 전 수정')
"""
from __future__ import annotations

import re

from bs4 import BeautifulSoup

from .base import Query, QuickParse, SourceAdapter, scrub_html, text_of

_CODES = {"BE": "84", "DA": "82,2248", "DE": "83"}   # 84 백엔드/서버개발 · 82 데이터분석가 + 2248 데이터 사이언티스트 · 83 데이터엔지니어


def _panel_count(text: str, code: str) -> int | None:
    """패널 인라인 JSON(따옴표가 ", \\", \\u0022로 섞임)에서 KEWD_CD_NO=code의 COUNT."""
    t = text.replace("\\u0022", '"').replace('\\"', '"')
    m = re.search(r'"KEWD_CD_NO"\s*:\s*"?%s"?[^{}]{0,400}?"COUNT"\s*:\s*"?(\d+)' % re.escape(code), t)
    return int(m.group(1)) if m else None


class Saramin(SourceAdapter):
    platform = "saramin"
    label = "사람인"
    host = "www.saramin.co.kr"
    page_size = 100

    def queries(self) -> list[Query]:
        return [Query(self.platform, g, g, {"cat_kewd": c}) for g, c in _CODES.items()]

    def request(self, q: Query, page_no: int) -> dict:
        return {"method": "GET", "url": "https://www.saramin.co.kr/zf_user/jobs/list/job-category",
                "params": {"cat_kewd": q.params["cat_kewd"], "sort": "RD", "page": page_no,
                           "page_count": self.page_size}}

    def quick_parse(self, text: str, q: Query, page_no: int) -> QuickParse:
        soup = BeautifulSoup(text, "lxml")
        wrap = soup.select_one("#default_list_wrap")
        rows = wrap.select("div.list_body > div.list_item") if wrap else []
        ids = [r["id"][4:] for r in rows if r.get("id", "").startswith("rec-")]
        total = None
        em = soup.select_one(".list_total_count .total_count em")
        if em and re.search(r"\d", em.get_text()):
            total = int(re.sub(r"\D", "", em.get_text()))
        elif "," not in q.params["cat_kewd"]:
            total = _panel_count(text, q.params["cat_kewd"])
        has_next = (page_no * self.page_size < total) if total is not None else len(ids) >= self.page_size
        region = scrub_html(str(wrap)) if wrap else ""
        return QuickParse(ids, total, has_next, region)

    def parse_items(self, region: str, q: Query) -> list[dict]:
        soup = BeautifulSoup(region, "lxml")
        items = []
        for row in soup.select("div.list_body > div.list_item"):
            pid = row.get("id", "")[4:]
            title_a = row.select_one(".job_tit a.str_tit")
            if not pid or title_a is None:
                continue
            career_full = text_of(row, ".recruit_info .career") or ""
            parts = [p.strip() for p in career_full.split("·") if p.strip()]
            items.append({
                "posting_id": pid,
                "company_name": text_of(row, ".company_nm .str_tit"),
                "title": (title_a.get("title") or text_of(title_a) or "").strip(),
                "url": f"https://www.saramin.co.kr/zf_user/jobs/relay/view?rec_idx={pid}",
                "location_raw": text_of(row, ".recruit_info .work_place"),
                "career_raw": " · ".join(parts[:-1]) if len(parts) > 1 else (parts[0] if parts else None),
                "employment_type": parts[-1] if len(parts) > 1 else None,
                "education": text_of(row, ".recruit_info .education"),
                "posted_raw": text_of(row, ".support_detail .deadlines"),
                "deadline_raw": text_of(row, ".support_detail .date"),
                "apply_start_date": None,
                "tags": [t for t in (text_of(s) for s in row.select(".job_meta .job_sector > span")) if t],
                "extra": {"badge": text_of(row, ".job_badge"), "group": text_of(row, ".main_corp")},
                "job_groups": None,     # 요청한 직무 코드로 기록
            })
        return items
