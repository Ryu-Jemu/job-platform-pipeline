"""인크루트 — 직종별 채용정보 목록(job.incruit.com/jobdb_list, SSR HTML·EUC-KR) 어댑터.

실측(2026-10-02): GET /jobdb_list/searchjob.asp?occ3=…&sortfield=reg&sortorder=1&articlecount=60&page=n
- 응답은 EUC-KR → request에 encoding='cp949'(fetch_with_backoff가 content를 그 인코딩으로 디코딩)
- www.incruit.com은 robots 전체 금지 — 요청하지 않고, 행 안의 회사 링크도 따라가지 않습니다.
- 여러 직종 코드 질의는 같은 키(occ3)를 반복하므로 params를 (키, 값) 튜플 목록으로 보냅니다.
- 행: ul.c_row[jobno] — 공고 ID 13자리, 앞 6자리 YYMMDD = 등록일
- 총건수: 단일 코드 질의만 직종 필터 라벨 '백엔드 (62)'의 괄호 숫자(여러 코드는 공고가 겹쳐 합계를 못 씀 → None)
- 등록: '(N시간전 등록)'은 원문 그대로(정규화가 시간 구간으로), 그 밖은 공고 ID 날짜(그날 12:00 KST, day)
"""
from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta

from bs4 import BeautifulSoup

from .. import normalize
from ..config import KST
from .base import Query, QuickParse, SourceAdapter, scrub_html, text_of

_HOST_URL = "https://job.incruit.com"
# occ3 직종 코드: 16765 백엔드 · 16981 데이터분석 + 16895 데이터사이언스
#                · 17452 데이터파이프라인 + 17170 ETL + 17451 데이터레이크 + 16501 데이터웨어하우징 + 16937 DW모델러
_CODES = {"BE": ("16765",), "DA": ("16981", "16895"), "DE": ("17452", "17170", "17451", "16501", "16937")}

_RE_CAREER = re.compile(r"신입|경력")                  # '경력무관' 포함('학력무관'은 학력이 먼저 가져감)
_RE_EDU = re.compile(r"졸|학력|석사|박사|고교")
_RE_EMP = re.compile(r"정규직|계약직|인턴|파견|프리랜서|위촉|아르바이트|병역|도급|교육생|전임|임원|일용")
_RE_REL_POSTED = re.compile(r"\d+\s*(분|시간)\s*전")   # 시간 단위 상대 표기 — 정규화가 관측 시각 기준 구간으로
_RE_DEADLINE_MD = re.compile(r"~\s*(\d{1,2})\s*[./]\s*(\d{1,2})\s*\(\s*([월화수목금토일])\s*\)")
_WEEKDAYS = "월화수목금토일"


def id_date(posting_id: str | None) -> date | None:
    """공고 ID 앞 6자리(YYMMDD) → 등록일. 형식이 다르면 None."""
    m = re.fullmatch(r"(\d{2})(\d{2})(\d{2})\d+", str(posting_id or ""))
    if not m:
        return None
    try:
        return date(2000 + int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def deadline_from_md(raw: str | None, registered: date | None) -> date | None:
    """'~MM.DD (요일)' → 날짜. 연도는 등록일 기준 올해·다음 해 중 요일이 맞는 쪽(관측 시각 없이 정해지는 순수 함수)."""
    m = _RE_DEADLINE_MD.search(str(raw or ""))
    if not m or registered is None:
        return None
    month, day, wd = int(m.group(1)), int(m.group(2)), _WEEKDAYS.index(m.group(3))
    for year in (registered.year, registered.year + 1):
        try:
            d = date(year, month, day)
        except ValueError:
            continue
        if d >= registered - timedelta(days=1) and d.weekday() == wd:
            return d
    return None


def classify_conditions(cells: list[str]) -> dict:
    """조건 칸(지역·경력·학력·고용형태 순)을 값 모양으로 검증해 배정합니다. 모양이 안 맞는 칸은 남는 자리(지역 우선)로."""
    out = {"location_raw": None, "career_raw": None, "education": None, "employment_type": None}
    rest = []
    for cell in cells:
        if out["education"] is None and _RE_EDU.search(cell):
            out["education"] = cell
        elif out["career_raw"] is None and _RE_CAREER.search(cell):
            out["career_raw"] = cell
        elif out["employment_type"] is None and _RE_EMP.search(cell):
            out["employment_type"] = cell
        else:
            rest.append(cell)
    if rest and out["location_raw"] is None:
        out["location_raw"] = rest.pop(0)
    return out


def _label_total(soup, code: str) -> int | None:
    """직종 필터 체크박스(input[value='&occ3=코드']) → label[for=id] 텍스트의 괄호 숫자."""
    inp = soup.select_one(f"input[value='&occ3={code}']")
    lab = soup.select_one(f"label[for='{inp['id']}']") if inp is not None and inp.get("id") else None
    m = re.search(r"\(\s*([\d,]+)\s*\)", lab.get_text()) if lab is not None else None
    return int(m.group(1).replace(",", "")) if m else None


def _max_linked_page(soup) -> int:
    """페이지 이동 링크(href의 page=N)의 최댓값 — 다음 페이지가 있는지 보는 보조 신호."""
    pages = [int(m.group(1)) for a in soup.select("a[href]") if (m := re.search(r"[?&]page=(\d+)", a["href"]))]
    return max(pages, default=0)


class Incruit(SourceAdapter):
    platform = "incruit"
    label = "인크루트"
    host = "job.incruit.com"
    body_format = "html"
    page_size = 60
    always_full = True          # 질의당 1~2페이지라 매 실행 전수

    def queries(self) -> list[Query]:
        return [Query(self.platform, g, g, {"occ3": codes}) for g, codes in _CODES.items()]

    def request(self, q: Query, page_no: int) -> dict:
        params = [("occ3", c) for c in q.params["occ3"]]
        params += [("sortfield", "reg"), ("sortorder", "1"), ("articlecount", str(self.page_size)),
                   ("page", str(page_no))]
        return {"method": "GET", "url": f"{_HOST_URL}/jobdb_list/searchjob.asp", "params": params,
                "encoding": "cp949"}

    def quick_parse(self, text: str, q: Query, page_no: int) -> QuickParse:
        soup = BeautifulSoup(text, "lxml")
        rows = soup.select("ul.c_row[jobno]")
        ids = [r["jobno"].strip() for r in rows if r["jobno"].strip()]
        codes = q.params["occ3"]
        total = _label_total(soup, codes[0]) if len(codes) == 1 else None
        by_rows = len(ids) >= self.page_size and (total is None or page_no * self.page_size < total)
        has_next = by_rows or _max_linked_page(soup) > page_no
        wrap = soup.select_one("#JobList_Area") or (rows[0].parent if rows else None)
        region = scrub_html(str(wrap)) if wrap is not None else ""
        return QuickParse(ids, total, has_next, region)

    def parse_items(self, region: str, q: Query) -> list[dict]:
        soup = BeautifulSoup(region, "lxml")
        items = []
        for row in soup.select("ul.c_row[jobno]"):
            pid = row["jobno"].strip()
            title_a = row.select_one("div.cell_mid div.cl_top a[href*='jobpost.asp?job=']")
            if not pid or title_a is None:
                continue
            title = (title_a.get("title") or text_of(title_a) or "").strip()
            cells = [t for t in (text_of(s) for s in row.select("div.cell_mid div.cl_md > span")) if t]
            deadline_raw = text_of(row, "div.cell_last div.cl_btm > span:nth-of-type(1)")
            posted_raw = text_of(row, "div.cell_last div.cl_btm > span:nth-of-type(2)")
            registered = id_date(pid)
            item = {
                "posting_id": pid,
                "company_name": text_of(row, "div.cell_first div.cl_top a.cpname"),
                "title": title,
                "url": title_a["href"].strip(),
                "posted_raw": posted_raw,
                "deadline_raw": deadline_raw,
                "apply_start_date": None,
                "tags": [t.rstrip(", ").strip() for t in (text_of(s) for s in row.select("div.cell_mid div.cl_btm span"))
                         if t and t.rstrip(", ").strip()],
                "extra": {"title_groups": sorted(normalize.title_job_groups(title)),
                          "company_tags": text_of(row, "div.cell_first div.cl_btm"), "cells": cells},
                "job_groups": None,     # 요청한 직종 코드로 기록
            }
            item.update(classify_conditions(cells))
            if registered:
                item.update(posted_at=datetime.combine(registered, time(12, 0), KST), posted_precision="day")
            due = deadline_from_md(deadline_raw, registered)
            if due:
                item.update(deadline_at=normalize.end_of_day(due), deadline_kind="date")
            items.append(item)
        return items
