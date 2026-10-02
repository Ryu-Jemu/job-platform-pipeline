"""잡코리아 웹 — 직무별 채용정보 목록 조각(_GI_List, SSR HTML 조각) 어댑터.

실측(2026-10-02): POST /Recruit/Home/_GI_List/ + X-Requested-With: XMLHttpRequest(없으면 200·0B 빈 응답)
- form: condition[duty]=코드, condition[menucode]=duty, order=2(등록일순), pagesize=50, page=n
- 총건수: input#hdnGICnt(일반공고 기준, '1,609'처럼 콤마 포함)
- 행: tr.devloopArea — data-gno(공고번호), data-brazeinfo(제목|gno|보조번호|시작일|마감일|기업명|기업번호)
- 조건 칸(p.etc span.cell)은 학력 칸이 빠진 행이 있어(재현 검증 DE 8/50행) 위치가 아니라 값 패턴으로 분류합니다.
- 상시채용은 brazeinfo 마감일이 '2070-01-01' 센티널로 들어옵니다.
"""
from __future__ import annotations

import re

from bs4 import BeautifulSoup

from .base import Query, QuickParse, SourceAdapter, scrub_html, text_of

_CODES = {"BE": "1000229", "DA": "1000418", "DE": "1000236"}   # 백엔드개발자 · 데이터분석가 · 데이터엔지니어(AI·개발·데이터 10031 하위)

_RE_CAREER = re.compile(r"신입|경력")             # '경력무관' 포함, '학력무관'은 제외
_RE_EDU = re.compile(r"졸|학력|석사|박사|고교")
_RE_EMP = re.compile(r"정규직|계약직|인턴|파견|프리랜서|위촉|아르바이트|병역|도급|교육생")


def classify_cells(cells: list[str]) -> dict:
    """조건 칸 텍스트들을 값 모양으로 경력·학력·고용형태·지역에 배정합니다(남는 칸은 지역 후보)."""
    out = {"career_raw": None, "education": None, "employment_type": None, "location_raw": None}
    for cell in cells:
        if out["education"] is None and _RE_EDU.search(cell):
            out["education"] = cell
        elif out["career_raw"] is None and _RE_CAREER.search(cell):
            out["career_raw"] = cell
        elif out["employment_type"] is None and _RE_EMP.search(cell):
            out["employment_type"] = cell
        elif out["location_raw"] is None:
            out["location_raw"] = cell
    return out


class JobKorea(SourceAdapter):
    platform = "jobkorea"
    label = "잡코리아"
    host = "www.jobkorea.co.kr"
    page_size = 50

    def queries(self) -> list[Query]:
        return [Query(self.platform, g, g, {"duty": c}) for g, c in _CODES.items()]

    def request(self, q: Query, page_no: int) -> dict:
        form = {"condition[duty]": q.params["duty"], "condition[menucode]": "duty", "page": page_no,
                "direct": 0, "order": 2, "pagesize": self.page_size, "tabindex": 0, "onePick": 0,
                "confirm": 0, "profile": 0}
        return {"method": "POST", "url": "https://www.jobkorea.co.kr/Recruit/Home/_GI_List/",
                "data": form, "headers": {"X-Requested-With": "XMLHttpRequest"}}

    def quick_parse(self, text: str, q: Query, page_no: int) -> QuickParse:
        soup = BeautifulSoup(text, "lxml")
        ids = [r["data-gno"] for r in soup.select("tr.devloopArea") if r.get("data-gno")]
        cnt = soup.select_one("#hdnGICnt")
        total = int(re.sub(r"\D", "", cnt.get("value", ""))) if cnt and re.search(r"\d", cnt.get("value", "")) else None
        has_next = (page_no * self.page_size < total) if total is not None else len(ids) >= self.page_size
        return QuickParse(ids, total, has_next, scrub_html(text))

    def parse_items(self, region: str, q: Query) -> list[dict]:
        soup = BeautifulSoup(region, "lxml")
        items = []
        for row in soup.select("tr.devloopArea"):
            gno = row.get("data-gno")
            if not gno:
                continue
            braze = (row.get("data-brazeinfo") or "").split("|")
            braze += [""] * (7 - len(braze))
            cells = [t for t in (text_of(c) for c in row.select("p.etc span.cell")) if t]
            keywords = text_of(row, "p.dsc") or ""
            item = {
                "posting_id": gno,
                "company_name": text_of(row, "td.tplCo a.link") or braze[5] or None,
                "title": text_of(row, "td.tplTit .titBx a.link") or braze[0],
                "url": f"https://www.jobkorea.co.kr/Recruit/GI_Read/{gno}",
                "posted_raw": text_of(row, "span.time"),
                "deadline_raw": braze[4] or text_of(row, "span.date"),
                "apply_start_date": braze[3] or None,
                "tags": [k.strip() for k in keywords.split(",") if k.strip()],
                "extra": {"deadline_text": text_of(row, "span.date"), "company_id": braze[6] or None,
                          "cells": cells},
                "job_groups": None,
            }
            item.update(classify_cells(cells))
            items.append(item)
        return items
