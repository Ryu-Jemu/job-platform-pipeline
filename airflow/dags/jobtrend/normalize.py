"""정규화 — 원천마다 다른 표기를 공통 의미로 바꾸는 순수 함수들(원본에서 언제든 다시 적용 가능)."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, time, timedelta

from .config import KST

# ── 회사명·제목 키(플랫폼 간 같은 공고 판정용) ───────────────────────────────────────────────
_KO_LEGAL = ["㈜", "(주)", "（주）", "주식회사", "(유)", "유한회사", "(사)", "사단법인", "(재)", "재단법인", "(합)", "합자회사", "(의)", "의료법인"]
_EN_LEGAL = re.compile(r"\b(co\.?\s*,?\s*ltd\.?|inc\.?|corp\.?|corporation|ltd\.?|limited|company|llc)\b", re.I)


def company_key(name: str | None) -> str:
    """17번 normalize_company_name(㈜·(주)·주식회사·공백 제거)을 확장: 법인 표기·영문 법인 접미사·구두점 제거, 소문자."""
    n = str(name or "")
    for token in _KO_LEGAL:
        n = n.replace(token, "")
    n = _EN_LEGAL.sub("", n).lower()
    n = re.sub(r"[^0-9a-z가-힣]+", "", n)
    return n or str(name or "").strip().lower()


# 대괄호류 머리말([회사명]·【공고】 등)만 지우고, 소괄호 안 내용(주니어/시니어·팀 이름)은 남겨 서로 다른 공고가 합쳐지지 않게 합니다.
_BRACKETS = re.compile(r"[\[【<〈「『［][^\]】>〉」』］]*[\]】>〉」』］]")
_NOISE = re.compile(r"20\d\d\s*년?|\d{1,2}\s*월|상반기|하반기|신입|경력|채용|모집|공고|정규직|계약직|인턴|부문|담당자|"
                    r"직원|사원|신규|대규모|수시|상시|전형|각\s|및\s|외\s|급구|우대")


def title_key(title: str | None) -> str:
    t = str(title or "").lower()
    # 괄호 안 팀/브랜드를 버리면 같은 회사의 서로 다른 채용이 합쳐진다. 내용은 보존한다.
    t = _NOISE.sub(" ", t)
    t = re.sub(r"[^0-9a-z가-힣]+", " ", t)
    return " ".join(t.split())


# ── 제목 기반 직무 분류(혼합 목록 분류 + 코드 기반 직무의 순도 점검) ─────────────────────────
_ROLE_PATTERNS = {
    "BE": re.compile(r"백엔드|back[\s-]?end|서버\s*(개발|엔지니어|프로그래머)|server[\s-]*(side|developer|engineer)|"
                     r"java\s*(개발|developer|engineer)|자바\s*개발|spring|스프링|node\.?js|django|golang|"
                     r"kotlin\s*(개발|developer)|api\s*(개발|developer)", re.I),
    "DA": re.compile(r"데이터\s*분석|data\s*analy(st|tics|sis)|데이터\s*사이언|data\s*scien(tist|ce)|분석가|애널리스트|"
                     r"analyst|비즈니스\s*인텔리전스|통계\s*분석|그로스\s*(분석|해커)", re.I),
    "DE": re.compile(r"데이터\s*엔지니어|data\s*engineer|데이터\s*(플랫폼|파이프라인|인프라|아키텍트|레이크|웨어하우스)|"
                     r"data\s*(platform|pipeline|infra|architect|warehouse|lake)|\betl\b|빅데이터\s*(엔지니어|개발)|"
                     r"big\s*data\s*(engineer|developer)|analytics\s*engineer|dw\s*(개발|엔지니어)", re.I),
}
_BI = re.compile(r"\bBI\b", re.I)      # 대소문자 무시 + 단어 경계(mobile 속 bi 오탐 방지)


def title_job_groups(title: str | None) -> set[str]:
    t = str(title or "")
    groups = {g for g, p in _ROLE_PATTERNS.items() if p.search(t)}
    if _BI.search(t):
        groups.add("DA")
    return groups


# ── 시각: 상대 표기('N분/시간/일 전 등록') → 첫 관측 기준 구간 ─────────────────────────────
_REL = re.compile(r"(\d+)\s*(분|시간|일)\s*전\s*(등록|수정)?")
_ABS = re.compile(r"(20\d\d)[.\-/](\d{1,2})[.\-/](\d{1,2})")
_MD_POSTED = re.compile(r"(\d{1,2})\s*/\s*(\d{1,2})\s*\([월화수목금토일]\)\s*등록")
_STEP = {"분": (timedelta(minutes=1), "minute"), "시간": (timedelta(hours=1), "hour"), "일": (timedelta(days=1), "day")}


def parse_posted(raw: str | None, observed_at: datetime) -> dict:
    """{posted_at, posted_lo, posted_hi, posted_precision, posted_kind('등록'|'수정'|None)}.

    'N분 전'은 [관측−(N+1)분, 관측−N분] 구간의 중앙값으로 둡니다. '수정' 표기는 등록 시각을 알 수 없어 비웁니다.
    첫 관측에서 정한 값을 고정하는 것은 적재(upsert) 쪽 규칙입니다.
    """
    out = {"posted_at": None, "posted_lo": None, "posted_hi": None, "posted_precision": "none", "posted_kind": None}
    r = str(raw or "")
    m = _REL.search(r)
    if m:
        n, unit, kind = int(m.group(1)), m.group(2), m.group(3) or "등록"
        out["posted_kind"] = kind
        if kind == "수정":
            return out
        step, precision = _STEP[unit]
        hi = observed_at - n * step
        lo = observed_at - (n + 1) * step
        out.update(posted_at=lo + (hi - lo) / 2, posted_lo=lo, posted_hi=hi, posted_precision=precision)
        return out
    m = _ABS.search(r)
    md = None if m else _MD_POSTED.search(r)
    if m or md:
        if m:
            d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        else:                                           # 잡코리아 'MM/DD (요일) 등록' — 과거 날짜라 미래면 작년
            today = observed_at.astimezone(KST).date()
            d = date(today.year, int(md.group(1)), int(md.group(2)))
            if d > today:
                d = date(today.year - 1, d.month, d.day)
        lo = datetime.combine(d, time(0, 0), KST)
        out.update(posted_at=lo + timedelta(hours=12), posted_lo=lo, posted_hi=lo + timedelta(days=1),
                   posted_precision="day", posted_kind="등록")
    return out


# ── 마감 ─────────────────────────────────────────────────────────────────────────────────────
def end_of_day(d: date) -> datetime:
    return datetime.combine(d, time(23, 59, 59), KST)


def _infer_year(month: int, day: int, observed: date) -> date | None:
    """연도 없는 'MM.DD'는 관측일 기준으로 연도를 정하고, 이미 지난 날짜면 다음 해로 봅니다."""
    try:
        d = date(observed.year, month, day)
    except ValueError:
        return None
    return d if d >= observed - timedelta(days=1) else date(observed.year + 1, month, day)


def parse_deadline(raw: str | None, observed_at: datetime) -> tuple[datetime | None, str]:
    """(deadline_at, deadline_kind) — kind: date | always(상시) | until_filled(채용시 마감) | unknown."""
    r = " ".join(str(raw or "").split())
    if not r:
        return None, "unknown"
    today = observed_at.astimezone(KST).date()
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", r)
    if m:                                               # 잡코리아 brazeinfo — 상시채용은 2070-01-01 센티널
        if m.group(1) >= "2070":
            return None, "always"
        return end_of_day(date(int(m.group(1)), int(m.group(2)), int(m.group(3)))), "date"
    if "상시" in r:
        return None, "always"
    if re.search(r"채용\s*시", r):
        return None, "until_filled"
    if re.search(r"오늘\s*마감", r):
        return end_of_day(today), "date"
    if re.search(r"내일\s*마감", r):
        return end_of_day(today + timedelta(days=1)), "date"
    m = re.search(r"D\s*-\s*(\d+)", r)
    if m:
        return end_of_day(today + timedelta(days=int(m.group(1)))), "date"
    m = re.search(r"(\d{1,2})\s*시\s*마감", r)
    if m:
        return datetime.combine(today, time(min(int(m.group(1)), 23), 0), KST), "date"
    m = _ABS.search(r)
    if m:
        return end_of_day(date(int(m.group(1)), int(m.group(2)), int(m.group(3)))), "date"
    m = re.search(r"(\d{1,2})\s*[./]\s*(\d{1,2})", r)
    if m:
        d = _infer_year(int(m.group(1)), int(m.group(2)), today)
        return (end_of_day(d), "date") if d else (None, "unknown")
    return None, "unknown"


# ── 경력·지역 ─────────────────────────────────────────────────────────────────────────────────
def parse_career(raw: str | None) -> tuple[str, int | None, int | None]:
    """(career_type, min_yr, max_yr) — type: new | exp | new_or_exp | any | unknown."""
    r = str(raw or "").replace(" ", "")
    if not r:
        return "unknown", None, None
    if "경력무관" in r or (r == "무관"):
        return "any", None, None
    if "년수무관" in r:                                  # '경력(년수무관)' = 경력자, 연차 제한 없음
        return "exp", None, None
    rng = re.search(r"(\d+)~(\d+)년", r)
    up = re.search(r"(\d+)년(↑|이상)", r)
    mn = int(rng.group(1)) if rng else (int(up.group(1)) if up else None)
    mx = int(rng.group(2)) if rng else None
    has_new = "신입" in r
    has_exp = "경력" in r or mn is not None
    if has_new and has_exp:
        return "new_or_exp", 0, mx
    if has_new:
        return "new", 0, 0
    if has_exp:
        return "exp", mn, mx
    return "unknown", None, None


_SIDO_MAP = {"서울": "서울", "경기": "경기", "인천": "인천", "부산": "부산", "대구": "대구", "광주": "광주", "대전": "대전",
             "울산": "울산", "세종": "세종", "강원": "강원", "충북": "충북", "충남": "충남", "전북": "전북", "전남": "전남",
             "경북": "경북", "경남": "경남", "제주": "제주", "충청북": "충북", "충청남": "충남", "전라북": "전북", "전북특별": "전북",
             "전라남": "전남", "경상북": "경북", "경상남": "경남", "강원특별": "강원", "제주특별": "제주"}


def sido(location: str | None) -> str | None:
    loc = str(location or "").strip()
    if not loc:
        return None
    if re.search(r"재택|원격|remote", loc, re.I):
        return "재택"
    if "해외" in loc:
        return "해외"
    if "전국" in loc:
        return "전국"
    for key in sorted(_SIDO_MAP, key=len, reverse=True):
        if loc.startswith(key):
            return _SIDO_MAP[key]
    return None


# ── 레코드 조립 ───────────────────────────────────────────────────────────────────────────────
_PRECISION_RANK = {"second": 0, "minute": 1, "hour": 2, "day": 3, "none": 9}


def build_record(platform: str, item: dict, observed_at: datetime) -> dict:
    """어댑터 dict → job_posting 컬럼. 어댑터가 정확한 posted_at·deadline_at을 주면 그것을 우선합니다."""
    posted = parse_posted(item.get("posted_raw"), observed_at)
    if item.get("posted_at"):
        p = item["posted_at"]
        posted.update(posted_at=p, posted_lo=p, posted_hi=p, posted_precision=item.get("posted_precision", "second"))
    if item.get("deadline_kind"):
        deadline_at, deadline_kind = item.get("deadline_at"), item["deadline_kind"]
    else:
        deadline_at, deadline_kind = parse_deadline(item.get("deadline_raw"), observed_at)
    ctype, cmin, cmax = parse_career(item.get("career_raw"))
    apply_start = item.get("apply_start_date")
    rec = {
        "platform": platform, "posting_id": str(item["posting_id"]),
        "company_name": (item.get("company_name") or "(미상)").strip(),
        "title": (item.get("title") or "").strip(), "url": item["url"],
        "location_raw": item.get("location_raw"), "sido": sido(item.get("location_raw")),
        "career_raw": item.get("career_raw"), "career_type": ctype, "career_min_yr": cmin, "career_max_yr": cmax,
        "education": item.get("education"), "employment_type": item.get("employment_type"),
        "posted_at": posted["posted_at"], "posted_lo": posted["posted_lo"], "posted_hi": posted["posted_hi"],
        "posted_precision": posted["posted_precision"],
        "apply_start_date": date.fromisoformat(apply_start) if isinstance(apply_start, str) and apply_start else apply_start,
        "deadline_at": deadline_at, "deadline_raw": item.get("deadline_raw"), "deadline_kind": deadline_kind,
        "tags_raw": list(item.get("tags") or []),
        "extra": {**(item.get("extra") or {}), "posted_raw": item.get("posted_raw"),
                  **({"posted_kind": posted["posted_kind"]} if posted["posted_kind"] else {})},
    }
    rec["company_key"] = company_key(rec["company_name"])
    rec["title_key"] = title_key(rec["title"])
    # 지문에는 원문 속성과 정규화 결과를 함께 넣습니다 → 정규화 규칙을 고친 뒤 replay하면 최신 관측 기준으로 갱신됩니다.
    attrs = {k: rec[k] for k in ("company_name", "title", "location_raw", "career_raw", "education", "employment_type",
                                 "deadline_raw", "tags_raw", "company_key", "title_key", "sido", "career_type",
                                 "career_min_yr", "career_max_yr", "deadline_at", "deadline_kind", "apply_start_date")}
    rec["attrs_sha"] = hashlib.sha1(json.dumps(attrs, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()
    return rec


def precision_rank(p: str | None) -> int:
    return _PRECISION_RANK.get(p or "none", 9)
