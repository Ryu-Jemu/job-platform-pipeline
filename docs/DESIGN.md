# CareerRadar light UI

`design:design-system-management`와 `design:design-critique` 지침을 적용했다.
첨부 HTML의 통합 공고 목록·검색·원천 링크 구조를 밝은 테마로 재구성한다.
첨부의 예시 공고와 수치, 원티드, 실시간 데몬·응답 시간 주장은 사용하지 않는다.

## 디자인 시스템

| 토큰 | 값 | 용도 |
|---|---|---|
| brand | `#6340cf` | 주요 행동, 선택 상태, 통합 수치 |
| brand-soft | `#f1edff` | 선택 배경 |
| ink | `#202631` | 제목, 공고명 |
| muted | `#637083` | 설명, 채용 조건 |
| faint | `#657184` | 보조 정보 |
| page / surface | `#f7f8fb` / `#ffffff` | 페이지 / 카드 |
| border | `#e5e8ef` | 구성요소 경계 |
| success | `#176d55` | 전수 기준 열림 |
| spacing | 8px 기준 | 카드, 입력, 목록 간격 |
| radius | 8px / 12px | 입력·버튼 / 카드 |

외부 폰트·CDN·차트 라이브러리는 사용하지 않는다. 운영체제 한국어 글꼴을
활용하고 CSS·JavaScript·실제 보고서 데이터를 HTML에 모두 내장한다.

메인 화면은 관측 시각 → 전체 데이터 규모 → 부분 관측 설명 → 검색 조건 →
실제 공고 목록 순서다. 관측 요약과 수집 상태는 실제 동작하는 탐색 메뉴다.
관측 요약 막대는 내장된 전체 스냅샷에서 계산되며 검색 필터에는 영향받지 않는다.
기술 차트·필터는 `skills`(technology)만 사용하며, `competencies`는 별도 역량 배열로
CSV에 보존한다.

## 구성요소와 상태

- 버튼: 기본 / hover / focus / disabled / 선택 상태를 구분한다.
- 필터: 회사·제목·기술 검색, BE/DA/DE, 원천, 지역, 경력, 마감 종류,
  관측 상태, 기술 키워드, 저장 공고를 지원한다. 검색어 여러 개는 AND 조건이다.
- 목록: 최근 관측·게시일·마감일·회사명·원천 수 정렬과 20/50/100개 페이지를 제공한다.
- 기술: 5개 이후 키워드는 접근 가능한 native details로 모두 열람할 수 있다.
- 저장: 브라우저 localStorage에 공고 ID만 저장하며 API·사용자 계정을 요구하지 않는다.
- 다운로드: 전체 통합 / 검색 결과 / 전체 원천 CSV를 HTML 데이터로 생성한다.
  UTF-8 BOM·인용부호 처리·스프레드시트 수식 이스케이프를 적용한다.
- 빈 결과: 안내와 필터 초기화 버튼을 제공한다.
- 모바일: 사이드바를 상단 탐색으로 바꾸고 테이블의 각 행을 읽기 순서대로 카드로 배치한다.

## 데이터의 의미

`reports/all_jobs.csv`, `reports/all_platform_postings.csv`, `reports/operations.json`
및 `reports/analysis_validation.json`을 입력으로 쓴다. 통합 ID 유일성, 원천 ID
집합의 일치, 링크 개수, HTTP(S) 주소를 빌드 시 검증한다.

지역·경력·학력·마감 조건은 통합 공고의 **대표 원천** 수집값이다. 다른
원천의 조건 합집합이나 제목에 대한 추정값이 아니다. 결측은 미확인으로 표시한다.
대표 원천 시·도(`sido`)를 지역 필터에 사용하고 공고에는 수집한 지역 원문을 보인다.
열린 상태는 원천별 마지막 완료 전수 스캔에 따른 관측 상태이며 현재 지원 가능
여부의 보증이 아니다. 수집 상태 메뉴와 상태 tooltip에 완료 전수 시각을 표시한다.

20:00 최종 슬롯 전에는 부분 관측을 상단에 명시한다. 최종 슬롯 후에도 수집
시작 전 결측 구간은 남는다고 설명한다. HTML은 자동 새로고침하는 서비스가 아닌
산출물 스냅샷이며 ETL·EDA 산출물을 재생성할 때 갱신한다.

## 접근성과 디자인 검토

텍스트는 색상만으로 상태를 전달하지 않는다. 각 입력에 접근 가능한 이름이 있으며
skip link, native select, 키보드 포커스, 현재 탐색·페이지의 `aria-current`,
저장 버튼의 `aria-pressed`, 상세 필터의 `aria-expanded`, 검색 결과의 live region을 제공한다.
애니메이션을 요구하지 않으며 reduced-motion 설정을 따른다.

직접 계산한 대비: brand/white **6.64:1**, muted/white **5.03:1**,
success/success-soft **5.63:1**. 실제 브라우저의 computedStyle을 읽고 각 화면에
배치된 직접 텍스트의 전경색·상위 배경색 대비를 계산해 일반 텍스트 **4.5:1**
미만을 교정했다. 검토 중 흐린 보조 텍스트, 상태 배지, 메뉴 수치와 긴 원천 ID의
줄바꿈 문제를 수정했다. 이는 렌더링된 텍스트의 색상 확인이며 전체 WCAG 인증을 의미하지 않는다.

첫 화면에서 데이터 규모와 관측 범위를 확인할 수 있고, 첫 검색 이후부터는 결과 수와
현재 페이지 범위를 함께 보여준다. 모바일에서는 공고명이 먼저 읽히고 원천 링크가
별도 행에 나온다. 숨겨진 알림·가짜 프로필·미동작 탭은 만들지 않는다.

## 실행과 검증

```sh
python scripts/build_web.py
python -m http.server 8766 --bind 127.0.0.1
```

`http://127.0.0.1:8766/reports/all_jobs.html` 또는 `web/index.html`을 연다.
HTML 파일만 직접 열어도 검색·필터·페이지·CSV 다운로드는 동작한다.
빌더 callable: `from scripts.build_web import build_web; build_web(root=PROJECT_ROOT)`.
실제 브라우저 검증 결과는 `web/validation/browser_validation.json`, 화면은
`web/validation/desktop.png`와 `web/validation/mobile.png`에 기록한다.
