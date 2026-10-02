# 직무별 채용 공고 ETL·EDA

2026-10-02 KST에 사람인·잡코리아·인크루트·링커리어를 관측한다. 직무는 백엔드(BE), 데이터 분석·사이언스(DA), 데이터 엔지니어(DE)다. API 키 없이 웹 원천을 완성한다는 사용자 결정을 반영했다.

## 결과물

- [ETL 노트북](../job_platform_etl.ipynb): 원천·페이지·고유키, 동시 요청·백오프, 원본/정제 계보, 실제 2회 적재, 중복 민감도, 품질·DAG 운영 증거.
- [EDA 노트북](../job_platform_eda.ipynb): 분석 단위, 결측·분포 판단, 목적 있는 SQL, 공고 변화·기업 집중도·지원 조건·원천 겹침·기술/역량·상관·IQR.
- [CareerRadar 웹 화면](../web/index.html): 밝은 테마, 전체 통합 공고와 모든 원천 링크, 검색·필터·정렬·페이지·CSV.
- [Notion 평가 기준과 증거 위치](../docs/NOTION-RUBRIC.md): 공개 원문을 재조회하여 평가표 일치를 확인했다. 원문은 노트북 1개, 최신 사용자 요청은 2개이며 두 파일 제출 허용 여부는 확인되지 않았다.

두 노트북은 각각 새 커널에서 실행하고 출력·그림을 저장했다. 전체 소스 복제, 단순 명사 후보 나열, 자료가 부족한 시차 상관 그림은 제외했다. 실제 점수는 평가자가 판단한다.

## 관측 범위와 해석

계획은 09:00~20:00의 10분 간격 67틱이다. 실제 첫 수집은 12:50이므로 오전 23틱은 좌절단으로 남긴다. 미래·진행 중·운영 중 누락을 구분하며 과거 관측을 사후 값으로 만들지 않는다. 20:00 전에 저장한 출력은 부분 자료다.

`raw_listing_page`와 최소 상세 필드는 원본 계층, `job_posting`은 플랫폼 자연키 마스터, `posting_snapshot`은 실행별 관측이다. 플랫폼 중복은 회사·제목·마감·지역 호환과 직무 충돌 제외로 추정하고 모든 링크를 보존한다. 분석은 `v_run_scope`의 완료 scheduled 실행을 같은 READ ONLY REPEATABLE READ 스냅샷으로 읽는다.

대표 지원 조건은 한 원천 공고에서 선택하고 기술·역량은 출처 합집합이다. 복수 직무는 각 직무 안에서 한 번씩 집계한다. 공고 수는 고용 인원이 아니며 살아남은 목록의 게시일 분포는 과거 수요 추이가 아니다. 이미지 상세와 사전 밖 기술 표현은 관측하지 못한다. 중복 통합과 키워드 검출의 한계는 노트북에 남겼다.

## 재실행

현재 검증 환경은 `/opt/anaconda3/bin/python`(Python 3.12)과 Jupyter `python3` 커널이다. 라이브러리 버전은 [requirements.txt](../requirements.txt)에 기록했다. 프로젝트 루트에서 다음을 실행한다.

```sh
PYTHONPATH=src /opt/anaconda3/bin/python scripts/run_notebook.py
PYTHONPATH=src /opt/anaconda3/bin/python scripts/operations.py
/opt/anaconda3/bin/python scripts/build_web.py
```

첫 명령은 ETL·EDA를 각각 별도 커널에서 실행한 뒤 통합본을 한 새 커널에서 전체 실행한다. 세 노트북이 모두 통과하면 저장하고, 실패하면 기존 결과물을 보존한다. 실제 웹 요청·배포는 하지 않는다. 마지막 명령은 CSV·운영 기록으로 독립 HTML을 만든다. 파일을 직접 열거나 `python -m http.server 8765`로 `http://localhost:8765/web/`를 열 수 있다. 외부 CDN은 필요하지 않다.

[notebook_validation.json](../reports/notebook_validation.json)은 오류·실행 순서·그림·해석·비밀 값 혼입 검사, [etl_validation.json](../reports/etl_validation.json)은 실제 ETL 시험, [analysis_validation.json](../reports/analysis_validation.json)은 SQL/pandas 독립 검증과 링크 전수 일치를 기록한다.

## 수집 운영과 자동 마무리

Airflow 3.3.1 LocalExecutor는 별도 Docker 스택에서 실행하며 UI는 `http://localhost:8080`이다. 기존 `db-pg`의 `job_platform_db`를 사용하고 KST를 설정했다. DB 재시작 정책은 `no`로 유지하며 중지된 DB를 자동 기동하지 않는다. 원천별 호스트 동시성 1·시작 간격 2초, robots 확인, 403 중단/30분 냉각, Retry-After를 적용한다. 담당자 연락처와 상세 본문 원문은 저장하지 않는다.

```sh
/opt/anaconda3/bin/python scripts/start_finalizer.py
```

중복 기동을 막는 독립 프로세스가 매시 운영 상태를 기록하고 20:00 슬롯의 Airflow 종료를 확인한다. 이후 DAG pause → 완료 원본 replay·품질 검증 → ETL·EDA·통합본 새 커널 실행 → CSV·웹 재빌드를 수행한다. [finalization.json](../reports/finalization.json)이 `complete`일 때만 최종 마무리가 확인된 것이다. 실패하면 `failed`와 이유를 남긴다. `waiting_for_20_00`은 수집 진행 중이다. 해당 날짜 종료 후에만 `scripts/finalize.py --final-now`로 재시도할 수 있다.

[operations.json](../reports/operations.json)은 슬롯별 상태·마지막 완료 전수·DB 정책·import 오류를, [recovery.json](../reports/recovery.json)은 13:50 구조화 학력 파싱 오류와 보존 원본 재실행을 기록한다.

## 새 환경 설치

[.env.example](../.env.example)을 `.env`로 복사해 실행 중인 PostgreSQL의 DSN을 넣는다. 비밀 값은 노트북·Git에 넣지 않는다. `JOBTREND_DSN`의 DB 이름은 `job_platform_db`로 한다.

```sh
python -m pip install -r requirements.txt
PYTHONPATH=src python scripts/bootstrap.py
docker compose -f airflow/docker-compose.yaml up -d
# Airflow 서비스가 healthy 상태가 된 뒤
PYTHONPATH=src python scripts/deploy.py
```

bootstrap은 전용 DB·스키마·원천 게이트를 초기화하고 Docker용 env가 없을 때만 만든다. 배포는 실행 중 DAG가 없는지 확인하고 패키지 후 DAG 순으로 교체한다. 현재 8080은 이 전용 스택이 사용 중이다. 다른 Airflow가 8080을 사용한다면 그 스택을 식별하고 포트 충돌을 먼저 해결해야 한다. 전용 스택을 내려 롤백할 때는 `docker compose ... down`만 사용하고 DB 볼륨은 보존한다.

기존 단일 노트북과 생성기는 `backups/notebook_split/`로 보관했다. 기본 제출물은 [통합본](../job_platform_integrated.ipynb)이며, ETL·EDA 분리본은 보조 자료다.

## 분리본을 유지하며 통합본만 생성

```sh
/opt/anaconda3/bin/python scripts/build_integrated_notebook.py
```

기존 두 노트북의 코드 셀을 ETL→EDA 순서로 가져와 한 새 커널에서 실행한다. 원본 두 파일은 수정하지 않고 전후 SHA-256을 비교한다. 코드·출력·그림·해석 검사를 통과한 경우에만 통합본을 교체한다. [통합 실행 기록](../reports/integrated_notebook_validation.json)에 실행 순서와 코드 보존, 분리본 파일 보존 결과를 남긴다. 통합본 실행은 저장 원본을 재처리하므로 `reports/`의 분석·시험 기록과 CSV를 갱신한다. 각 노트북의 자료 기준은 해당 출력에서 확인한다.
