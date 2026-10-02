# 채용 공고 파이프라인 실행 안내

사람인·잡코리아·인크루트·링커리어 웹 목록에서 백엔드(BE), 데이터 분석·사이언스(DA), 데이터 엔지니어(DE) 공고를 수집한다. API 키 없이 운영하며 PostgreSQL에 원본·정제 공고·시간별 관측을 분리해 저장한다.

## 노트북과 웹 화면

- [ETL 노트북](../job_platform_etl.ipynb): 원천과 페이지 수집, 백오프·동시 요청, 저장 구조, 반복 적재, 중복 통합과 품질·Airflow 운영을 다룬다.
- [EDA 노트북](../job_platform_eda.ipynb): 직무별 공고 구성, 기업 집중도, 지원 조건, 플랫폼 겹침, 기술·역량, 상관과 분포를 분석한다.
- [통합 노트북](../job_platform_integrated.ipynb): ETL→EDA를 한 커널에서 순서대로 실행하는 전체 작업 흐름이다.
- [CareerRadar](../web/index.html): 검색·필터·정렬·저장·CSV 다운로드를 제공하는 정적 웹 화면이다.

## 환경 설정과 초기 설치

Python과 Jupyter `python3` 커널, Docker, 실행 중인 `db-pg` PostgreSQL 컨테이너가 필요하다. 아래 명령은 프로젝트 루트에서 실행한다. [.env.example](../.env.example)을 `.env`로 복사하고 `JOBTREND_DSN`에 접속 정보를 넣는다. DB 이름은 `job_platform_db`를 사용하며 비밀번호는 로컬 `.env`에 둔다.

```sh
python -m pip install -r requirements.txt
mkdir -p reports
PYTHONPATH=src python scripts/bootstrap.py
docker compose -f airflow/docker-compose.yaml up -d
```

bootstrap은 전용 DB·스키마와 원천 접근 설정을 초기화한다. DB의 시간대는 `Asia/Seoul`이며 Docker용 접속 설정은 처음 설치할 때 생성한다. Airflow 서비스가 준비된 뒤 배포한다.

```sh
PYTHONPATH=src python scripts/deploy.py
```

Airflow UI는 `http://localhost:8080`이다. UI에서 DAG `jobtrend_snapshot_10min`의 일정과 설정을 확인하고 활성화한다. 다른 서비스가 8080을 쓰면 포트 충돌을 먼저 해결한다. 수집일은 [설정](../src/jobtrend/config.py)과 [DAG](../src/jobtrend_dag.py)에 정의되어 있으므로 다른 날짜로 운영할 때 함께 변경한다.

## 저장 자료로 재실행

```sh
PYTHONPATH=src python scripts/run_notebook.py
PYTHONPATH=src python scripts/operations.py
python scripts/build_web.py
```

첫 명령은 ETL·EDA를 각각 새 커널에서 실행한 뒤 통합본을 한 새 커널에서 실행한다. 세 파일이 모두 성공하면 결과를 저장하고, 실패하면 기존 파일을 보존한다. 노트북 실행은 저장 원본을 재정제하며 새 웹 요청이나 배포를 수행하지 않는다. 운영 점검 명령은 슬롯별 수집 상태를 출력하고 웹 빌드는 분석 자료를 HTML에 반영한다.

기존 ETL·EDA 파일을 유지하면서 통합본만 다시 만들려면 다음을 실행한다.

```sh
python scripts/build_integrated_notebook.py
```

웹 화면은 HTML 파일을 직접 열거나 로컬 서버로 확인할 수 있다.

```sh
python -m http.server 8765 --bind 127.0.0.1
```

브라우저에서 `http://127.0.0.1:8765/web/`를 연다. 화면은 내장된 자료를 보여주므로 새로운 관측은 노트북과 웹을 다시 생성할 때 반영된다.

## 수집 정책과 자동 마무리

일정은 평일 09:00~20:00 KST의 10분 간격이다. 최초·정각 실행은 전수, 나머지는 최신 목록의 증분 관측이다. 같은 호스트는 동시 요청 1개·시작 간격 2초를 적용하고 robots를 확인한다. 403은 중단·냉각하며 429는 Retry-After를 따른다. 신규 공고의 상세는 필요한 필드만 처리하고 담당자 연락처와 상세 본문 원문은 저장하지 않는다.

```sh
python scripts/start_finalizer.py
```

자동 마무리는 중복 기동을 막고 운영 상태를 점검한다. 설정된 20:00 실행 종료 후 DAG를 pause하고 완료 원본 재정제→품질 점검→세 노트북 실행→웹 재생성을 수행한다. 상태는 로컬 `reports/finalization.json`에서 확인한다. `waiting_for_20_00`은 종료 대기, `complete`는 마무리 성공, `failed`는 실패다. 설정된 종료 시각 이후 재시도는 다음 명령으로 수행한다.

```sh
PYTHONPATH=src python scripts/finalize.py --final-now
```

DB가 중지되면 자동으로 재시작하지 않는다. 스택을 내릴 때는 `docker compose -f airflow/docker-compose.yaml down`을 사용하고 DB 볼륨은 보존한다.

공고 수는 고용 인원이 아니다. 중복 통합과 키워드는 추정이며 원천 링크를 함께 확인한다. 목록이 제공하지 않는 값은 결측으로 남기고 관측하지 못한 시간은 사후 값으로 채우지 않는다. 각 노트북의 기준 시각과 부분 관측 여부는 출력에서 확인한다.
