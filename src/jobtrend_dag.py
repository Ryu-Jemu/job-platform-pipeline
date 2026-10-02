"""jobtrend_snapshot_10min — 채용 공고 10분 스냅샷 DAG.

노트북 job_platform_pipeline.ipynb가 %%writefile로 만들고 배포 게이트를 거쳐 airflow/dags/에 복사합니다(직접 수정 금지).
- 스케줄: 평일 09:00~19:50 10분 간격 + 20:00(Asia/Seoul) — MultipleCronTriggerTimetable
- 흐름: open_run → extract_raw(원천·질의 동시 수집, 원본 저장) → transform_load(정제·관측 적재)
        → resolve_duplicates(플랫폼 간 같은 공고 묶기) → check_quality → route → alert_and_fail | finish
        (14번 모듈 validated_pipeline의 '적재 후 검증 → 분기' 패턴)
- 최상위에서는 I/O를 하지 않습니다(dag-processor가 30초마다 파싱).
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.trigger import MultipleCronTriggerTimetable

from jobtrend import CODE_VERSION, collect, config, dedup, quality, runlog, transform

KST = "Asia/Seoul"
log = logging.getLogger(__name__)


def _dag_run():
    return get_current_context()["dag_run"]


def _run_kind(dr) -> str:
    kind = str(getattr(dr, "run_type", "") or "").lower()
    return "scheduled" if "scheduled" in kind else "manual"


def notify_failure(context) -> None:
    """Task가 재시도를 모두 쓰고 최종 실패했을 때만 호출됩니다(14번 모듈의 on_failure_callback 패턴)."""
    ti = context["task_instance"]
    log.error("[ALERT] %s.%s 최종 실패 run_id=%s", ti.dag_id, ti.task_id, ti.run_id)
    try:
        runlog.mark_failed(config.dsn(), ti.run_id, f"task {ti.task_id} failed")
    except Exception:  # noqa: BLE001
        log.exception("crawl_run 갱신 실패(무시)")


@dag(
    dag_id="jobtrend_snapshot_10min",
    schedule=MultipleCronTriggerTimetable("*/10 9-19 * * 1-5", "0 20 * * 1-5", timezone=KST),
    start_date=pendulum.datetime(2026, 10, 2, 9, 0, tz=KST),
    end_date=pendulum.datetime(2026, 10, 2, 20, 0, tz=KST),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=9),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=20), "on_failure_callback": notify_failure},
    tags=["jobtrend", "snapshot-10min"],
    doc_md=__doc__,
)
def jobtrend_snapshot_10min():
    @task(retries=2, retry_delay=timedelta(seconds=10), execution_timeout=timedelta(seconds=60))
    def open_run() -> dict:
        dr = _dag_run()
        kind = _run_kind(dr)
        run_after = getattr(dr, "run_after", None) or getattr(dr, "logical_date", None)
        info = runlog.open_run(config.dsn(), dr.run_id, runlog.slot_for(run_after, kind), kind, CODE_VERSION)
        log.info("open_run %s", info)
        return info

    @task(execution_timeout=timedelta(minutes=5))
    def extract_raw() -> dict:
        summary = collect.collect_run(config.dsn(), _dag_run().run_id)
        log.info("extract_raw %s", summary)
        return {k: summary[k] for k in ("elapsed_s", "ok_pages")}

    @task(trigger_rule="all_done", retries=1, retry_delay=timedelta(seconds=15), execution_timeout=timedelta(minutes=2))
    def transform_load() -> dict:
        result = transform.transform_load(config.dsn(), _dag_run().run_id)
        log.info("transform_load %s", result)
        return result

    @task(trigger_rule="all_done", retries=1, execution_timeout=timedelta(minutes=1))
    def resolve_duplicates() -> dict:
        result = dedup.resolve_duplicates(config.dsn())
        log.info("resolve_duplicates %s", result)
        return result

    @task(trigger_rule="all_done", retries=0, execution_timeout=timedelta(minutes=1))
    def check_quality() -> dict:
        try:
            result = quality.check_run(config.dsn(), _dag_run().run_id)
        except Exception as exc:  # noqa: BLE001 — 점검 자체가 실패해도 run 마감은 진행
            log.exception("check_quality 실패")
            result = {"severity": "critical", "failed": [f"check_error: {type(exc).__name__}"], "n_checks": 0}
        log.info("check_quality %s", result)
        return result

    @task.branch(retries=0)
    def route(q: dict) -> str:
        return "alert_and_fail" if q["severity"] == "critical" else "finish"

    @task(retries=0)
    def alert_and_fail(q: dict) -> None:
        runlog.finalize_run(config.dsn(), _dag_run().run_id, force="failed")
        raise RuntimeError(f"[ALERT][critical] {q['failed']}")

    @task(retries=1, execution_timeout=timedelta(seconds=90))
    def finish() -> dict:
        result = runlog.finalize_run(config.dsn(), _dag_run().run_id)
        log.info("finish %s", result)
        return {"status": result["status"]}

    q = check_quality()
    open_run() >> extract_raw() >> transform_load() >> resolve_duplicates() >> q
    route(q) >> [alert_and_fail(q), finish()]


jobtrend_snapshot_10min()
