from airflow.timetables.base import TimeRestriction
import pendulum,json
import jobtrend_dag

def verify():
    dag=jobtrend_dag.jobtrend_snapshot_10min()
    restriction=TimeRestriction(earliest=dag.start_date,latest=dag.end_date,catchup=True)
    last=None;ticks=[]
    for _ in range(100):
        info=dag.timetable.next_dagrun_info(last_automated_data_interval=last,restriction=restriction)
        if info is None:break
        ticks.append(info.run_after.in_timezone('Asia/Seoul').isoformat());last=info.data_interval
    assert len(ticks)==67,(len(ticks),ticks[:2],ticks[-2:])
    assert ticks[0].startswith('2026-10-02T09:00') and ticks[-1].startswith('2026-10-02T20:00')
    assert set(dag.task_ids)=={'open_run','extract_raw','transform_load','resolve_duplicates','check_quality','route','alert_and_fail','finish'}
    assert dag.get_task('transform_load').trigger_rule=='all_done'
    result={'ticks':len(ticks),'first':ticks[0],'last':ticks[-1],'tasks':dag.task_ids,'max_active_runs':dag.max_active_runs}
    print(json.dumps(result,ensure_ascii=False))
    return result
if __name__=='__main__':verify()
