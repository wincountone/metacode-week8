"""과제 03 — 부동산 실거래가 Gold (집계 + PostgreSQL 적재)

wait_silver (ExternalTaskSensor: silver_realestate_transform 같은 logical date 성공 대기)
  → gold_spark_submit (BashOperator + spark-submit → scripts/gold_spark_sql.py, write.jdbc)
  → validate_gold_tables (PostgresHook: 5개 테이블 row count > 0)
"""
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = os.environ.get("COLLECTOR_NAME", "한승수")
SCRIPT = os.path.join(os.environ.get("SPARK_SCRIPTS_DIR", "/opt/airflow/scripts"), "q3", "gold_spark_sql.py")
GOLD_TABLES = [
    "gold_realestate_district_avg",
    "gold_realestate_top10",
    "gold_realestate_size_dist",
    "gold_realestate_age_avg",
    "gold_realestate_mom_change",
]


def validate_gold_tables():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="realestate_pg")
    empty = []
    for table in GOLD_TABLES:
        count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        print(f"{table:<32} {count:>5} rows")
        if count == 0:
            empty.append(table)
    if empty:
        raise ValueError(f"row count = 0: {empty}")


with DAG(
    dag_id="gold_realestate_aggregate",
    description="과제03 S3 silver → Spark SQL 5종 집계 → PostgreSQL gold 5 테이블",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2024, 3, 31, tz="Asia/Seoul"),
    catchup=True,
    max_active_runs=1,
    default_args={"owner": OWNER, "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["gold", "realestate", "postgres", "q3"],
) as dag:
    wait_silver = ExternalTaskSensor(
        task_id="wait_silver",
        external_dag_id="silver_realestate_transform",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    gold_spark_submit = BashOperator(
        task_id="gold_spark_submit",
        bash_command=(
            "spark-submit --master 'local[*]' --name gold_realestate_aggregate "
            "--conf spark.ui.port=4040 "
            "--conf spark.hadoop.fs.s3a.endpoint.region=ap-northeast-2 "
            f"{SCRIPT} "
            "--yyyymm {{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y%m') }} "
            "--bucket ${REALESTATE_BUCKET}"
        ),
    )

    validate = PythonOperator(task_id="validate_gold_tables", python_callable=validate_gold_tables)

    wait_silver >> gold_spark_submit >> validate
