"""과제 02 — 부동산 실거래가 Silver (PySpark 정제 + UDF)

wait_bronze (ExternalTaskSensor: bronze_realestate_collect 같은 logical date 의 DAG run 성공 대기)
  → silver_spark_submit (BashOperator + spark-submit → scripts/silver_spark.py)

bronze 와 schedule/start_date/end_date 를 똑같이 맞춰야 logical date 가 일치해 센서가 매칭된다.
"""
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = os.environ.get("COLLECTOR_NAME", "한승수")
SCRIPT = os.path.join(os.environ.get("SPARK_SCRIPTS_DIR", "/opt/airflow/scripts"), "q2", "silver_spark.py")

with DAG(
    dag_id="silver_realestate_transform",
    description="과제02 S3 bronze XML → PySpark 정제·UDF·IQR → S3 silver parquet",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2024, 3, 31, tz="Asia/Seoul"),
    catchup=True,
    max_active_runs=1,
    default_args={"owner": OWNER, "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["silver", "realestate", "spark", "q2"],
) as dag:
    wait_bronze = ExternalTaskSensor(
        task_id="wait_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id=None,  # None = 태스크 하나가 아니라 DAG run 전체의 성공을 기다림
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",  # 기다리는 동안 워커 슬롯을 반납
        poke_interval=30,
        timeout=60 * 60,
    )

    silver_spark_submit = BashOperator(
        task_id="silver_spark_submit",
        bash_command=(
            "spark-submit --master 'local[*]' --name silver_realestate_transform "
            "--conf spark.ui.port=4040 "
            "--conf spark.hadoop.fs.s3a.endpoint.region=ap-northeast-2 "
            "--conf spark.sql.sources.partitionOverwriteMode=dynamic "
            f"{SCRIPT} "
            "--yyyymm {{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y%m') }} "
            "--bucket ${REALESTATE_BUCKET}"
        ),
    )

    wait_bronze >> silver_spark_submit
