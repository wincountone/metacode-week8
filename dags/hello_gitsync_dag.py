"""과제 04 — git-sync 동작 확인용 테스트 DAG (BashOperator 만 사용)

이 파일은 로컬 dags/ 폴더가 아니라 GitHub 레포(dags/)에만 있다.
Airflow on Kubernetes 목록에 보이고 실행에 성공하면 git-sync 로 배포된 것이다.
"""
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_gitsync",
    description="git-sync 로 배포됐는지 확인하는 테스트 DAG",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args={"owner": "한승수"},
    tags=["gitsync", "q4", "week8"],
) as dag:
    BashOperator(
        task_id="say_hello",
        bash_command='echo "hello from git-sync - $(date) - host=$(hostname) - by 한승수"',
    )
