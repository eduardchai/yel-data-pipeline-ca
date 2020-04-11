
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

default_args = {
    "owner": "eduard.chai",
    "start_date": datetime(2020, 3, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

dag = DAG(
    "scrapper_job",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    catchup=True,
    max_active_runs=5
)

start_scrapper = KubernetesPodOperator(
    namespace="spark-operator",
    task_id="start-scrapping",
    name="start-scrapping",
    image="edchai/scrapper:v1.1",
    image_pull_policy="IfNotPresent",
    arguments=["--date", "{{ ds }}"],
    get_logs=True,
    dag=dag
)
