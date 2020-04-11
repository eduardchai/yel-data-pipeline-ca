
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

secrets = [
	Secret(
		deploy_type="volume",
		deploy_target="/etc/secrets",
		secret="common-svc-acc",
		key="sa-key.json")
]

default_args = {
    "owner": "eduard.chai",
    "start_date": datetime(2020, 3, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

dag = DAG(
    "feature_engineer",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    catchup=False,
    max_active_runs=1
)

feature_engineer = KubernetesPodOperator(
    namespace="spark-operator",
    task_id="feature-engineer",
    name="feature-engineer",
    image="edchai/feature_engineer:v1.1",
    image_pull_policy="IfNotPresent",
    arguments=["{{ ds }}"],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": "/etc/secrets/sa-key.json"
    },
    secrets=secrets,
    get_logs=True,
    dag=dag
)
