from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dags.shop_398.sync import product_pipeline, TRADER_ID, GOOGLE_DRIVE_ADDRESS
from dags.shop_398.sync_images import load_files_from_google_to_sftp
from dags.helpers.dag_helpers import (
    slack_notifier_factory,
    create_slack_error_message_from_task_context,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jakob.j.kolb@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
}

dag = DAG(
    f"shop_{TRADER_ID}_product_upload",
    default_args=default_args,
    description="Extract product data and image links from google and upload to lozuka api",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=["example"],
    on_failure_callback=slack_notifier_factory(create_slack_error_message_from_task_context),
)

load_images = PythonOperator(
    task_id="load_images_from_ggl_to_ftp",
    python_callable=lambda *_: load_files_from_google_to_sftp(TRADER_ID, GOOGLE_DRIVE_ADDRESS),
    dag=dag,
)

load_product_data = PythonOperator(
    task_id="load_product_data_to_lozuka",
    python_callable=product_pipeline,
    dag=dag,
)


load_images >> load_product_data
