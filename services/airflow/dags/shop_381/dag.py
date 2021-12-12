from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from dags.airflow_fp import execute_push_df, pull_execute
from dags.shop_381.sync import product_pipeline, TRADER_ID
from dags.shop_381.sync_images import load_images_to_sftp
from dags.helpers.dag_helpers import (
    slack_notifier_factory,
    create_slack_error_message_from_task_context,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
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
    task_id="load_images_from_trader_to_ftp",
    python_callable=execute_push_df(
        "products",
        lambda *_: load_images_to_sftp(TRADER_ID),
    ),
    dag=dag,
)

load_product_data = PythonOperator(
    task_id="load_product_data_to_lozuka",
    python_callable=pull_execute("products", product_pipeline),
    dag=dag,
)


load_images >> load_product_data
