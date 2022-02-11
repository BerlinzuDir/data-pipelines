from datetime import timedelta
from pathlib import Path

import ramda as R
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from dags.helpers.dag_helpers import (
    slack_notifier_factory,
    create_slack_error_message_from_task_context,
)
from dags.generic_google_product_imports.sync_products.sync import product_pipeline
from dags.generic_google_product_imports.sync_images import (
    load_files_from_google_to_sftp,
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


def load_config_files(folder_name: str):
    files = sorted(Path(__file__).with_name(folder_name).glob("**/*"))
    return R.map(load_yaml_file, files)


def load_yaml_file(path) -> dict:
    with open(path, "r") as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)


dags = []

for config in load_config_files("config_files"):

    dag = DAG(
        f"shop_{config['trader_id']}_product_upload",
        default_args=default_args,
        description="Extract product data and image links from google and upload to lozuka api",
        schedule_interval=timedelta(days=1),
        start_date=days_ago(2),
        tags=["product import"],
        on_failure_callback=slack_notifier_factory(create_slack_error_message_from_task_context),
    )

    dags.append(dag)

    load_images = PythonOperator(
        task_id="load_images_from_ggl_to_ftp",
        python_callable=load_files_from_google_to_sftp,
        op_kwargs=config,
        dag=dag,
    )

    load_product_data = PythonOperator(
        task_id="load_product_data_to_lozuka",
        python_callable=product_pipeline,
        op_kwargs=config,
        dag=dag,
    )

    load_images >> load_product_data
