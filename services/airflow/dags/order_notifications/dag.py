from airflow import DAG
from airflow.utils.dates import days_ago

from dags.order_notifications.email_to_store_task_generator import (
    generate_email_task,
)
from pathlib import Path
import ramda as R
import yaml

dag = DAG(
    dag_id="test_email",
    tags=["testing"],
    start_date=days_ago(1),
)

# TODO: implement emails to CROW


def load_config_files(folder_name: str):
    files = Path(__file__).with_name(folder_name).glob("**/*")
    return R.map(load_yaml_file, files)


def load_yaml_file(path) -> dict:
    with open(path, "r") as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)


email_tasks = []
for config in load_config_files("store_configs"):
    email_tasks.append(generate_email_task(dag, config))
