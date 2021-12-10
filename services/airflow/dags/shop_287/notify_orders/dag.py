import pendulum
import os
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators import email
from dags.shop_287.sync_products.sync import TRADER_ID
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


def days_ago(n, hour=0, minute=0, second=0, microsecond=0):

    print(os.environ.get("AIRFLOW__AIRFLOW__CORE__DEFAULT_TIMEZONE"))
    tz = pendulum.tz.timezone("Europe/Berlin")
    today = dt.datetime.now(tz=tz).replace(
        hour=hour, minute=minute, second=second, microsecond=microsecond
    )
    return today - timedelta(days=n)


with DAG(
    f"shop_{TRADER_ID}_order_notification",
    default_args=default_args,
    description="Notify shop about the orders of the day",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2, hour=10),
    tags=["example"],
    on_failure_callback=slack_notifier_factory(
        create_slack_error_message_from_task_context
    ),
) as dag:

    email = email.EmailOperator(
        task_id="send_email",
        to="jakob.j.kolb@gmail.com",
        html_content="Hello Jakob, This is airflow",
        subject="[Airflow] Test shop notifications",
    )
