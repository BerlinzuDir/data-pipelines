from datetime import datetime
from typing import List

import pandas as pd
import ramda as R
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

from dags.order_notifications.get_order_data import get_orders_for_store


def generate_email_task(dag, config, send=send_email):
    load_orders_and_send_email = R.pipe(
        datetime.now,
        get_orders_for_store(config["id"]),
        R.if_else(
            there_are_orders,
            R.pipe(
                generate_email_body_from_order_list(config),
                send_email_body(send, config),
            ),
            R.T,
        ),
    )

    return PythonOperator(
        task_id=f"send_order_email_to_{config['store_name']}".replace(" ", "_"),
        python_callable=load_orders_and_send_email,
        dag=dag,
    )


@R.curry
def send_email_body(send, config: dict, email_body: str) -> None:
    send(
        to=config["email"],
        html_content=email_body,
        subject="Bestellungen heute bei Berlinzudir",
    )


there_are_orders = R.pipe(
    R.length,
    lambda l: R.gt(l, 0),
)


@R.curry
def generate_email_body_from_order_list(
    config: dict, orders: List[pd.DataFrame]
) -> str:
    email_body = f"<h3>Hallo {config['name']}</h3><br> Anbei die Bestellungen für den heutigen Tag zur abholung ab 13:00<br>"
    for i, order in enumerate(orders):
        email_body += f"<h4> Bestellung No. {i+1}</h4>" + order.to_html().replace(
            "\n", ""
        )
    return email_body
