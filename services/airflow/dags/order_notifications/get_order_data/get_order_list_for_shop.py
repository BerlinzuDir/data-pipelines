import json
from typing import Union, List
from datetime import date
import ramda as R
from datetime import datetime
from dags.order_notifications.get_order_data.endpoint_wrappers import (
    get_pickup_overview,
    authenticate,
)
import pandas as pd


def get_order_list(shop_id: str, order_date: date):
    return R.pipe(
        R.converge(
            get_pickup_overview,
            [
                lambda *_: authenticate(),
                R.always(50),
                R.head,
                R.pipe(R.nth(1), lambda d: d.day - datetime.today().day),
            ],
        ),
        lambda get_orders_for_delivery_time: R.map(get_orders_for_delivery_time, ["17:00", "19:00"]),
        R.reduce(
            lambda l, o: R.concat(l, R.prop("orders", o)),
            [],
        ),
    )([shop_id, order_date])


def parse_order(order: dict):
    item_list = R.pipe(
        R.prop("orderitem"),
        R.map(
            R.apply_spec(
                {
                    "Preis": R.prop("actual_price"),
                    "Menge": R.converge(
                        lambda a, b, c: str(a * b) + " " + c,
                        [
                            R.prop("amount"),
                            R.pipe(
                                R.path(["fkarticle", "unityAmount"]),
                                R.default_to(1),
                            ),
                            R.path(["fkarticle", "unity"]),
                        ],
                    ),
                    "Artikel": R.path(["fkarticle", "name"]),
                }
            ),
        ),
    )(order)

    order_df = pd.read_json(json.dumps(item_list))

    order_df["Kunde"] = order["fkUserId"]["firstname"] + " " + order["fkUserId"]["name"]
    order_df["Telefon"] = R.path(["fkUserId", "phone"], order)
    order_df["Bestell Nr."] = R.prop("id", order)

    return order_df


@R.curry
def get_orders_for_store(store_id: Union[int, str], order_date: datetime) -> List[pd.DataFrame]:
    return R.pipe(get_order_list, R.map(parse_order))(store_id, order_date)
