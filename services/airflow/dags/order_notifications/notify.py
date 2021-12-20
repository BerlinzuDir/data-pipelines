import json

import ramda as R
import requests

from dags.order_notifications.get_orders import get_pickup_overview, get_auth_context, get_delivery_api_auth_token


def notify(trader_id: str, recipient_url: str):
    orders = _get_orders(trader_id)
    _send_json(recipient_url, _format(orders))


def _get_orders(trader_id: str):
    token = R.pipe(get_auth_context, get_delivery_api_auth_token)("/delivery_api_credentials.json")
    return (
        get_pickup_overview(token, 50, trader_id, "17:00")["orders"]
        + get_pickup_overview(token, 50, trader_id, "19:00")["orders"]
    )


def _format(orders: dict):
    return {
        "orders": [
            {
                "id": order["id"],
                "address_delivery": {
                    "firstname_delivery": order["li_firstname"],
                    "name_delivery": order["li_name"],
                    "street_delivery": order["li_street"],
                    "streetno_delivery": order["li_streetno"],
                    "zip_delivery": order["li_plz"],
                    "city_delivery": order["li_city"]
                },
                "address_invoice": {
                    "firstname_invoice": order["re_firstname"],
                    "name_invoice": order["re_name"],
                    "street_invoice": order["re_street"],
                    "streetno_invoice": order["re_streetno"],
                    "zip_invoice": order["re_plz"],
                    "city_invoice": order["re_city"]
                },
                "articles": [
                    {
                        "id": article["fkarticle"]["articlenr"],
                        "name": article["fkarticle"]["name"],
                        "price_total": article["actual_price"],
                        "price_netto": article["fkarticle"]["priceNetto"],
                        "price_brutto": article["fkarticle"]["priceBrutto"],
                        "amount": article["amount"],
                    } for article in order["orderitem"]
                ]
            } for order in orders
        ]
    }


def _send_json(recipient_url, orders):
    requests.post(recipient_url, data=json.dumps(orders))
