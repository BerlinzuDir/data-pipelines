import requests
import json
import pathlib
import ramda as R
from typing import TypedDict

BASE_URL = "https://siegen.lozuka.de/api/v1"


class OrdersAuthCredentials(TypedDict):
    grant_type: str
    client_id: str
    client_secret: str
    username: str
    password: str


class OrdersAPIToken(TypedDict):
    access_token: str


@R.curry
def get_dropoff_overview(token: OrdersAPIToken, order_id, delivery_time):
    return R.pipe(requests.get, R.prop("text"), json.loads)(
        f"{BASE_URL}/getDeliveryOverview"
        + f"?order={order_id}"
        + f"&deliveryTime=0,{delivery_time}"
        + f"&access_token={token['access_token']}"
    )


@R.curry
def get_pickup_overview(token: OrdersAPIToken, driver_id, trader_id, delivery_time):
    return R.pipe(requests.get, R.prop("text"), json.loads)(
        f"{BASE_URL}/getPickupOverview"
        + f"?driver={driver_id}"
        + f"&trader={trader_id}"
        + f"&deliveryTime=0,{delivery_time}"
        + f"&access_token={token['access_token']}"
    )


def get_tour_overview(token: OrdersAPIToken):
    return R.pipe(requests.get, R.prop("text"), json.loads)(
        f"{BASE_URL}/getDriversOverview" + "?selectedDay=0" + f"&access_token={token['access_token']}"
    )


def get_delivery_api_auth_token(auth_context: OrdersAuthCredentials) -> OrdersAPIToken:
    res = requests.post("https://siegen.lozuka.de/oauth/v2/token", data=auth_context)
    return json.loads(res.text)


def get_auth_context(auth_file: str) -> OrdersAuthCredentials:
    with open(_get_path_of_file() + auth_file) as f:
        auth_context = json.load(f)
    return auth_context


def _get_path_of_file() -> str:
    return str(pathlib.Path(__file__).parent.resolve())
