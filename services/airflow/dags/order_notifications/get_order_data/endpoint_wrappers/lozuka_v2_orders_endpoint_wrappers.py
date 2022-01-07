import requests
import json
import ramda as R

from dags.order_notifications.get_order_data.endpoint_wrappers.get_lozuka_v2_auth import (
    OrdersAPIToken,
)

BASE_URL = "https://siegen.lozuka.de/api/v1"


@R.curry
def get_dropoff_overview(token: OrdersAPIToken, order_id, delivery_time):
    return R.pipe(requests.get, R.prop("text"), json.loads)(
        f"{BASE_URL}/getDeliveryOverview"
        + f"?order={order_id}"
        + f"&deliveryTime=0,{delivery_time}"
        + f"&access_token={token['access_token']}"
    )


@R.curry
def get_pickup_overview(
    token: OrdersAPIToken,
    driver_id,
    trader_id,
    distance_of_days_from_today,
    delivery_time,
):
    return R.pipe(requests.get, R.prop("text"), json.loads)(
        f"{BASE_URL}/getPickupOverview"
        + f"?driver={driver_id}"
        + f"&trader={trader_id}"
        + f"&deliveryTime={distance_of_days_from_today},{delivery_time}"
        + f"&access_token={token['access_token']}"
    )


def get_tour_overview(token: OrdersAPIToken):
    return R.pipe(requests.get, R.prop("text"), json.loads)(
        f"{BASE_URL}/getDriversOverview"
        + "?selectedDay=0"
        + f"&access_token={token['access_token']}"
    )
