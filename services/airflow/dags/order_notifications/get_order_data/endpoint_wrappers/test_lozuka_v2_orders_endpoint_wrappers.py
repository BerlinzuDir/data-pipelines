import ramda as R
import vcr
import json

from .lozuka_v2_orders_endpoint_wrappers import (
    get_tour_overview,
    get_pickup_overview,
    get_dropoff_overview,
)
from .get_lozuka_v2_auth import get_delivery_api_auth_token, get_auth_context

RECORD_MODE = "once"  # or "new_episodes"


@vcr.use_cassette("dags/order_notifications/get_order_data/endpoint_wrappers/cassettes/authentication_token.yaml")
@vcr.use_cassette(
    "dags/order_notifications/get_order_data/endpoint_wrappers/cassettes/tour_overview.yaml",
    record_mode=RECORD_MODE,
)
def test_drivers_overview_returns_json_with_stops():
    drivers_overview = R.pipe(get_auth_context, get_delivery_api_auth_token, get_tour_overview)(
        "/delivery_api_credentials.json"
    )
    pprint(drivers_overview)
    assert len(drivers_overview) == 2
    assert drivers_overview[0]["deliveryTime"] == "17:00"
    assert drivers_overview[1]["deliveryTime"] == "19:00"


@vcr.use_cassette("dags/order_notifications/get_order_data/endpoint_wrappers/cassettes/authentication_token.yaml")
@vcr.use_cassette(
    "dags/order_notifications/get_order_data/endpoint_wrappers/cassettes/pickup_overview.yaml",
    record_mode=RECORD_MODE,
)
def test_pickup_overview_returns_json_with_orders():
    token = R.pipe(get_auth_context, get_delivery_api_auth_token)("/delivery_api_credentials.json")
    # pprint(get_pickup_overview(token, 50, 287, 0, "17:00"))
    # pprint(get_pickup_overview(token, 50, 287, 0, "19:00"))
    # pprint(get_pickup_overview(token, 50, 409, 0, "17:00"))
    # pprint(get_pickup_overview(token, 50, 409, 0, "19:00"))
    pickup = get_pickup_overview(token, 50, 287, 0, "17:00")
    pprint(pickup)
    assert len(pickup["orders"]) == 2  # two orders to pick up
    assert len(pickup["orders"][0]["orderitem"]) == 2  # two items in first order
    assert pickup["orders"][0]["fkUserId"]["email"] == "jakob.j.kolb@gmail.com"  # email of first client


@vcr.use_cassette("dags/order_notifications/get_order_data/endpoint_wrappers/cassettes/authentication_token.yaml")
@vcr.use_cassette(
    "dags/order_notifications/get_order_data/endpoint_wrappers/cassettes/dropoff_overview.yaml",
    record_mode=RECORD_MODE,
)
def test_dropoff_overview_returns_json_dropoff_details():
    token = R.pipe(get_auth_context, get_delivery_api_auth_token)("/delivery_api_credentials.json")
    # pprint(get_dropoff_overview(token, 22776, "17:00"))
    # pprint(get_dropoff_overview(token, 22777, "17:00"))
    # pprint(get_dropoff_overview(token, 22778, "19:00"))
    dropoff = get_dropoff_overview(token, 22776, "17:00")
    assert len(dropoff["orderitem"]) == 3
    assert dropoff["li_firstname"] == "Jakob"
    assert dropoff["li_name"] == "Kolb"


def pprint(json_string):
    print(json.dumps(json_string, indent=4, sort_keys=True))
