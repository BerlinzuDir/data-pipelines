import ramda as R
import vcr

from dags.order_notifications.get_order_data.endpoint_wrappers.get_lozuka_v2_auth import (
    get_auth_context,
    get_delivery_api_auth_token,
)


@vcr.use_cassette("dags/order_notifications/get_order_data/endpoint_wrappers/cassettes/authentication_token.yaml")
def test_authentication_returns_token():

    auth_response = R.pipe(get_auth_context, get_delivery_api_auth_token)("/delivery_api_credentials.json")
    assert isinstance(auth_response["access_token"], str)
