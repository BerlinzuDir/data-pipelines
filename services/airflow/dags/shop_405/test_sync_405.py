from sync import product_pipeline, TRADER_ID
from api_wrappers.lozuka.lozuka_api.caller import BASE_URL
import responses
import urllib
import json


@responses.activate
def test_product_pipeline():
    responses.add_passthru("https://oauth2.googleapis.com/token")
    responses.add_passthru("https://sheets.googleapis.com")
    _setup_request_mocks()
    product_pipeline()
    assert len(responses.calls) == 2
    assert len(json.loads(responses.calls[1].request.body)["data"]["articles"]) == 322
    assert json.loads(responses.calls[1].request.body)["data"]["articles"][0] == FIRST_PRODUCT


def _setup_request_mocks() -> None:
    _mock_access_token_endpoint(access_token)
    _mock_post_articles_endpoint(TRADER_ID)


def _mock_access_token_endpoint(access_token) -> None:
    request_url = urllib.parse.urljoin(BASE_URL, "/auth/login")
    responses.add(
        responses.POST,
        request_url,
        match_querystring=True,
        body=access_token,
        status=200,
    )


def _mock_post_articles_endpoint(trader_id: int) -> None:
    endpoint = f"/import/v1/articles/import?trader={trader_id}&access_token={123456789}"
    request_url = urllib.parse.urljoin(BASE_URL, endpoint)
    responses.add(responses.POST, request_url, match_querystring=True, status=200)


access_token: bytes = (
    b'["{\\"access_token\\":\\"123456789\\",'
    b'\\"expires_in\\":7200,'
    b'\\"token_type\\":\\"bearer\\",'
    b'\\"scope\\":null,'
    b'\\"refresh_token\\":\\"111\\"}"]'
)

FIRST_PRODUCT = {
    "name": "Akazienhonig mit Sommertracht mild, flüßig",
    "itemNumber": "4030011750737",
    "category": ["42"],
    "priceBrutto": 6.99,
    "priceNetto": 6.53,
    "description": "",
    "vat": "7",
    "images": "https://catalog.stolitschniy.shop/static/img/products/normalized/4030011750737.jpg",
    "stock": None,
    "unitSection": {
        "weightUnit": "g",
        "weight": "500.0",
        "priceSection": {"price": 6.99, "vat": "7"},
        "variantSection": [],
        "ean": None,
    },
}
