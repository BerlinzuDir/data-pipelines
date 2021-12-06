from .sync import product_pipeline, TRADER_ID
from api_wrappers.lozuka.lozuka_api.caller import BASE_URL
import responses
import urllib
import json
import pytest


@pytest.mark.block_network
@pytest.mark.vcr
@responses.activate
def test_product_pipeline():
    responses.add_passthru("https://oauth2.googleapis.com/token")
    responses.add_passthru("https://sheets.googleapis.com")

    _setup_request_mocks()

    product_pipeline(PRODUCTS_INPUT)

    assert len(responses.calls) == 2
    assert len(json.loads(responses.calls[1].request.body)["data"]["articles"]) == 2
    assert json.loads(responses.calls[1].request.body)["data"]["articles"][1] == FIRST_PRODUCT


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

PRODUCTS_INPUT = (
    '{"id":{"0":4030957751188,"1":4030011750737},"name":{"0":"Akazienhonig","1":"Akazienhonig mit Sommertracht mild, '
    'fl\\u00fc\\u00dfig"},"ru name":{"0":"\\u0410\\u043a\\u0430\\u0446\\u0438\\u0435\\u0432\\u044b\\u0439 \\u043c\\u04'
    '51\\u0434","1":"\\u041c\\u0451\\u0434 \\u0430\\u043a\\u0430\\u0446\\u0438\\u0435\\u0432\\u044b\\u0439"},"price":{'
    '"0":5.29,"1":6.99},"tax":{"0":7,"1":7},"weight":{"0":500.0,"1":500.0},"unit":{"0":"g","1":"g"},"category":{"0":"s'
    'uesswaren","1":"suesswaren"},"tags":{"0":"imported, online, wolt","1":"imported, online, wolt"},"supplier":{"0":"'
    'Monolith","1":"Monolith"},"pic":{"0":"https:\\/\\/catalog.stolitschniy.shop\\/static\\/img\\/products\\/normalize'
    'd\\/4030957751188.jpg","1":"https:\\/\\/catalog.stolitschniy.shop\\/static\\/img\\/products\\/normalized\\/4030011'
    '750737.jpg"},"description":{"0":null,"1":null}}')

FIRST_PRODUCT = {
    "name": "Akazienhonig mit Sommertracht mild, flüßig",
    "itemNumber": "4030011750737",
    "category": ["42"],
    "priceBrutto": 6.99,
    "priceNetto": 6.53,
    "description": "",
    "vat": "7",
    "images": "http://s739086489.online.de/bzd-bilder/405/4030011750737.jpg",
    "stock": None,
    "unitSection": {
        "weightUnit": "g",
        "weight": "500.0",
        "priceSection": {"price": 6.99, "vat": "7"},
        "variantSection": [],
        "ean": None,
    },
}
