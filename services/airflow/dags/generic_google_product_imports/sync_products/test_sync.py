from .sync import (
    product_pipeline,
    _map_product_category,
)
from api_wrappers.lozuka import BASE_URL
from api_wrappers.google.google_sheets import get_default_category_mapping
import responses
import urllib
import json
from typing import Union


def test_trader_id():
    assert isinstance(TRADER_ID, str)


@responses.activate
def test_product_pipeline():
    responses.add_passthru("https://oauth2.googleapis.com/token")
    responses.add_passthru("https://sheets.googleapis.com")
    _setup_request_mocks(TRADER_ID, ACCESS_TOKEN)
    product_pipeline(
        "287",
        "1HrA07_T95T6OyL-T012tGF4F6ZuaHalzFmSTtYAyjpo",
        "http://s739086489.online.de/bzd-bilder",
    )
    assert len(responses.calls) == 6

    assert json.loads(responses.calls[3].request.body) == {"data": {"articles": [{"itemNumber": "12", "active": "0"}]}}

    assert "import" in responses.calls[5].request.url
    assert len(json.loads(responses.calls[5].request.body)["data"]["articles"]) == 2

    assert json.loads(responses.calls[5].request.body)["data"]["articles"][0] == FIRST_PRODUCT


def test_map_product_category_returns_correct_product_id():
    mapping = get_default_category_mapping()
    assert _map_product_category(mapping, "Alkoholhaltige Getränke") == ["51"]
    assert _map_product_category(mapping, "Obst Gemüse") == ["36"]


def _setup_request_mocks(trader_id: Union[str, int], token: bytes) -> None:
    _mock_access_token_endpoint(token)
    _mock_get_articles_endpoint(trader_id)
    _mock_post_articles_endpoint(trader_id)


def _mock_access_token_endpoint(token: bytes) -> None:
    request_url = urllib.parse.urljoin(BASE_URL, "/auth/login")
    responses.add(
        responses.POST,
        request_url,
        match_querystring=True,
        body=token,
        status=200,
    )


def _mock_get_articles_endpoint(trader_id: Union[str, int]) -> None:
    endpoint = f"/import/v1/articles?trader={trader_id}&access_token={123456789}"
    request_url = urllib.parse.urljoin(BASE_URL, endpoint)
    responses.add(
        responses.GET,
        request_url,
        match_querystring=True,
        status=200,
        body=b'{"data": [{"articlenr": "12", "name": "bla"}]}',
    )


def _mock_post_articles_endpoint(trader_id: Union[str, int]) -> None:
    endpoint = f"/import/v1/articles/import?trader={trader_id}&access_token={123456789}"
    request_url = urllib.parse.urljoin(BASE_URL, endpoint)
    responses.add(responses.POST, request_url, match_querystring=True, status=200)


TRADER_ID = "287"

ACCESS_TOKEN: bytes = (
    b'["{\\"access_token\\":\\"123456789\\",'
    b'\\"expires_in\\":7200,'
    b'\\"token_type\\":\\"bearer\\",'
    b'\\"scope\\":null,'
    b'\\"refresh_token\\":\\"111\\"}"]'
)

FIRST_PRODUCT = {
    "name": "Apfel - Pink Lady changed",
    "itemNumber": "1",
    "category": ["36"],
    "priceBrutto": 0.59,
    "priceNetto": 0.55,
    "description": "Süß und Knackig\nHerkunft: Spanien,\n\n"
    + "Tip: für längere Haltbarkeit, Äpfel und Bananen getrennt lagern.",
    "vat": "7",
    "images": "http://s739086489.online.de/bzd-bilder/287/1.png",
    "stock": 10,
    "unitSection": {
        "weightUnit": "stk",
        "weight": "1",
        "priceSection": {"price": 0.59, "vat": "7"},
        "variantSection": [],
        "ean": "",
    },
}
