from .sync import (
    product_pipeline,
    TRADER_ID,
    _map_product_category,
)
from api_wrappers.lozuka.lozuka_api.caller import BASE_URL
from api_wrappers.google.google_sheets import get_default_category_mapping
import responses
import urllib
import json
import pandas as pd


@responses.activate
def test_product_pipeline():
    responses.add_passthru("https://oauth2.googleapis.com/token")
    responses.add_passthru("https://sheets.googleapis.com")
    _setup_request_mocks()
    product_pipeline(FILE_LIST)
    assert len(responses.calls) == 2
    assert len(json.loads(responses.calls[1].request.body)["data"]["articles"]) == 2
    assert json.loads(responses.calls[1].request.body)["data"]["articles"][0] == FIRST_PRODUCT


def test_map_product_category_returns_correct_product_id():
    mapping = get_default_category_mapping()

    assert _map_product_category(mapping, "Alkoholhaltige Getränke") == ["51"]
    assert _map_product_category(mapping, "Obst Gemüse") == ["36"]


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
    "name": "Apfel - Pink Lady changed",
    "itemNumber": "1",
    "category": ["36"],
    "priceBrutto": 0.59,
    "priceNetto": 0.55,
    "description": "Süß und Knackig\nHerkunft: Spanien,\n\n"
    + "Tip: für längere Haltbarkeit, Äpfel und Bananen getrennt lagern.",
    "vat": "7",
    "images": "1.png",
    "stock": 10,
    "unitSection": {
        "weightUnit": "stk",
        "weight": "1",
        "priceSection": {"price": 0.59, "vat": "7"},
        "variantSection": [],
        "ean": "",
    },
}

FILE_LIST = pd.DataFrame.from_dict(
    {
        "link": {
            0: "https://drive.google.com/uc?id=1ym44i-TWgTHu5Ncd9XIQjS4UIKdCAOfa&export=download",
            1: "https://drive.google.com/uc?id=10ROgXCXo1uC9M8nLuiMdHe526fLTpKyd&export=download",
        },
        "title": {0: "2.jpeg", 1: "1.png"},
        "hash": {
            0: "15bdf97b0e2de0293ec02720e25144ec",
            1: "6de5e0a8daffcd6b92e180d703518639",
        },
    }
)
