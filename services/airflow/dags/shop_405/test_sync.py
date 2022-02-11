from .sync import product_pipeline, TRADER_ID
from api_wrappers.lozuka import BASE_URL
import responses
import urllib
import json
import pytest


def test_trader_id():
    assert isinstance(TRADER_ID, str)


@pytest.mark.block_network
@pytest.mark.vcr
@responses.activate
def test_product_pipeline():
    responses.add_passthru("https://oauth2.googleapis.com/token")
    responses.add_passthru("https://sheets.googleapis.com")

    _setup_request_mocks()

    product_pipeline(PRODUCTS_INPUT)

    assert len(responses.calls) == 6
    assert len(json.loads(responses.calls[5].request.body)["data"]["articles"]) == 4
    assert json.loads(responses.calls[5].request.body)["data"]["articles"][0] == FIRST_PRODUCT
    assert json.loads(responses.calls[3].request.body) == {"data": {"articles": [{"itemNumber": "12", "active": "0"}]}}


def _setup_request_mocks() -> None:
    _mock_access_token_endpoint(access_token)
    _mock_get_articles_endpoint(TRADER_ID)
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


def _mock_get_articles_endpoint(trader_id: int) -> None:
    endpoint = f"/import/v1/articles?trader={TRADER_ID}&access_token={123456789}"
    request_url = urllib.parse.urljoin(BASE_URL, endpoint)
    responses.add(
        responses.GET,
        request_url,
        match_querystring=True,
        status=200,
        body=b'{"data": [{"articlenr": "12", "name": "bla"}]}',
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
    '{"id":{"0":4030957751188,"1":4030011750737,"2":4850001270355,"3":4850001279907},"name":{"0":"Akazienhonig","1":"'
    'Akazienhonig mit Sommertracht mild, fl\\u00fc\\u00dfig","2":"Armenischer Brandy \\"Mane\\" 8 Jahre alt 40% vol."'
    ',"3":"Armenischer Brandy \\"SHERAM\\" 10 Jahre 40% vol. in Geschenkbox"},"ru name":{"0":"\\u0410\\u043a\\u0430\\u'
    '0446\\u0438\\u0435\\u0432\\u044b\\u0439 \\u043c\\u0451\\u0434","1":"\\u041c\\u0451\\u0434 \\u0430\\u043a\\u0430\\'
    'u0446\\u0438\\u0435\\u0432\\u044b\\u0439","2":"\\u0410\\u0440\\u043c\\u044f\\u043d\\u0441\\u043a\\u0438\\u0439 \\'
    'u043a\\u043e\\u043d\\u044c\\u044f\\u043a \\"\\u041c\\u0430\\u043d\\u044d\\" \\u0432\\u044b\\u0434\\u0435\\u0440\\'
    'u0436\\u043a\\u0430 8 \\u043b\\u0435\\u0442 40% \\u0430\\u043b\\u043a.","3":"\\u0410\\u0440\\u043c\\u044f\\u043d\\'
    'u0441\\u043a\\u0438\\u0439 \\u043a\\u043e\\u043d\\u044c\\u044f\\u043a \\"\\u0428\\u0415\\u0420\\u0410\\u041c\\""}'
    ',"price":{"0":5.29,"1":6.99,"2":16.99,"3":18.49},"tax":{"0":7,"1":7,"2":19,"3":19},"weight":{"0":500.0,"1":500.0'
    ',"2":0.5,"3":0.5},"unit":{"0":"g","1":"g","2":"l","3":"l"},"category":{"0":"suesswaren","1":"suesswaren","2":"sp'
    'irituosen","3":"spirituosen"},"tags":{"0":"imported, online, wolt","1":"imported, online, wolt","2":"imported, o'
    'nline, wolt","3":"imported, online, wolt"},"supplier":{"0":"Monolith","1":"Monolith","2":"Monolith","3":"Monolit'
    'h"},"pic":{"0":"https:\\/\\/catalog.stolitschniy.shop\\/static\\/img\\/products\\/normalized\\/4030957751188.jpg'
    '","1":"https:\\/\\/catalog.stolitschniy.shop\\/static\\/img\\/products\\/normalized\\/4030011750737.jpg","2":"ht'
    'tps:\\/\\/catalog.stolitschniy.shop\\/static\\/img\\/products\\/normalized\\/4850001270355.jpg","3":"https:\\/\\'
    '/catalog.stolitschniy.shop\\/static\\/img\\/products\\/normalized\\/4850001279907.jpg"},"description":{"0":null,'
    '"1":null,"2":null,"3":null}}'
)

FIRST_PRODUCT = {
    "name": "Akazienhonig",
    "itemNumber": "4030957751188",
    "category": ["42"],
    "priceBrutto": 5.29,
    "priceNetto": 4.94,
    "description": "",
    "vat": "7",
    "images": "http://s739086489.online.de/bzd-bilder/bzd/405/4030957751188.jpg",
    "stock": None,
    "unitSection": {
        "weightUnit": "g",
        "weight": "500.0",
        "priceSection": {"price": 5.29, "vat": "7"},
        "variantSection": [],
        "ean": None,
    },
}
