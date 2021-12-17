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


def test_trader_id():
    assert isinstance(TRADER_ID, str)


@responses.activate
def test_product_pipeline():
    responses.add_passthru("https://oauth2.googleapis.com/token")
    responses.add_passthru("https://sheets.googleapis.com")
    _setup_request_mocks()
    product_pipeline(PRODUCTS_INPUT)
    assert len(responses.calls) == 2
    assert len(json.loads(responses.calls[1].request.body)["data"]["articles"]) == 4
    assert json.loads(responses.calls[1].request.body)["data"]["articles"][0] == PRODUCT_OUTPUT


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

PRODUCTS_INPUT = (
    '{"ID":{"0":"1","1":"2","2":"3","3":"4"},"Titel":{"0":"Zitronen Fino Kal 2-3","1":"Limetten","2":"Rote Grapefruit '
    'Star Ruby","3":"Satsumas Kal 2-3"},"Beschreibung":{"0":"Herkunft: Spanien\\nKlasse: II\\nVerband: EG-Bio \\n","1"'
    ':"Herkunft: Kolumbien\\nKlasse: II\\nVerband: EG-Bio \\n","2":"Herkunft: Spanien\\nKlasse: II\\nVerband: EG-Bio'
    ' \\n","3":"Herkunft: Griechenland\\nKlasse: II\\nVerband: EG-Bio\\n"},"Bruttopreis":{"0":"\\u20ac3,99","1":"\\u2'
    '0ac0,75","2":"\\u20ac1,49","3":"\\u20ac3,39"},"Mehrwertsteuer prozent":{"0":7,"1":7,"2":7,"3":7},"Ma\\u00dfeinhei'
    't":{"0":"kg","1":"stk","2":"stk","3":"kg"},"Verpackungsgr\\u00f6\\u00dfe":{"0":1,"1":1,"2":1,"3":1},"Verpackungsg'
    'r\\u00f6\\u00dfe (Verkauf)":{"0":"0,50","1":"1,00","2":"1,00","3":"0,50"},"Kategorie":{"0":"Obst Gem\\u00fcse","'
    '1":"Obst Gem\\u00fcse","2":"Obst Gem\\u00fcse","3":"Obst Gem\\u00fcse"},"R\\u00fcckgabe M\\u00f6glich":{"0":"nei'
    'n","1":"nein","2":"nein","3":"nein"},"K\\u00fchlpflichtig":{"0":"","1":" ","2":"","3":""},"Produktbild \\n(Datein'
    'ame oder url)":{"0":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/bzd\\/412\\/1.jpg","1":"http:\\/\\/s739086489'
    '.online.de\\/bzd-bilder\\/bzd\\/412\\/2.jpg","2":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/bzd\\/412\\/3.jp'
    'g","3":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/bzd\\/412\\/4.jpg"},"Bestand":{"0":"","1":"","2":"","3":""'
    '},"Ma\\u00dfeinheit \\nf\\u00fcr Bestand":{"0":"","1":"","2":"","3":""},"GTIN\\/EAN":{"0":"","1":"","2":"","3":""'
    '},"ISBN":{"0":"","1":"","2":"","3":""},"SEO \\nkeywords":{"0":"","1":"","2":"","3":""},"SEO \\nBeschreibungstext"'
    ':{"0":"","1":"","2":"","3":""},"SEO \\nSeitentitel":{"0":"","1":"","2":"","3":""}}'
)

PRODUCT_OUTPUT = {
    "name": "Bio Zitronen Fino Kal 2-3",
    "itemNumber": "1",
    "category": ["36"],
    "priceBrutto": 1.99,
    "priceNetto": 1.86,
    "description": "Herkunft: Spanien\nKlasse: II\nVerband: EG-Bio \n",
    "vat": "7",
    "images": "http://s739086489.online.de/bzd-bilder/bzd/412/1.jpg",
    "stock": "",
    "unitSection": {
        "weightUnit": "g",
        "weight": "500.0",
        "priceSection": {"price": 1.99, "vat": "7"},
        "variantSection": [],
        "ean": "",
    },
}
