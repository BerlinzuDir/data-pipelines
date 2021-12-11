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
    # TODO compare post request data
    assert len(responses.calls) == 2
    assert len(json.loads(responses.calls[1].request.body)["data"]["articles"]) == 4


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
    '{"ID":{"0":1330,"1":1593,"2":1578,"3":1459},"Titel":{"0":"Wei\\u00dfwein: Allegria Vino Bianco","1":"Wei\\u00dfwein: Falerio Saladini Pilastri","2":"Wei\\u00dfwein: Moncaro Bianco","3":"Wei\\u00dfwein: BIOVINUM Arien blanco"},"Beschreibung":{"0":"Herkunft: Italien.\\nAllegria Vino Bianco \\/ Italien, Umbrien \\/ Cuve \\/ trocken, frische Frucht von Cox Orange und Mirabelle bei s\\u00fcffig, knackiger S\\u00e4ure \\/ 12,0% vol \\/ 1L \\/ enth\\u00e4lt Sulfite.","1":"Herkunft: Italien.\\nMarken \\/ Rebsorte: Chardonnay, Trebbiano, Pecorino, Passerina \\/ trocken mit geschmeidigen Noten von Pfirsich, Kirsche mit floral kr\\u00e4utrigen Ankl\\u00e4ngen \\/ 13% vol \\/ 0,75 L (1L 10,13\\u20ac \\/ enth\\u00e4lt Sulfite.","2":"Herkunft: Italien.\\nMarken \\/ Rebsorten: Trebbiano, Passerina \\/ troclen, gr\\u00fcne Apfelschale mit leicht cremiger Walnuss bei vollmundiger Saftigkeit \\/ 12,5% vol \\/ 0,75 L (1L 10,40\\u20ac) \\/ enth\\u00e4lt Sulfite.","3":"Herkunft: Spanien.\\nLa Mancha \\/ Rebsorte: Arien \\/ trocken, fruchtig-frische Zitrus- und Apfelaromen mit geschmeidiger S\\u00e4ure \\/ \\/ 12,0% vol \\/ 0,75 L \\/ enth\\u00e4lt Sulfite."},"Bruttopreis":{"0":"\\u20ac7.40","1":"\\u20ac7.90","2":"\\u20ac8.60","3":"\\u20ac8.70"},"Mehrwertsteuer prozent":{"0":19,"1":19,"2":19,"3":19},"Ma\\u00dfeinheit":{"0":"l","1":"l","2":"l","3":"l"},"Verpackungsgr\\u00f6\\u00dfe":{"0":1.0,"1":0.75,"2":0.75,"3":0.75},"Kategorie":{"0":"Alkoholhaltige Getr\\u00e4nke","1":"Alkoholhaltige Getr\\u00e4nke","2":"Alkoholhaltige Getr\\u00e4nke","3":"Alkoholhaltige Getr\\u00e4nke"},"R\\u00fcckgabe M\\u00f6glich":{"0":"nein","1":"nein","2":"nein","3":"nein"},"K\\u00fchlpflichtig":{"0":"Raumtemperatur: 15-25\\u00b0C","1":"Raumtemperatur: 15-25\\u00b0C","2":"Raumtemperatur: 15-25\\u00b0C","3":"Raumtemperatur: 15-25\\u00b0C"},"Produktbild \\n(Dateiname oder url)":{"0":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/407\\/1330.jpg","1":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/407\\/1593.jpg","2":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/407\\/1578.jpg","3":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/407\\/1459.jpg"},"Bestand":{"0":"","1":"","2":"","3":""},"Ma\\u00dfeinheit \\nf\\u00fcr Bestand":{"0":"","1":"","2":"","3":""},"GTIN\\/EAN":{"0":"","1":"","2":"","3":""},"ISBN":{"0":"","1":"","2":"","3":""},"SEO \\nkeywords":{"0":"","1":"","2":"","3":""},"SEO \\nBeschreibungstext":{"0":"","1":"","2":"","3":""},"SEO \\nSeitentitel":{"0":"","1":"","2":"","3":""}}'
)
