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
    '{"ID":{"0":1,"1":2,"2":3,"3":4},"Titel":{"0":"Red Bull Energy Drink 250 ml","1":"Mandarin Veneto 330 ml","2":"Hot '
    'Blood Energy Drink 250 ml","3":"Peison Energy Drink 250 ml"},"Beschreibung":{"0":"Herkunft: \\u00d6sterreich.\\nZu'
    "taten: Wasser, Saccharose, Glucose, S\\u00e4ureregulator Natriumcitrate, Kohlens\\u00e4ure, Taurin (0,4%), Glucur"
    "onolacton (0,24%), Koffein (0,03%), Inosit, Vitamine (Niacin, Pantothens\\u00e4ure, B6,B12), Aroma, Farbstoffe (e"
    'infache Zuckerkul\\u00f6r, Riboflavin).","1":"Name: \\u0645\\u0646\\u062f\\u0631\\u064a\\u0646 \\u0641\\u064a\\u0'
    "646\\u062a\\u0648\\nHerkunft: T\\u00fcrkei.\\nZutaten: Wasser, Fruktose, Glukose, sirup, Kohlendioxid, S\\u00e4ure"
    "regulator (ZitronenS\\u00e4ure), Farbstoffe (schwarzes Karottensaftkonzentrat), Ammonium -sulfite- karamell), Arom"
    "en, Konservierungsmittel ( Natriumbenzoat).\\n\\nTip: Nach dem \\u00d6ffnen gek\\u00fchlt bei maximal 7\\u00b0C f"
    "\\u00fcr maximal 3 Tage in einem geschlossenen Beh\\u00e4lter aufbewahren. (Fleischerzeugnis nicht in der ge\\u00"
    'f6ffneten Konservendose aufbewahren).","2":"Herkunft: Deutschland.\\nZutaten: Wasser, Zucker, Dextrose, S\\u00e4u'
    "erungsmittel: Zitronens\\u00e4ure, Kohlens\\u00e4ure, Taurin (0,4%); S\\u00e4ueregulatoren: Natriumcitrate, Magne"
    "siumcarbonate, Aroma, Koffein (0,024), Farbstoffe: Ammonsulfiz-Zuckerkul\\u00f6r, Riboflavine; Vitamine: Niacin, "
    "Pantothens\\u00e4ure, VitaminB6, Vitamin B12; Inosit (0,01%).\\n\\nTip: Trocken und vor W\\u00e4rme gesch\\u00fct"
    'zt lagern.","3":"Herkunft: T\\u00fcrkei.\\nZutaten: Kohlens\\u00e4urehaltiges Wasser, Zucker, S\\u00e4ure (Zitron'
    "ens\\u00e4ure), Koffein, Taurin, Glucuronolacton, Inositol, nat\\u00fcrliche Farbstoffe (Karamell E150c, Riboflav"
    "in E101), naturidentisches Rote-Beere-Aroma, Konservierungsstoff (Natriumbonzoat, Kaliumsorbat), Niacin, Pamothen"
    's\\u00e4ure, Vitamin B6, Vitamin B12.\\n\\nTip: Trocken und vor W\\u00e4rme gesch\\u00fctzt lagern."},"Bruttoprei'
    's":{"0":"$1.00","1":"$1.00","2":"$1.00","3":"$1.00"},"Mehrwertsteuer prozent":{"0":7,"1":7,"2":7,"3":7},"Ma\\u00d'
    'feinheit":{"0":"ml","1":"ml","2":"ml","3":"ml"},"Verpackungsgr\\u00f6\\u00dfe":{"0":250.0,"1":330.0,"2":250.0,"3"'
    ':250.0},"Kategorie":{"0":"Erfrischungsgetr\\u00e4nke","1":"Erfrischungsgetr\\u00e4nke","2":"Erfrischungsgetr\\u00'
    'e4nke","3":"Erfrischungsgetr\\u00e4nke"},"R\\u00fcckgabe M\\u00f6glich":{"0":"nein","1":"nein","2":"nein","3":"ne'
    'in"},"K\\u00fchlpflichtig":{"0":"Raumtemperatur: 15-25\\u00b0C","1":"Raumtemperatur: 15-25\\u00b0C","2":"Raumtempe'
    'ratur: 15-25\\u00b0C","3":"Raumtemperatur: 15-25\\u00b0C"},"Produktbild \\n(Dateiname oder url)":{"0":"http:\\/\\/'
    's739086489.online.de\\/bzd-bilder\\/391\\/1.jpg","1":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/391\\/2.jpg",'
    '"2":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/391\\/3.jpg","3":"http:\\/\\/s739086489.online.de\\/bzd-bilder'
    '\\/391\\/4.jpg"},"Bestand":{"0":"","1":"","2":"","3":""},"Ma\\u00dfeinheit \\nf\\u00fcr Bestand":{"0":"","1":"","2'
    '":"","3":""},"GTIN\\/EAN":{"0":"","1":"","2":"","3":""},"ISBN":{"0":"","1":"","2":"","3":""},"SEO \\nkeywords":{"0'
    '":"Getr\\u00e4nke, Erfischungsgetr\\u00e4nke, Saft, Cola, Sirup, alkoholfrei, Fanta, Sprite, Wasser, Energy Drink"'
    ',"1":"Getr\\u00e4nke, Erfischungsgetr\\u00e4nke, Saft, Cola, Sirup, alkoholfrei, Fanta, Sprite, Wasser, Energy Dri'
    'nk","2":"Getr\\u00e4nke, Erfischungsgetr\\u00e4nke, Saft, Cola, Sirup, alkoholfrei, Fanta, Sprite, Wasser, Energy '
    'Drink","3":"Getr\\u00e4nke, Erfischungsgetr\\u00e4nke, Saft, Cola, Sirup, alkoholfrei, Fanta, Sprite, Wasser, Ener'
    'gy Drink, Malzgetr\\u00e4nk, Eistee, Eiskaffee"},"SEO \\nBeschreibungstext":{"0":"","1":"","2":"","3":""},"SEO \\n'
    'Seitentitel":{"0":"","1":"","2":"","3":""}}'
)
