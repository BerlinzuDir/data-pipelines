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
    product_pipeline()
    # TODO compare post request data
    assert len(responses.calls) == 2
    assert len(json.loads(responses.calls[1].request.body)["data"]["articles"]) == 168


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


def _mock_post_articles_endpoint(trader_id: str) -> None:
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
    '{"ID":{"0":1,"1":2,"2":3,"3":4},"Titel":{"0":"Al Amira Sonnenblumenkerne 250g","1":"Al-Khair Ghee pflanzlich 2kg",'
    '"2":"Al Raii Corned Beef 340 g","3":"Al Raii Labneh Balls mit  Minzgeschmack 725 g"},"Beschreibung":{"0":"Name: '
    "\\u0628\\u0632\\u0631 \\u062f\\u0648\\u0627\\u0631 \\u0627\\u0644\\u0634\\u0645\\u0633\\nHerkunft:Libanon. \\nZu"
    'taten:Sonnenblumenkerne, Salz, Maisst\\u00e4rke, Zitronens\\u00e4ure (E330).","1":"Name: \\u0627\\u0644\\u062e\\'
    "u064a\\u0631 \\u0633\\u0645\\u0646\\u0629 \\u0646\\u0628\\u0627\\u062a\\u064a\\u0629 \\u0646\\u0642\\u064a\\u062"
    "9\\nHerkunft: Syrien. \\nZutaten: Raffiniertes Palm\\u00f6l, Zusatzstoffe Beta-Carotin 0,0025%, Aromen: 0,012%, "
    "Antioxidans (0,01% von BHT + BHA). Vitamine w\\u00e4hrend des Produktionsprozesses: Vitamin A (2000 International"
    'e Einheiten\\/100 g), Vitamin D (200 Internationale Einheiten\\/100 g). ","2":"Name: \\u0644\\u062d\\u0645 \\u062'
    "8\\u0642\\u0631\\u064a \\u0643\\u0648\\u0631\\u0646\\u062f\\u0628\\u064a\\u0641 \\u0627\\u0644\\u0631\\u0627\\u06"
    "39\\u064a\\nHerkunft: Brasilien.  \\nZutaten: Rindfleisch 98,3%, (Rindfleisch 57%, Kopffleisch, Rindfleischherz),"
    ' Salz, Konservierungsmittel (Natiumnitrit E250).","3":"Name: \\u0644\\u0628\\u0646\\u0629 \\u0627\\u0644\\u0631\\'
    "u0627\\u0639\\u064a \\u0645\\u0639 \\u0627\\u0644\\u0646\\u0639\\u0646\\u0627\\u0639\\nHerkunft: Niederlande.  \\"
    'nZutaten: Kuhmilch, Salz, Starterkultur, Labaustauschstoff, Minze."},"Bruttopreis":{"0":"\\u20ac1,49","1":"\\u20a'
    'c7,49","2":"\\u20ac3,99","3":"\\u20ac5,00"},"Mehrwertsteuer prozent":{"0":7,"1":7,"2":7,"3":7},"Ma\\u00dfeinheit"'
    ':{"0":"g","1":"kg","2":"g","3":"g"},"Verpackungsgr\\u00f6\\u00dfe":{"0":250,"1":2,"2":340,"3":725},"Kategorie":{"'
    '0":"N\\u00fcsse Saaten","1":"\\u00d6l Essig Gew\\u00fcrze","2":"K\\u00e4se Milchprodukte","3":"K\\u00e4se Milchpr'
    'odukte"},"R\\u00fcckgabe M\\u00f6glich":{"0":"nein","1":"nein","2":"nein","3":"nein"},"K\\u00fchlpflichtig":{"0":'
    '"Raumtemperatur: 15-25\\u00b0C","1":"Raumtemperatur: 15-25\\u00b0C","2":"Raumtemperatur: 15-25\\u00b0C","3":"K\\u'
    '00fchlschranktemperatur: 2-8\\u00b0C"},"Produktbild \\n(Dateiname oder url)":{"0":"http:\\/\\/s739086489.online.d'
    'e\\/bzd-bilder\\/bzd\\/390\\/1.jpg","1":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/bzd\\/390\\/2.jpg","2":"h'
    'ttp:\\/\\/s739086489.online.de\\/bzd-bilder\\/bzd\\/390\\/3.jpg","3":"http:\\/\\/s739086489.online.de\\/bzd-bilde'
    'r\\/bzd\\/390\\/4.jpg"},"Bestand":{"0":"","1":"","2":"","3":""},"Ma\\u00dfeinheit \\nf\\u00fcr Bestand":{"0":"","'
    '1":"","2":"","3":""},"GTIN\\/EAN":{"0":"","1":"","2":"","3":""},"ISBN":{"0":"","1":"","2":"","3":""},"SEO \\nkeyw'
    'ords":{"0":"N\\u00fcsse, Saaten, Samen, Sonnenblumen, Erdn\\u00fcsse, Kerne, melonenkerne, Honigmelonenkerne, Ger'
    '\\u00f6stet, R\\u00f6sterei, Knabber, Knabbereien, Schalenfr\\u00fcchte, Haselnuss ","1":"\\u00d6l, Butter, Butte'
    "rschmalz, Butterreinfett, Fett, Creme, Ern\\u00e4hrung, Speisequark, Quark, Backstein, Butter, Milch, Milchproduk"
    "te, Laban, K\\u00e4se, Milcheiwei\\u00df, Pflanzen\\u00f6l Pflanzenfett, Distel\\u00f6l, Sonnenblumen\\u00f6l, Er"
    'dnuss\\u00f6l, Sesam\\u00f6l, Raps\\u00f6l, Oliven\\u00f6l, Lein\\u00f6l","2":"Lebensmittel, Ern\\u00e4hrung, Fle'
    "isch, H\\u00e4hnchen, H\\u00fchnerfleisch, H\\u00e4hnchenbrust, H\\u00e4hnchenschenkel, Keule, H\\u00e4hnchen-Unt"
    "erschenkel, Rindfleisch, Lammfleisch, Salami, Beef, Luncheon, Paniermehl, Hackfleisch, Rinderhack, Konserven, Wur"
    'st","3":"Speisequark, Quark, Backstein, Butter, Milch, Milchprodukte, Laban, K\\u00e4se, Ghee, Ern\\u00e4hrung"},'
    '"SEO \\nBeschreibungstext":{"0":"","1":"","2":"","3":""},"SEO \\nSeitentitel":{"0":"","1":"","2":"","3":""}}'
)
