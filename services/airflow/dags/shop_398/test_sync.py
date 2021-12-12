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
    '{"ID":{"0":1,"1":2,"2":3,"3":4},"Titel":{"0":"Baklava Premium 1000 g","1":"Baklava Premium 500 g","2":"Baklava Premiu'
    'm 250 g","3":"Baklava 1000 g"},"Beschreibung":{"0":"Name:  \\u0628\\u0642\\u0644\\u0627\\u0648\\u0629 \\u0645\\u0634\
    \u0643\\u0644 \\u0641\\u0627\\u062e\\u0631\\n\\nHerkunft: Berlin, Deutschland.\\n\\nZutaten: Weizenmehl 18%, Salz 2%, '
    'Wasser 10%, Maisstr\\u00e4ke 10%, Planzenfett (Palmfett) 10%, Butter 6%, Pistanzien 12%, Cashewkerne 6%, Walnuss 6%, Z'
    'ucker 20%, Zitronens\\u00e4ure 1%. kann Spuren von Nussschalen entahlten.\\n\\nTip: Trocken lagern, vor W\\u00e4rme un'
    'd direktem Sonnenlicht sch\\u00fczen.","1":"Name:  \\u0628\\u0642\\u0644\\u0627\\u0648\\u0629 \\u0645\\u0634\\u0643\\u'
    '0644 \\u0641\\u0627\\u062e\\u0631\\n\\nHerkunft: Berlin, Deutschland.\\n\\nZutaten: Weizenmehl 18%, Salz 2%, Wasser 10'
    '%, Maisstr\\u00e4ke 10%, Planzenfett (Palmfett) 10%, Butter 6%, Pistanzien 12%, Cashewkerne 6%, Walnuss 6%, Zucker 20%'
    ', Zitronens\\u00e4ure 1%. kann Spuren von Nussschalen entahlten.\\n\\nTip: Trocken lagern, vor W\\u00e4rme und direkte'
    'm Sonnenlicht sch\\u00fczen.","2":"Name:  \\u0628\\u0642\\u0644\\u0627\\u0648\\u0629 \\u0645\\u0634\\u0643\\u0644 \\u0'
    '641\\u0627\\u062e\\u0631\\n\\nHerkunft: Berlin, Deutschland.\\n\\nZutaten: Weizenmehl 18%, Salz 2%, Wasser 10%, Maisst'
    'r\\u00e4ke 10%, Planzenfett (Palmfett) 10%, Butter 6%, Pistanzien 12%, Cashewkerne 6%, Walnuss 6%, Zucker 20%, Zitrone'
    'ns\\u00e4ure 1%. kann Spuren von Nussschalen entahlten.\\n\\nTip: Trocken lagern, vor W\\u00e4rme und direktem Sonnenl'
    'icht sch\\u00fczen.","3":"Name: \\u0628\\u0642\\u0644\\u0627\\u0648\\u0629 \\u0645\\u0634\\u0643\\u0644\\n\\nHerkunft:'
    ' Berlin, Deutschland.\\n\\nZutaten: Weizenmehl 18%, Salz 1%, Wasser 12%, Maisstr\\u00e4ke 10%, Palmfett 12%, Weizengri'
    'e\\u00df 10%, Kokos 1%, Glukosesirup 1%, Erdn\\u00fcsse 13%, Zucker 20%, Zitronens\\u00e4ure 1%, Rosen Aroma 0,5%, Ses'
    'ampaste 0,5%, Farbestoffe E131, E160a (AZO frei). kann Spuren von Pistazien, Mandeln und Walnuss entahlten.\\n\\nTip: '
    'Trocken lagern, vor W\\u00e4rme und direktem Sonnenlicht sch\\u00fczen."},"Bruttopreis":{"0":"$12.00","1":"$6.00","2":'
    '"$3.50","3":"$10.00"},"Mehrwertsteuer prozent":{"0":7,"1":7,"2":7,"3":7},"Ma\\u00dfeinheit":{"0":"g","1":"g","2":"g","'
    '3":"g"},"Verpackungsgr\\u00f6\\u00dfe":{"0":1000,"1":500,"2":250,"3":1000},"Kategorie":{"0":"S\\u00fc\\u00dfwaren  Sal'
    'zgeb\\u00e4ck","1":"S\\u00fc\\u00dfwaren  Salzgeb\\u00e4ck","2":"S\\u00fc\\u00dfwaren  Salzgeb\\u00e4ck","3":"S\\u00fc'
    '\\u00dfwaren  Salzgeb\\u00e4ck"},"R\\u00fcckgabe M\\u00f6glich":{"0":"nein","1":"nein","2":"nein","3":"nein"},"K\\u00f'
    'chlpflichtig":{"0":"Raumtemperatur: 15-25\\u00b0C","1":"Raumtemperatur: 15-25\\u00b0C","2":"Raumtemperatur: 15-25\\u00'
    'b0C","3":"Raumtemperatur: 15-25\\u00b0C"},"Produktbild \\n(Dateiname oder url)":{"0":"http:\\/\\/s739086489.online.de\
    \/bzd-bilder\\/bzd\\/398\\/1.jpg","1":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/bzd\\/398\\/2.jpg","2":"http:\\/\
    \/s739086489.online.de\\/bzd-bilder\\/bzd\\/398\\/3.jpg","3":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/bzd\\/398\
    \/4.jpg"},"Bestand":{"0":"","1":"","2":"","3":""},"Ma\\u00dfeinheit \\nf\\u00fcr Bestand":{"0":"","1":"","2":"","3":""}'
    ',"GTIN\\/EAN":{"0":"","1":"","2":"","3":""},"ISBN":{"0":"","1":"","2":"","3":""},"SEO Keywords":{"0":"Schoko, Schokola'
    'de Waffel, Keks, Biskuit, S\\u00fc\\u00dfwaren, S\\u00fc\\u00dfigkeit, Knabber, Kinder, Chio Chips, Butterkekse, Geb\\'
    'u00e4ck, Geb\\u00e4ckspezialit\\u00e4t, Tahini, Halva, Kokosb\\u00e4llchen, Dattelfinger, Sandteig, Sesamkekse, Namura'
    ', Teigtaschen, Butterkekse, ","1":"Schoko, Schokolade Waffel, Keks, Biskuit, S\\u00fc\\u00dfwaren, S\\u00fc\\u00dfigke'
    'it, Knabber, Kinder, Chio Chips, Butterkekse, Geb\\u00e4ck, Geb\\u00e4ckspezialit\\u00e4t, Tahini, Halva, Kokosb\\u00e'
    '4llchen, Dattelfinger, Sandteig, Sesamkekse, Namura, Teigtaschen, Butterkekse, ","2":"Schoko, Schokolade Waffel, Keks'
    ', Biskuit, S\\u00fc\\u00dfwaren, S\\u00fc\\u00dfigkeit, Knabber, Kinder, Chio Chips, Butterkekse, Geb\\u00e4ck, Geb\\'
    'u00e4ckspezialit\\u00e4t, Tahini, Halva, Kokosb\\u00e4llchen, Dattelfinger, Sandteig, Sesamkekse, Namura, Teigtaschen'
    ', Butterkekse, ","3":"Schoko, Schokolade Waffel, Keks, Biskuit, S\\u00fc\\u00dfwaren, S\\u00fc\\u00dfigkeit, Knabber,'
    ' Kinder, Chio Chips, Butterkekse, Geb\\u00e4ck, Geb\\u00e4ckspezialit\\u00e4t, Tahini, Halva, Kokosb\\u00e4llchen, Da'
    'ttelfinger, Sandteig, Sesamkekse, Namura, Teigtaschen, Butterkekse, "},"SEO \\nBeschreibungstext":{"0":"","1":"","2":'
    '"","3":""},"SEO \\nSeitentitel":{"0":"","1":"","2":"","3":""}}'
)
