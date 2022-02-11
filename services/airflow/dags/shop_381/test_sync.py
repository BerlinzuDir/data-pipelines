from .sync import (
    product_pipeline,
    TRADER_ID,
    _map_product_category,
)
from api_wrappers.lozuka import BASE_URL
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
    '{"ID":{"0":"1","1":"2","2":"3","3":"4"},"Titel":{"0":"Malandra Crystal (500ml, 40%vol.)","1":"Malandra Oak (500ml,'
    ' 40%vol.)","2":"Malandra Set - Crystal & Oak (2x 500ml, 40%vol.)","3":"Malandra Tasting Set Single - Crystal + Oa'
    'k (2x 50ml, 40%vol.)"},"Beschreibung":{"0":"Malandra Crystal hat den urspr\\u00fcnglichen brasilianische Spirituos'
    "e.\\nHergestellt auf dem Land in Brasilien und destilliert aus dem frischen Saft von Zuckerrohr, ist dieser Maland"
    "ra frisch und sanft.\\n\\nMalandra Crystal verbrachte 6 (sechs) Monate in F\\u00e4ssern aus rostfreiem Stahl und 1"
    " (ein) Jahr in Holzf\\u00e4ssern aus Cariniana, einem einheimischen brasilianischen Baum, der keine Farbe, sondern"
    ' fruchtige Noten verleiht.\\n\\nErwarten Sie einen klaren Cacha\\u00e7a, mit Zuckerrohr- und Fruchtnoten.","1":"Ma'
    "landra Oak ist eine Kombination aus Brasilien und Deutschland.\\n\\nIn den l\\u00e4ndlichen Gebieten Brasiliens he"
    "rgestellt und aus dem frischen Saft des Zuckerrohrs destilliert, ist diese Malandra weich und rund.\\n\\nDie Malan"
    "dra Oak ist in zwei F\\u00e4ssern abgef\\u00fcllt. Sie verbrachte 6 (sechs) Monate in F\\u00e4ssern aus rostfreiem"
    " Stahl, 1 (ein) Jahr in Holzf\\u00e4ssern aus Cariniana (brasilianischer einheimischer Baum) und dann 1 (ein) Jahr"
    " in europ\\u00e4ischen Eichenf\\u00e4ssern. Da es sich nicht um eine Mischung handelt, hat die Malandra Oak eine d"
    "oppelte Schicht von Noten: die fruchtige Note des Cariniana-Holzfasses und Noten von Vanille, Kirsche und Eiche au"
    's dem europ\\u00e4ischen Eichenfass.\\n\\nWir empfehlen, ihn pur oder zu Cocktails und Longdrinks zu trinken.","2"'
    ':"Das Malandra Set wird mit einer Flasche Malandra Crystal (500mL, 40%vol., 79,98\\/L) und einer Flasche Malandra '
    "Oak (500mL, 40%vol., 91,98\\/L) geliefert.\\n\\nMalandra Crystal hat den urspr\\u00fcnglichen brasilianischen Geis"
    "t. Hergestellt auf dem Land in Brasilien und destilliert aus dem frischen Saft von Zuckerrohr, ist dieser Malandra"
    " frisch und sanft. Malandra Crystal verbrachte 6 (sechs) Monate in F\\u00e4ssern aus rostfreiem Stahl und 1 (ein) "
    "Jahr in Holzf\\u00e4ssern aus Cariniana, einem einheimischen brasilianischen Baum, der keine Farbe, sondern frucht"
    "ige Noten verleiht. Erwarten Sie einen klaren Cacha\\u00e7a, mit Zuckerrohr- und Fruchtnoten.\\n\\nMalandra Oak is"
    "t eine Kombination aus Brasilien und Europa. In den l\\u00e4ndlichen Gebieten Brasiliens hergestellt und aus dem f"
    "rischen Saft des Zuckerrohrs destilliert, ist diese Malandra weich und rund. Die Malandra Oak ist in zwei F\\u00e4"
    "ssern abgef\\u00fcllt. Sie verbrachte 6 (sechs) Monate in F\\u00e4ssern aus rostfreiem Stahl, 1 (ein) Jahr in Holz"
    "f\\u00e4ssern aus Cariniana (brasilianischer einheimischer Baum) und dann 1 (ein) Jahr in europ\\u00e4ischen Eiche"
    "nf\\u00e4ssern. Da es sich nicht um eine Mischung handelt, hat die Malandra Oak eine doppelte Schicht von Noten: d"
    "ie fruchtige Note des Cariniana-Holzfasses und Noten von Vanille, Kirsche und Eiche aus dem europ\\u00e4ischen Eic"
    'henfass. Wir empfehlen, ihn pur oder zu Cocktails und Longdrinks zu trinken.","3":"Das Malandra Tasting Set wird m'
    "it einer Flasche Malandra Crystal (50mL, 40%vol.),\\u00a0 einer Flasche Malandra Oak (50mL, 40%vol.) und ein Ebook"
    " mit Tasting Tips geliefert.\\n\\n> Malandra Crystal hat den urspr\\u00fcnglichen brasilianischen Geist. Hergestel"
    "lt auf dem Land in Brasilien und destilliert aus dem frischen Saft von Zuckerrohr, ist dieser Malandra frisch und "
    "sanft. Malandra Crystal verbrachte 6 (sechs) Monate in F\\u00e4ssern aus rostfreiem Stahl und 1 (ein) Jahr in Holz"
    "f\\u00e4ssern aus Cariniana, einem einheimischen brasilianischen Baum, der keine Farbe, sondern fruchtige Noten ve"
    "rleiht. Erwarten Sie einen klaren Cacha\\u00e7a, mit Zuckerrohr- und Fruchtnoten.\\n> Malandra Oak ist eine Kombin"
    "ation aus Brasilien und Europa. In den l\\u00e4ndlichen Gebieten Brasiliens hergestellt und aus dem frischen Saft"
    " des Zuckerrohrs destilliert, ist diese Malandra weich und rund. Die Malandra Oak ist in zwei F\\u00e4ssern abgef"
    "\\u00fcllt. Sie verbrachte 6 (sechs) Monate in F\\u00e4ssern aus rostfreiem Stahl, 1 (ein) Jahr in Holzf\\u00e4ss"
    "ern aus Cariniana (brasilianischer einheimischer Baum) und dann 1 (ein) Jahr in europ\\u00e4ischen Eichenf\\u00e4"
    "ssern. Da es sich nicht um eine Mischung handelt, hat die Malandra Oak eine doppelte Schicht von Noten: die fruch"
    "tige Note des Cariniana-Holzfasses und Noten von Vanille, Kirsche und Eiche aus dem europ\\u00e4ischen Eichenfass"
    ". Wir empfehlen, ihn pur oder zu Cocktails und Longdrinks zu trinken.\\n> Booklet \\u00fcber die Geschichte des C"
    "acha\\u00e7a, Produktionsmethoden, zur Alterung von Malandra verwendete H\\u00f6lzer, sensorische Bewertung, Paar"
    'ung von Malandra und Cocktailrezepten."},"Bruttopreis":{"0":"\\u20ac33,53","1":"\\u20ac38,57","2":"\\u20ac67,14",'
    '"3":"\\u20ac16,72"},"Mehrwertsteuer prozent":{"0":19,"1":19,"2":19,"3":19},"Ma\\u00dfeinheit":{"0":"ml","1":"ml",'
    '"2":"ml","3":"ml"},"Verpackungsgr\\u00f6\\u00dfe":{"0":500,"1":500,"2":1000,"3":100},"Kategorie":{"0":"Alkoholhal'
    'tige Getr\\u00e4nke","1":"Alkoholhaltige Getr\\u00e4nke","2":"Alkoholhaltige Getr\\u00e4nke","3":"Alkoholhaltige '
    'Getr\\u00e4nke"},"R\\u00fcckgabe M\\u00f6glich":{"0":"ja","1":"ja","2":"ja","3":"ja"},"K\\u00fchlpflichtig":{"0":'
    '"Raumtemperatur: 15-25\\u00b0C","1":"Raumtemperatur: 15-25\\u00b0C","2":"Raumtemperatur: 15-25\\u00b0C","3":"Raum'
    'temperatur: 15-25\\u00b0C"},"Produktbild \\n(Dateiname oder url)":{"0":"http:\\/\\/s739086489.online.de\\/bzd-bil'
    'der\\/bzd\\/381\\/1.jpg","1":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/bzd\\/381\\/2.jpg","2":"http:\\/\\/s'
    '739086489.online.de\\/bzd-bilder\\/bzd\\/381\\/3.jpg","3":"http:\\/\\/s739086489.online.de\\/bzd-bilder\\/bzd\\/3'
    '81\\/4.jpg"},"Bestand":{"0":3,"1":3,"2":3,"3":3},"Ma\\u00dfeinheit \\nf\\u00fcr Bestand":{"0":"stk","1":"stk","2"'
    ':"stk","3":"stk"},"GTIN\\/EAN":{"0":7898957893010,"1":7898957893003,"2":"7898957893010 and 7898957893003","3":"-"'
    '},"ISBN":{"0":"","1":"","2":"","3":""},"SEO \\nkeywords":{"0":"Cachaca, Pitu, pitu cocktails, pitu cocktail, cock'
    'tail mit pitu, pitu rezept, caipi, caipirinha, mojito, caipirinha selber machen","1":"","2":"","3":""},"SEO \\nBe'
    'schreibungstext":{"0":"","1":"","2":"","3":""},"SEO \\nSeitentitel":{"0":"Handcrafted, small batch, Brazilian Cac'
    'ha\\u00e7a","1":"","2":"","3":""}}'
)
