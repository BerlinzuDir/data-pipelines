import json

import pandas as pd
import pathlib
from api_wrappers.external.sheets import get_product_data_from_sheets
from api_wrappers.google.google_sheets import get_default_category_mapping
from api_wrappers.lozuka.lozuka_api import post_articles
import ramda as R


TRADER_ID = "405"
PRODUCTS_CSV_ENDPOINT = "https://catalog.stolitschniy.shop/private/2VNgFokABP/vendors/berlinzudir/export"
FTP_SERVER_URL = 'http://home739086481.1and1-data.host/bzd-bilder/405/'
FTP_ENDPOINT = "http://s739086489.online.de/bzd-bilder"
PRODUCTS_TRANSLATION_DICT = {
    "id": "ID",
    "name": "Titel",
    "description": "Beschreibung",
    "price": "Bruttopreis",
    "tax": "Mehrwertsteuer prozent",
    "unit": "Maßeinheit",
    "weight": "Verpackungsgröße",
    "category": "Kategorie",
    "Rückgabe Möglich": "Rückgabe Möglich",
    "Kühlpflichtig": "Kühlpflichtig",
    "Produktbild \n(Dateiname oder url)": "Produktbild \n(Dateiname oder url)",
    "Bestand": "Bestand",
    "Maßeinheit \nfür Bestand": "Maßeinheit \nfür Bestand",
    "GTIN/EAN": "GTIN/EAN",
    "ISBN": "ISBN",
    "SEO \nkeywords": "SEO \nkeywords",
    "SEO \nBeschreibungstext": "SEO \nBeschreibungstext",
    "SEO \nSeitentitel": "SEO \nSeitentitel",
}
CATEGORIES_TRANSLATION_DICT = {
    "suesswaren": "Süßwaren Salzgebäck",
    "spirituosen": "Alkoholhaltige Getränke",
    "konserven": "Konserven",
    "getreide": "Backwaren  Cerealien",
    "fisch": "Fisch Meeresfrüchte",
    "getraenke": "Erfrischungsgetränke",
    "teigwaren": "Süßwaren Salzgebäck",
    "molkerei": "Käse Milchprodukte",
    "kaviar": "Fisch Meeresfrüchte",
    "fleisch": "Fleisch- Wurstwaren",
    "drogerie": "Drogerie",
    "kaffe": "Kaffee Tee",
    "brot": "Backwaren Cerealien",
    "tee": "Kaffee Tee",
    "tk": "Tiefkühlkost",
}


def product_pipeline():
    return R.pipe(
        lambda *args: _load_product_data(),
        _transform_product_data,
        post_articles(_load_credentials("/shop-secrets.json"), TRADER_ID),
    )("")


def _load_credentials(filename: str):
    with open(_get_path_of_file() + filename) as secrets_json:
        return json.load(secrets_json)


def _get_path_of_file() -> str:
    return str(pathlib.Path(__file__).parent.resolve())


def _load_product_data() -> pd.DataFrame:
    return get_product_data_from_sheets(PRODUCTS_CSV_ENDPOINT)


def _transform_product_data(product_data: pd.DataFrame) -> pd.DataFrame:
    product_data = product_data.rename(columns=PRODUCTS_TRANSLATION_DICT)
    mapping = get_default_category_mapping()
    category_difference = set(product_data["Kategorie"].unique()).difference(CATEGORIES_TRANSLATION_DICT.keys())
    if category_difference:
        raise MissingCategoryTranslation(category_difference)
    product_data["Kategorie"] = product_data["Kategorie"].apply(
        lambda category_name: _map_product_category(mapping, CATEGORIES_TRANSLATION_DICT[category_name])
    )
    product_data["Beschreibung"].fillna("", inplace=True)
    product_data["Produktbild \n(Dateiname oder url)"] = (
        f"{FTP_ENDPOINT}/{TRADER_ID}/" + product_data["ID"].astype(str) + '.jpg'
    )
    return product_data


@R.curry
def _map_product_category(mapping: pd.DataFrame, category_name: str) -> int:
    return R.pipe(
        lambda df: df[df["category_name"] == category_name].astype({"category_id": str}),
        lambda df: df["category_id"].values,
        R.if_else(lambda x: len(x) == 1, list, lambda: raise_value_error("Invalid category")),
    )(mapping)


def raise_value_error(message):
    raise ValueError(message)


class MissingCategoryTranslation(Exception):
    """Raised when the input value is too small"""
    def __init__(self, category):
        message = f'Category "{category}" not in Translation Dict'
        super().__init__(message)


if __name__ == "__main__":
    product_pipeline()
