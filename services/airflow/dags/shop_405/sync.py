import json

import pandas as pd
import pathlib
from api_wrappers.google.google_sheets import get_default_category_mapping
from api_wrappers.lozuka import post_articles, deactivate_products, get_articles
import ramda as R


FTP_ENDPOINT = "http://s739086489.online.de/bzd-bilder/bzd"
TRADER_ID = "405"


def product_pipeline(products: json):
    R.pipe(
        _from_json_records, _transform_product_data, _post_products(_load_credentials("/shop-secrets.json"), TRADER_ID)
    )(products)


def _from_json_records(products: str) -> pd.DataFrame:
    return pd.DataFrame.from_records(json.loads(products))


def _transform_product_data(product_data: pd.DataFrame) -> pd.DataFrame:
    product_data = product_data.rename(columns=_translation_dict_products())
    mapping = get_default_category_mapping()
    category_difference = set(product_data["Kategorie"].unique()).difference(_translation_dict_categories().keys())
    if category_difference:
        product_data = product_data[~product_data["Kategorie"].isin(list(category_difference))]
        # TODO: error logging and notification
    product_data["Kategorie"] = product_data["Kategorie"].apply(
        lambda category_name: _map_product_category(mapping, _translation_dict_categories()[category_name])
    )
    product_data["Beschreibung"].fillna("", inplace=True)
    product_data["Produktbild \n(Dateiname oder url)"] = (
        f"{FTP_ENDPOINT}/{TRADER_ID}/" + product_data["ID"].astype(str) + ".jpg"
    )
    return product_data


@R.curry
def _map_product_category(mapping: pd.DataFrame, category_name: str) -> int:
    return R.pipe(
        lambda df: df[df["category_name"] == category_name].astype({"category_id": str}),
        lambda df: df["category_id"].values,
        R.if_else(lambda x: len(x) == 1, list, lambda: _raise_value_error("Invalid category")),
    )(mapping)


def _raise_value_error(message):
    raise ValueError(message)


def _load_credentials(filename: str):
    with open(_get_path_of_file() + filename) as secrets_json:
        return json.load(secrets_json)


def _get_path_of_file() -> str:
    return str(pathlib.Path(__file__).parent.resolve())


@R.curry
def _post_products(login_details: dict, trader_id: str, products: pd.DataFrame):
    product_ids_on_platform = [article["articlenr"] for article in get_articles(login_details, trader_id)]
    to_be_deactivated = set(product_ids_on_platform).difference(set(products["ID"].astype(str).values))
    deactivate_products(login_details, trader_id, to_be_deactivated)
    post_articles(login_details, trader_id, products)


def _translation_dict_products() -> dict:
    return {
        "id": "ID",
        "name": "Titel",
        "description": "Beschreibung",
        "price": "Bruttopreis",
        "tax": "Mehrwertsteuer prozent",
        "unit": "Ma??einheit",
        "weight": "Verpackungsgr????e",
        "category": "Kategorie",
        "R??ckgabe M??glich": "R??ckgabe M??glich",
        "K??hlpflichtig": "K??hlpflichtig",
        "Produktbild \n(Dateiname oder url)": "Produktbild \n(Dateiname oder url)",
        "Bestand": "Bestand",
        "Ma??einheit \nf??r Bestand": "Ma??einheit \nf??r Bestand",
        "GTIN/EAN": "GTIN/EAN",
        "ISBN": "ISBN",
        "SEO \nkeywords": "SEO \nkeywords",
        "SEO \nBeschreibungstext": "SEO \nBeschreibungstext",
        "SEO \nSeitentitel": "SEO \nSeitentitel",
    }


def _translation_dict_categories() -> dict:
    return {
        "suesswaren": "S????waren Salzgeb??ck",
        "spirituosen": "Alkoholhaltige Getr??nke",
        "konserven": "Konserven",
        "getreide": "Backwaren  Cerealien",
        "fisch": "Fisch Meeresfr??chte",
        "getraenke": "Erfrischungsgetr??nke",
        "teigwaren": "S????waren Salzgeb??ck",
        "molkerei": "K??se Milchprodukte",
        "kaviar": "Fisch Meeresfr??chte",
        "fleisch": "Fleisch- Wurstwaren",
        "drogerie": "Drogerie",
        "kaffe": "Kaffee Tee",
        "brot": "Backwaren Cerealien",
        "tee": "Kaffee Tee",
        "tk": "Tiefk??hlkost",
        "fischkonserven": "Konserven",
    }


class MissingCategoryTranslation(Exception):
    """Raised when the input value is too small"""

    def __init__(self, category):
        message = f'Categories "{category}" not in Translation Dict'
        super().__init__(message)


if __name__ == "__main__":
    from api_wrappers.external.sheets import get_product_data_from_sheets
    from dags.shop_405.sync_images.sync_images import PRODUCTS_CSV_ENDPOINT

    products = get_product_data_from_sheets(PRODUCTS_CSV_ENDPOINT)
    product_pipeline(products.to_json())
