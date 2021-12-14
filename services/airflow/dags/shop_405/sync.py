import json

import pandas as pd
import pathlib
from api_wrappers.google.google_sheets import get_default_category_mapping
from api_wrappers.lozuka.lozuka_api import post_articles, deactivate_products, get_articles
import ramda as R


FTP_ENDPOINT = "http://s739086489.online.de/bzd-bilder/bzd"
VARIANTS = [{"name": "Gebinde", "variant_values": [{"name": "Flasche", "price_column": "deposit"}]}]
TRADER_ID: str = "405"


def product_pipeline(products: json):
    R.pipe(
        _from_json_records,
        _translate_columns,
        _drop_na_values,
        _category_mapping,
        _set_beschreibung,
        _set_title,
        _set_image_url,
        _set_deposit,
        _post_products(_load_credentials("/shop-secrets.json"), TRADER_ID, VARIANTS),
    )(products)


def _from_json_records(products: str) -> pd.DataFrame:
    return pd.DataFrame.from_records(json.loads(products))


def _translate_columns(products: pd.DataFrame) -> pd.DataFrame:
    return products.rename(columns=_translation_dict_products())


def _drop_na_values(products: pd.DataFrame) -> pd.DataFrame:
    mandatory_fields = [
        "Kategorie",
        "ID",
        "Titel",
        "pic",
        "Bruttopreis",
        "Mehrwertsteuer prozent",
        "Maßeinheit",
        "Verpackungsgröße",
    ]
    return products.dropna(subset=mandatory_fields)


def _category_mapping(products: pd.DataFrame) -> pd.DataFrame:
    mapping = get_default_category_mapping()
    category_difference = set(products["Kategorie"].unique()).difference(_translation_dict_categories().keys())
    if category_difference:
        products = products[~products["Kategorie"].isin(list(category_difference))]
        # TODO: error logging and notification
    products["Kategorie"] = products["Kategorie"].apply(
        lambda category_name: _map_product_category(mapping, _translation_dict_categories()[category_name])
    )
    return products


@R.curry
def _map_product_category(mapping: pd.DataFrame, category_name: str) -> int:
    return R.pipe(
        lambda df: df[df["category_name"] == category_name].astype({"category_id": str}),
        lambda df: df["category_id"].values,
        R.if_else(lambda x: len(x) == 1, list, lambda: _raise_value_error("Invalid category")),
    )(mapping)


def _raise_value_error(message):
    raise ValueError(message)


def _set_beschreibung(products: pd.DataFrame) -> pd.DataFrame:
    products["Beschreibung"].fillna("", inplace=True)
    return products


def _set_title(products: pd.DataFrame) -> pd.DataFrame:
    products.loc[products["tags"].str.contains("mehrweg"), "Titel"] += " MEHRWEG"
    products.loc[products["tags"].str.contains("einweg"), "Titel"] += " EINWEG"
    products["Titel"] = products["Titel"].str.replace('"', "'")
    return products


def _set_deposit(products: pd.DataFrame) -> pd.DataFrame:
    products["deposit"] = 0
    products.loc[products["tags"].str.contains("p8"), "deposit"] = "0.08"
    products.loc[products["tags"].str.contains("p25"), "deposit"] = "0.25"
    return products


def _set_image_url(products: pd.DataFrame) -> pd.DataFrame:
    products["Produktbild \n(Dateiname oder url)"] = (
        f"{FTP_ENDPOINT}/{TRADER_ID}/" + products["ID"].astype(str) + ".jpg"
    )
    return products


def _load_credentials(filename: str):
    with open(_get_path_of_file() + filename) as secrets_json:
        return json.load(secrets_json)


def _get_path_of_file() -> str:
    return str(pathlib.Path(__file__).parent.resolve())


@R.curry
def _post_products(login_details: dict, trader_id: str, variants: list, products: pd.DataFrame):
    product_ids_on_platform = [article["articlenr"] for article in get_articles(login_details, trader_id)]
    to_be_deactivated = set(product_ids_on_platform).difference(set(products["ID"].astype(str).values))
    deactivate_products(login_details, trader_id, list(to_be_deactivated))
    post_articles(login_details, trader_id, variants, products)


def _translation_dict_products() -> dict:
    return {
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


def _translation_dict_categories() -> dict:
    return {
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
    from dotenv import load_dotenv

    load_dotenv()
    products = get_product_data_from_sheets(PRODUCTS_CSV_ENDPOINT)
    product_pipeline(products.to_json())
