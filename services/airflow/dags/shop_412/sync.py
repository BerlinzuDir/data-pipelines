import json
import math

import numpy as np
import pandas as pd
import pathlib
from api_wrappers.google.google_sheets import get_default_category_mapping
from api_wrappers.lozuka.lozuka_api import post_articles
import ramda as R


TRADER_ID: str = "412"
FTP_ENDPOINT = "http://s739086489.online.de/bzd-bilder/bzd"
GOOGLE_SHEETS_ADDRESS = "1e6bvl7Rb075bjd1rN6X_hexiaawowSSNmIphG9pZp5g"
GOOGLE_DRIVE_ADDRESS = "1QT0oswEcSBrubzQQ3ysAzpAeu86vNr3t"


def product_pipeline(products: json):
    R.pipe(
        _from_json_records,
        _set_bruttopreis,
        _set_verpackungsgroesse,
        _set_titel,
        _category_mapping,
        post_articles(_load_credentials("/shop-secrets.json"), TRADER_ID),
    )(products)


def _from_json_records(products: str) -> pd.DataFrame:
    return pd.DataFrame.from_records(json.loads(products))


def _load_credentials(filename: str) -> dict:
    with open(_get_path_of_file() + filename) as secrets_json:
        return json.load(secrets_json)


def _get_path_of_file() -> str:
    return str(pathlib.Path(__file__).parent.resolve())


def _set_bruttopreis(products: pd.DataFrame) -> pd.DataFrame:
    products["Bruttopreis"] = products["Bruttopreis"].apply(
        lambda price: R.pipe(
            lambda val: val[1:],
            lambda val: val.replace(",", "."),
            lambda val: float(val),
        )(price)
    )
    products.loc[products["Verpackungsgröße (Verkauf)"] == "", "Verpackungsgröße (Verkauf)"] = np.nan
    products["Bruttopreis"] = (
        products["Verpackungsgröße (Verkauf)"].str.replace(",", ".").astype(float) * products["Bruttopreis"]
    )
    products["Bruttopreis"] = products["Bruttopreis"].apply(lambda x: math.ceil(x * 10 ** 1) / 10 ** 1 - 0.01)
    products = products.dropna(
        subset=[
            "Bruttopreis",
            "ID",
            "Kategorie",
            "Kühlpflichtig",
            "Maßeinheit",
            "Mehrwertsteuer prozent",
            "Produktbild \n(Dateiname oder url)",
            "Titel",
            "Verpackungsgröße",
            "Verpackungsgröße (Verkauf)",
        ]
    )
    return products


def _set_verpackungsgroesse(products: pd.DataFrame) -> pd.DataFrame:
    products["Verpackungsgröße"] = products["Verpackungsgröße (Verkauf)"].str.replace(",", ".").astype(float)
    return products


def _set_titel(products: pd.DataFrame) -> pd.DataFrame:
    products["Titel"] = "Bio " + products["Titel"]
    return products


def _category_mapping(products: pd.DataFrame) -> pd.DataFrame:
    mapping = get_default_category_mapping()
    products["Kategorie"] = products["Kategorie"].apply(
        lambda category_name: _map_product_category(mapping, category_name)
    )
    return products


@R.curry
def _map_product_category(mapping: pd.DataFrame, category_name: str) -> int:
    return R.pipe(
        lambda df: df[df["category_name"] == category_name].astype({"category_id": str}),
        lambda df: df["category_id"].values,
        R.if_else(lambda x: len(x) == 1, list, lambda: raise_value_error("Invalid category")),
    )(mapping)


def raise_value_error(message):
    raise ValueError(message)


if __name__ == "__main__":
    from api_wrappers.google.google_sheets import get_product_data_from_sheets
    from dags.shop_412.sync_images.sync_images import GOOGLE_SHEETS_ADDRESS
    from dotenv import load_dotenv

    load_dotenv()

    products = get_product_data_from_sheets(GOOGLE_SHEETS_ADDRESS)
    product_pipeline(products.to_json())
