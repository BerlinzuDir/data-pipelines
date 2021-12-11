import json

import pandas as pd
import pathlib
from api_wrappers.google.google_sheets import get_default_category_mapping
from api_wrappers.lozuka.lozuka_api import post_articles
import ramda as R


TRADER_ID: str = "407"
FTP_ENDPOINT = "http://s739086489.online.de/bzd-bilder/bzd"
GOOGLE_SHEETS_ADDRESS = "1_wRovPxM810eGCseDIga-N5iI7dwrSnlsWrItw17O8c"
GOOGLE_DRIVE_ADDRESS = "1EgUTQ7p8jFGWzSBvTlwRMRkTpLa2ptez"


def product_pipeline(products: json):
    R.pipe(
        _from_json_records,
        _set_bruttopreis,
        _category_mapping,
        post_articles(_load_credentials("/shop-secrets.json"), TRADER_ID),
    )(products)


def _from_json_records(products: str) -> pd.DataFrame:
    return pd.DataFrame.from_records(json.loads(products))


def _load_credentials(filename: str):
    with open(_get_path_of_file() + filename) as secrets_json:
        return json.load(secrets_json)


def _get_path_of_file() -> str:
    return str(pathlib.Path(__file__).parent.resolve())


def _set_bruttopreis(products: pd.DataFrame):
    products["Bruttopreis"] = products["Bruttopreis"].apply(
        lambda price: R.pipe(
            lambda val: val[1:],
            lambda val: val.replace(",", "."),
            lambda val: float(val),
        )(price)
    )
    return products


def _category_mapping(products: pd.DataFrame):
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
