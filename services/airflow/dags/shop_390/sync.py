import json

import pandas as pd
import pathlib
from api_wrappers.google.google_sheets import get_default_category_mapping
from api_wrappers.lozuka import post_articles
import ramda as R


TRADER_ID: str = "390"
FTP_ENDPOINT = "http://s739086489.online.de/bzd-bilder/bzd"
GOOGLE_SHEETS_ADDRESS = "1tz7VqWhnkWEM8Nul3Y7eKgMDXPrDmn2pjJHDp2Orzqk"
GOOGLE_DRIVE_ADDRESS = "1f4XBu-bUNtBrtC5dkkUwbvvxkF67yxWa"


def product_pipeline(products: json):
    return R.pipe(
        _from_json_records,
        _set_bruttopreis,
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
