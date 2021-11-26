import json

import pandas as pd
from typing import List
import pathlib
from api_wrappers.google import get_product_data_from_sheets
from api_wrappers.google.google_sheets import get_default_category_mapping
from api_wrappers.lozuka.lozuka_api import post_articles
import ramda as R


TRADER_ID = "287"
GOOGLE_SHEETS_ADDRESS = "1HrA07_T95T6OyL-T012tGF4F6ZuaHalzFmSTtYAyjpo"
GOOGLE_DRIVE_ADDRESS = "1lQ2dyF3bschhZIl4MdMZ-Bn0VmbEz5Qv"


def product_pipeline(file_list: dict):
    return R.pipe(
        pd.DataFrame.from_dict,
        _load_product_data,
        _transform_product_data,
        post_articles(_load_credentials("/shop-secrets.json"), TRADER_ID),
    )(file_list)


def _load_credentials(filename: str):
    with open(_get_path_of_file() + filename) as secrets_json:
        return json.load(secrets_json)


def _get_path_of_file() -> str:
    return str(pathlib.Path(__file__).parent.resolve())


def _load_product_data(file_list):
    return [
        get_product_data_from_sheets(GOOGLE_SHEETS_ADDRESS),
        file_list,
    ]


def _transform_product_data(product_data: List[pd.DataFrame]):
    products, file_list = product_data
    products["Bruttopreis"] = products["Bruttopreis"].apply(
        lambda price: R.pipe(
            lambda val: val[1:],
            lambda val: val.replace(",", "."),
            lambda val: float(val),
        )(price)
    )

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
