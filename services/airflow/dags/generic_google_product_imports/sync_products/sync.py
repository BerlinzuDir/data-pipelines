import json
import pathlib

import pandas as pd
import ramda as R

from api_wrappers.google import get_product_data_from_sheets
from api_wrappers.google.google_sheets import get_default_category_mapping
from api_wrappers.lozuka import post_articles, get_articles, deactivate_products
from dags.generic_google_product_imports.types import DagConfig
from strongtyping.strong_typing import match_typing


@match_typing
def product_pipeline(config: DagConfig):
    return R.pipe(
        _load_product_data,
        _transform_product_data(config["trader_id"], config["ftp_endpoint"]),
        _update_products(_load_credentials("/shop-secrets.json"), config["trader_id"]),
    )(config["google_sheets_id"])


def _load_product_data(google_sheets_address) -> pd.DataFrame:
    return get_product_data_from_sheets(google_sheets_address)


@R.curry
def _transform_product_data(trader_id: str, ftp_endpoint: str):
    return df_apply(
        {
            "Bruttopreis": parse_price_str,
            "Kategorie": _map_product_category(get_default_category_mapping()),
            "Produktbild \n(Dateiname oder url)": R.concat(f"{ftp_endpoint}/{trader_id}/"),
        }
    )


@R.curry
def _update_products(login_details: dict, trader_id: str, products: pd.DataFrame):
    product_ids_on_platform = [article["articlenr"] for article in get_articles(login_details, trader_id)]
    to_be_deactivated = set(product_ids_on_platform).difference(set(products["ID"].astype(str).values))
    deactivate_products(login_details, trader_id, to_be_deactivated)
    post_articles(login_details, trader_id, products)


@R.curry
def df_apply(applications: dict, df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    for key, applicator in applications.items():
        tmp[key] = tmp[key].apply(applicator)
    return tmp


parse_price_str = R.pipe(
    lambda val: val[1:],
    lambda val: val.replace(",", "."),
    lambda val: float(val),
)


def _load_credentials(filename: str):
    with open(_get_path_of_file() + filename) as secrets_json:
        return json.load(secrets_json)


def _get_path_of_file() -> str:
    return str(pathlib.Path(__file__).parent.resolve())


@R.curry
def _map_product_category(mapping: pd.DataFrame, category_name: str) -> int:
    return R.pipe(
        lambda df: df[df["category_name"] == category_name].astype({"category_id": str}),
        lambda df: df["category_id"].values,
        R.if_else(lambda x: len(x) == 1, list, lambda: raise_value_error("Invalid category")),
    )(mapping)


def raise_value_error(message):
    raise ValueError(message)
