import os
import pandas as pd


def post_process_product_data(data_directory, excluded_brands, excluded_categories) -> pd.DataFrame:
    products = _load_data(data_directory)
    return (
        products.pipe(
            _drop_missing_eans).pipe(
            _drop_corrupt_eans).pipe(
            _exclude_brands, excluded_brands=excluded_brands).pipe(
            _exclude_categories, excluded_categories=excluded_categories)
    )


def _load_data(data_directory: str) -> pd.DataFrame:
    df_list = []
    for file in os.listdir(data_directory):
        if '.csv' in file:
            df_list.append(pd.read_csv(os.path.join(data_directory, file)))
    return pd.concat(df_list, ignore_index=True)


def _drop_missing_eans(products: pd.DataFrame) -> pd.DataFrame:
    products_mask = products["Kategorie"].str.contains("Obst & Gemüse")
    products.loc[products_mask, "gtins/eans"] = products.loc[products_mask, "gtins/eans"].fillna("0")
    return products.loc[products["gtins/eans"].notnull()]


def _drop_corrupt_eans(products: pd.DataFrame) -> pd.DataFrame:
    corrupt_ean = products.apply(_mask_corrupt_ean, axis=1)
    return products.loc[corrupt_ean]


def _mask_corrupt_ean(row):
    ean = row["gtins/eans"]
    ean = ean.replace("'", '')
    ean = ean.replace('[', '')
    ean = ean.replace(']', '')
    try:
        eans = ean.split(',')
        for ean in eans:
            int(ean)
        return True
    except Exception:
        return False


def _exclude_brands(products: pd.DataFrame, excluded_brands: list) -> pd.DataFrame:
    products["Marke"] = products["Marke"].str.lower()
    return products.loc[~products["Marke"].isin(excluded_brands)]


def _exclude_categories(products: pd.DataFrame, excluded_categories: list) -> pd.DataFrame:
    products["Kategorie"] = products["Kategorie"].str.lower()
    for excluded_category in excluded_categories:
        products = products.loc[~products["Kategorie"].str.contains(excluded_category)]
    return products


if __name__ == '__main__':
    DATA_NAME = ""
    DATA_DIRECTORY = f'api_wrappers/metro/data/{DATA_NAME}'
    EXCLUDED_BRANDS = [
        "metro chef",
        "aro",
        "metro chef bio"
        "metro premium",
        "metro chef gourvenience",
    ]
    EXCLUDED_CATEGORIES_KEYWORDS = [
        "bier",
        "fleisch",
        "fisch",
        "wurst",
        "aufschnitt",
        "molkereiprodukte",
        "feinkost",
        "ostern",
        "wein",
    ]
    products = post_process_product_data(
        DATA_DIRECTORY,
        excluded_brands=EXCLUDED_BRANDS,
        excluded_categories=EXCLUDED_CATEGORIES_KEYWORDS
    )
    products.to_csv(f'products_{DATA_NAME}.csv')
