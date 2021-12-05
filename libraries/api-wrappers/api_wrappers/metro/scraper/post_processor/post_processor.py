import os

import pandas as pd


def post_process_product_data(data_directory, excluded_brands, excluded_categories) -> pd.DataFrame:
    products = _load_data(data_directory)
    return (
        products.pipe(
            _drop_missing_eans).pipe(
            _drop_corrupt_eans).pipe(
            _exclude_brands, excluded_brands=excluded_brands).pipe(
            _exclude_categories, excluded_categories=excluded_categories).pipe(
            _drop_duplicates
        )
    )


def remove_intersections(df_main: pd.DataFrame, df_sub: pd.DataFrame) -> pd.DataFrame:
    df_main_ids = set(df_main["Id"].values)
    df_sub_ids = set(df_sub["Id"].values)
    intersection = df_main_ids.intersection(df_sub_ids)
    return {
        "df_main": df_main[~df_main["Id"].isin(intersection)],
        "df_sub": df_sub,
    }


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


def _drop_duplicates(products: pd.DataFrame) -> pd.DataFrame:
    return products.drop_duplicates()


if __name__ == '__main__':
    DATA_NAMES = {"df_main": "filtered_by_category", "df_sub": "bio_query"}
    EXCLUDED_BRANDS = [
        "metro chef",
        "aro",
        "metro chef bio",
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

    df_dict = {}
    for key, name in DATA_NAMES.items():
        data_directory = f'api_wrappers/metro/data/{name}'
        df_dict[key] = post_process_product_data(
            data_directory,
            excluded_brands=EXCLUDED_BRANDS,
            excluded_categories=EXCLUDED_CATEGORIES_KEYWORDS
        )

    df_dict = remove_intersections(**df_dict)

    for key, name in DATA_NAMES.items():
        df_dict[key].to_csv(f'products_{name}.csv')
