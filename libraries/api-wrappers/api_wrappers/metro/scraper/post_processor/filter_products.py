import os
import pandas as pd


def post_process_product_data(data_directory):
    products = _load_data(data_directory)
    return (
        products.pipe(_drop_missing_eans)
                .pipe(_drop_corrupt_eans)
            )


def _load_data(data_directory: str):
    df_list = []
    for file in os.listdir(data_directory):
        if '.csv' in file:
            df_list.append(pd.read_csv(os.path.join(data_directory, file)))
    return pd.concat(df_list, ignore_index=True)


def _drop_missing_eans(products: pd.DataFrame):
    return products.loc[products["gtins/eans"].notnull()]


def _drop_corrupt_eans(products: pd.DataFrame):
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


if __name__ == '__main__':
    DATA_DIRECTORY = 'api_wrappers/metro/data/'
    products = post_process_product_data(DATA_DIRECTORY)
    products.to_csv('bio.csv')
