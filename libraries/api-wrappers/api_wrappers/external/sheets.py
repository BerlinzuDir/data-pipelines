import pandas as pd


def get_product_data_from_sheets(sheet_url: str) -> pd.DataFrame:
    return pd.read_csv(sheet_url)
