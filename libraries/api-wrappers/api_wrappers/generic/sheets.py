import pandas as pd


def get_product_data_from_sheets(sheet_url: str) -> pd.DataFrame:
    import requests
    response = requests.get(sheet_url)
    import pdb; pdb.set_trace()

    return pd.read_csv(sheet_url)
