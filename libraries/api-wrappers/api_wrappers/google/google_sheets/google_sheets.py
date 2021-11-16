import pandas as pd
import ramda as R
from returns.curry import curry
from returns.pipeline import pipe

from api_wrappers.google.shared import connect_to_service_account

DEFAULT_CATEGORY_MAPPING_SHEET = "1vlQEu1yFn535-paVcP16Ffm_kH4xRYDLkJw0OZMDBfM"


def get_product_data_from_sheets(
    sheet_address: str, credentials_file: str = "api-credentials.json"
) -> pd.DataFrame:
    return R.use_with(
        _get_sheet_with_account("Products"),
        [connect_to_service_account, R.identity],
    )(credentials_file, sheet_address)


def get_default_category_mapping(
    credentials_file: str = "api-credentials.json",
) -> pd.DataFrame:
    df = R.use_with(
        _get_sheet_with_account("Category mapping"),
        [connect_to_service_account, R.identity],
    )(credentials_file, DEFAULT_CATEGORY_MAPPING_SHEET)

    return df.astype({"category_id": int, "category_name": str})


@curry
def _get_sheet_with_account(
    worksheet_name: str, account, sheet_address: str
) -> pd.DataFrame:
    return pipe(
        _open_sheet(account),
        _select_worksheet(worksheet_name),
        _get_all_records,
        _as_dataframe,
    )(sheet_address)


@curry
def _open_sheet(account, sheet_address: str):
    return account.open_by_key(sheet_address)


@curry
def _select_worksheet(worksheet_name: str, sheet):
    return sheet.worksheet(worksheet_name)


def _get_all_records(worksheet):
    return worksheet.get_all_records()


def _as_dataframe(records):
    return pd.DataFrame(records)
