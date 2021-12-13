from contextlib import contextmanager

import pandas as pd
from typing import TypedDict
import paramiko
import os
import ramda as R

from api_wrappers.google.google_drive import download_file_from_drive
from dags.helpers.decorators import cwd_cleanup
from api_wrappers.google import get_file_list_from_drive, get_product_data_from_sheets
from dags.shop_412.sync import GOOGLE_SHEETS_ADDRESS, GOOGLE_DRIVE_ADDRESS, TRADER_ID, FTP_ENDPOINT


class FtpCredentials(TypedDict):
    username: str
    password: str
    hostname: str
    port: int


@cwd_cleanup
def load_images_to_sftp(store_id: str) -> pd.DataFrame:
    products = _load_product_data()
    R.pipe(
        _get_file_list(GOOGLE_DRIVE_ADDRESS),
        _download_all_files,
        _load_all_files_to_sftp(store_id),
    )(products)
    return _set_image_url(products)


def _load_product_data() -> pd.DataFrame:
    return get_product_data_from_sheets(GOOGLE_SHEETS_ADDRESS)


@R.curry
def _get_file_list(google_drive_adress: str, products: pd.DataFrame) -> pd.DataFrame:
    file_list_drive = get_file_list_from_drive(google_drive_adress)
    file_list_drive = _standardize_image_format(file_list_drive, column="title")
    products = _standardize_image_format(products, column="Produktbild \n(Dateiname oder url)")
    products = _check_products_for_image_file(file_list_drive, products)
    products["ID"] = products["ID"].astype(str)
    return _get_file_id_filename(file_list_drive, products)


def _standardize_image_format(dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
    format_translation = {"JPG": "jpg", "jpeg": "jpg", "JPEG": "jpg", "PNG": "png"}
    for key, val in format_translation.items():
        dataframe[column] = dataframe[column].str.replace(key, val)
    return dataframe


def _check_products_for_image_file(file_list_drive: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    not_assigned_images_sheet = set(products["Produktbild \n(Dateiname oder url)"].values).difference(
        file_list_drive["title"].values
    )
    if not_assigned_images_sheet:
        # TODO: send notification and log warning
        return products[~products["Produktbild \n(Dateiname oder url)"].isin(not_assigned_images_sheet)]
    return products


def _get_file_id_filename(file_list: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    products = products.merge(file_list, how="left", left_on="Produktbild \n(Dateiname oder url)", right_on="title")
    products["sftp_filename"] = products["ID"] + products["title"].apply(lambda x: "." + x.split(".")[-1])
    return products[["id", "sftp_filename"]]


def _download_all_files(file_list: pd.DataFrame) -> pd.DataFrame:
    file_list[["id", "sftp_filename"]].apply(
        lambda row: download_file_from_drive(row["id"], row["sftp_filename"]), axis=1
    )
    return file_list


@R.curry
def _load_all_files_to_sftp(store_id: str, file_list: pd.DataFrame) -> pd.DataFrame:
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as sftp_client:
        file_list["sftp_filename"].apply(lambda filename: _load_single_image_to_sftp(sftp_client, store_id, filename))
    return file_list


def _load_sftp_credentials_from_env() -> FtpCredentials:
    return FtpCredentials(
        username=os.environ["FTP_USER"],
        password=os.environ["FTP_PASSWORD"],
        hostname=os.environ["FTP_HOSTNAME"],
        port=int(os.environ["FTP_PORT"]),
    )


@contextmanager
def _connect_to_sftp(credentials: FtpCredentials):
    transport = paramiko.Transport((credentials["hostname"], credentials["port"]))
    transport.connect(username=credentials["username"], password=credentials["password"])
    client = paramiko.SFTPClient.from_transport(transport)
    try:
        yield client
    finally:
        client.close()


@R.curry
def _load_single_image_to_sftp(sftp_client, store_id: str, filename: str) -> str:
    if "bzd" not in sftp_client.listdir():
        sftp_client.mkdir("bzd/")
    if store_id not in sftp_client.listdir("bzd"):
        sftp_client.mkdir(f"bzd/{store_id}/")
    sftp_client.put(filename, f"bzd/{store_id}/{filename}")
    return filename


def _set_image_url(products: pd.DataFrame) -> pd.DataFrame:
    products["Produktbild \n(Dateiname oder url)"] = (
        f"{FTP_ENDPOINT}/{TRADER_ID}/"
        + products["ID"].astype(str)
        + "."
        + products["Produktbild \n(Dateiname oder url)"].apply(lambda x: x.split(".")[-1].lower())
    )
    return products
