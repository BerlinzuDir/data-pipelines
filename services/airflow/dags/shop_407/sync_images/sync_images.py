from contextlib import contextmanager

import pandas as pd
from typing import TypedDict
import paramiko
import os
import ramda as R

from api_wrappers.google import get_product_data_from_sheets
from api_wrappers.google.google_drive import download_file_from_drive
from dags.helpers.decorators import cwd_cleanup
from api_wrappers.google import get_file_list_from_drive
from dags.shop_407.sync import GOOGLE_SHEETS_ADDRESS, GOOGLE_DRIVE_ADDRESS


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
        lambda df: df.to_dict(),  # return values have to be json serializable
    )(products)
    return products


def _load_product_data() -> pd.DataFrame:
    return get_product_data_from_sheets(GOOGLE_SHEETS_ADDRESS)


@R.curry
def _get_file_list(google_drive_adress: str, products: pd.DataFrame) -> pd.DataFrame:
    file_list_drive = get_file_list_from_drive(google_drive_adress)
    products = products.rename(columns={"ID": "title"})
    file_list_drive["title"] = file_list_drive["title"].str.replace('JPG', 'jpg')
    products["Produktbild \n(Dateiname oder url)"] = products["Produktbild \n(Dateiname oder url)"].str.replace('JPG', 'jpg').str.replace('PNG', 'png')
    not_assigned_images_sheet = set(products["Produktbild \n(Dateiname oder url)"].values).difference(file_list_drive["title"].values)
    not_assigned_images_drive = set(file_list_drive["title"].values).difference(products["Produktbild \n(Dateiname oder url)"].values)
    if not_assigned_images_sheet or not_assigned_images_drive:
        # TODO: send notification and log warning
        products = products[~products["Produktbild \n(Dateiname oder url)"].isin(not_assigned_images_sheet)]
    products["title"] = products["title"].astype(str)
    products = products.merge(file_list_drive, how='left', left_on="Produktbild \n(Dateiname oder url)", right_on='title')
    products["title"] = products["title_x"] + products["title_y"].apply(lambda x: '.' + x.split('.')[-1])
    return products[["id", "title"]]


def _download_all_files(file_list: pd.DataFrame) -> pd.DataFrame:
    file_list[["id", "title"]].apply(lambda row: download_file_from_drive(row["id"], row["title"]), axis=1)
    return file_list


@R.curry
def _load_all_files_to_sftp(store_id: str, file_list: pd.DataFrame) -> pd.DataFrame:
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as sftp_client:
        file_list["title"].apply(lambda title: _load_single_image_to_sftp(sftp_client, store_id, title))
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
