from contextlib import contextmanager

import requests
import shutil
import pandas as pd
from typing import TypedDict
import paramiko
from time import sleep
import os
import ramda as R

from dags.helpers.decorators import clean_up
from api_wrappers.external.sheets import get_product_data_from_sheets


PRODUCTS_CSV_ENDPOINT = "https://catalog.stolitschniy.shop/private/2VNgFokABP/vendors/berlinzudir/export"


class FtpCredentials(TypedDict):
    username: str
    password: str
    hostname: str
    port: int


@clean_up
def load_images_to_sftp(store_id: int) -> pd.DataFrame:
    products = _load_product_data()
    R.pipe(
        _get_file_list,
        _file_difference_sftp(store_id),
        _download_all_files,
        _load_all_files_to_sftp(store_id),
    )(products)
    return products


def _load_product_data() -> pd.DataFrame:
    return get_product_data_from_sheets(PRODUCTS_CSV_ENDPOINT)


def _get_file_list(products: pd.DataFrame) -> pd.DataFrame:
    products = products.rename(columns={"pic": "link", "id": "title"})
    products["title"] = products["title"].astype(str) + ".jpg"
    return products[["link", "title"]]


@R.curry
def _file_difference_sftp(store_id: int, file_list: pd.DataFrame):
    file_list_sftp = _file_list_sftp(store_id)
    return file_list[~file_list["title"].isin(file_list_sftp)]


def _file_list_sftp(store_id: int):
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as client:
        file_list = client.listdir(f"bzd/{store_id}")
        for filename in file_list:
            client.remove(f"bzd/{store_id}/{filename}")
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


def _download_all_files(file_list: pd.DataFrame) -> pd.DataFrame:
    file_list[["link", "title"]].apply(lambda row: _download(row["link"], row["title"]), axis=1)
    return file_list


def _download(url: str, filename: str) -> None:
    while not os.path.isfile(filename):
        response = requests.get(url, stream=True)

        # Check if the image was retrieved successfully
        response.raise_for_status()
        # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
        response.raw.decode_content = True

        # Open a local file with wb ( write binary ) permission.
        with open(filename, "wb") as f:
            shutil.copyfileobj(response.raw, f)
        sleep(1)


@R.curry
def _load_all_files_to_sftp(store_id: int, file_list: pd.DataFrame):
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as sftp_client:
        file_list["title"].apply(lambda title: _load_single_image_to_sftp(sftp_client, store_id, title))


@R.curry
def _load_single_image_to_sftp(sftp_client, store_id: int, filename: str) -> str:
    if "bzd" not in sftp_client.listdir():
        sftp_client.mkdir("bzd/")
    if str(store_id) not in sftp_client.listdir("bzd"):
        sftp_client.mkdir(f"bzd/{store_id}/")
    sftp_client.put(filename, f"bzd/{store_id}/{filename}")
    return filename
