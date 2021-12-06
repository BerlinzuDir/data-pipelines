"""
if not os.path.isdir("405"):
    os.mkdir("405")
for idx, row in products.iterrows():
    if os.path.isfile(f'405/{row["id"]}.jpg'):
        continue
    response = requests.get(row["pic"], stream=True)
    response.raw.decode_content = True
    with open(f'405/{row["id"]}.jpg', 'wb') as f:
        shutil.copyfileobj(response.raw, f)
"""

from contextlib import contextmanager
import requests
import shutil
import pandas as pd
from typing import TypedDict, List
import paramiko
from time import sleep
import os
import ramda as R


class FtpCredentials(TypedDict):
    username: str
    password: str
    hostname: str


def load_images_to_sftp(products_df: pd.DataFrame, store_id: int) -> pd.DataFrame:
    R.pipe(
        _get_file_list,
        _download_all_files,
        _load_all_files_to_sftp(store_id),
    )(products_df)
    return products_df


def _get_file_list(products_df: pd.DataFrame) -> pd.DataFrame:
    products_df["ID"] += ".jpg"
    products_df = products_df.rename(columns={"Produktbild \n(Dateiname oder url)": "link", "ID": "title"})
    return products_df[["link", "title"]]


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


def _load_sftp_credentials_from_env() -> FtpCredentials:
    return FtpCredentials(
        username=os.environ["FTP_USER"],
        password=os.environ["FTP_PASSWORD"],
        hostname=os.environ["FTP_HOSTNAME"],
    )


@contextmanager
def _connect_to_sftp(credentials: FtpCredentials):
    transport = paramiko.Transport((credentials["hostname"], 22))
    transport.connect(username=credentials["username"], password=credentials["password"])
    client = paramiko.SFTPClient.from_transport(transport)
    try:
        yield client
    finally:
        client.close()


@R.curry
def _load_single_image_to_sftp(sftp_client, store_id: int, filename: str) -> str:
    if "bzd" not in sftp_client.listdir():
        sftp_client.mkdir("bzd/")
    if str(store_id) not in sftp_client.listdir("bzd"):
        sftp_client.mkdir(f"bzd/{store_id}/")
    sftp_client.put(filename, f"bzd/{store_id}/{filename}")
    return filename