from contextlib import contextmanager
import requests
import shutil
import pandas as pd
from typing import TypedDict, List
import paramiko
from time import sleep
import os
import ramda as R
from api_wrappers.google import get_file_list_from_drive


class FileListDict(TypedDict):
    title: List[str]
    url: List[str]
    hash: List[str]


def load_files_from_google_to_sftp(store_id: int, google_drive_folder_id: str) -> None:
    R.pipe(
        get_file_list_from_drive,
        _download_all_files,
        _load_all_files_to_sftp(store_id),
        lambda df: df.to_dict(),  # return values have to be json serializable
    )(google_drive_folder_id)


class FtpCredentials(TypedDict):
    username: str
    password: str
    hostname: str


def _load_sftp_credentials_from_env() -> FtpCredentials:
    return {
        "username": os.environ["FTP_USER"],
        "password": os.environ["FTP_PASSWORD"],
        "hostname": os.environ["FTP_HOSTNAME"],
    }


@R.curry
def _load_all_files_to_sftp(store_id: int, file_list: pd.DataFrame) -> pd.DataFrame:
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as sftp_client:
        file_list["title"].apply(lambda title: _load_single_image_to_sftp(sftp_client, store_id, title))
    return file_list


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
    with open(filename, "rb") as file:
        if store_id not in sftp_client.list_dir():
            sftp_client.mkdir(f"/{store_id}/")
        sftp_client.put(filename, f"{store_id}/{filename}")
    return filename


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
