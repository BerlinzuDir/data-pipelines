from contextlib import contextmanager

import pandas as pd
from typing import TypedDict, List
import paramiko
import os
import ramda as R

from api_wrappers.google.google_drive import download_file_from_drive
from dags.helpers.decorators import cwd_cleanup
from api_wrappers.google import get_file_list_from_drive


class FileListDict(TypedDict):
    title: List[str]
    url: List[str]
    hash: List[str]


@cwd_cleanup
def load_files_from_google_to_sftp(store_id: str, google_drive_folder_id: str) -> None:
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
    port: int


def _load_sftp_credentials_from_env() -> FtpCredentials:
    return FtpCredentials(
        username=os.environ["FTP_USER"],
        password=os.environ["FTP_PASSWORD"],
        hostname=os.environ["FTP_HOSTNAME"],
        port=int(os.environ["FTP_PORT"]),
    )


@R.curry
def _load_all_files_to_sftp(store_id: str, file_list: pd.DataFrame) -> pd.DataFrame:
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as sftp_client:
        file_list["title"].apply(lambda title: _load_single_image_to_sftp(sftp_client, store_id, title))
    return file_list


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


def _download_all_files(file_list: pd.DataFrame) -> pd.DataFrame:
    file_list[["id", "title"]].apply(lambda row: download_file_from_drive(row["id"], row["title"]), axis=1)
    return file_list
