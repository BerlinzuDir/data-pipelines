import os
from contextlib import contextmanager
from typing import TypedDict, List

import pandas as pd
import paramiko
import ramda as R
from strongtyping.strong_typing import match_typing

from api_wrappers.google import get_file_list_from_drive
from api_wrappers.google.google_drive import download_file_from_drive
from dags.generic_google_product_imports.types import DagConfig
from dags.helpers.decorators import cwd_cleanup


class FileListDict(TypedDict):
    title: List[str]
    url: List[str]
    hash: List[str]


class FtpCredentials(TypedDict):
    username: str
    password: str
    hostname: str
    port: int


@cwd_cleanup
@match_typing
def load_files_from_google_to_sftp(config: DagConfig) -> None:
    R.pipe(
        get_file_list_from_drive,
        _download_all_files,
        _load_all_files_to_sftp(config["trader_id"]),
        R.invoker(0, "to_dict"),  # return values have to be json serializable
    )(config["google_drive_id"])


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
