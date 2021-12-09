from typing import List
from typing import TypedDict

import pandas as pd
import ramda as R
from pydrive2.drive import GoogleDrive

from api_wrappers.google.shared import connect_do_drive_with_service_account


class File(TypedDict):
    url: str
    title: str


def get_file_list_from_drive(folder_id: str, credentials_file: str = "api-credentials.json") -> pd.DataFrame:
    return R.pipe(
        connect_do_drive_with_service_account,
        _get_file_list_from_drive_instance(folder_id),
        _assert_that_files_are_present,
        _select_title_and_id_from_file_list,
        pd.DataFrame,
        _set_column_names,
    )(credentials_file)


def download_file_from_drive(drive_file_id: str, filename: str, credentials_file: str = "api-credentials.json") -> None:
    R.pipe(
        connect_do_drive_with_service_account,
        _download_file(drive_file_id, filename)
    )(credentials_file)


@R.curry
def _download_file(drive_file_id: str, filename: str, drive: GoogleDrive):
    file = drive.CreateFile({'id': drive_file_id})
    return file.GetContentFile(filename)


def _select_title_and_id_from_file_list(
    file_list: List[GoogleDrive],
) -> List[File]:
    return R.map(R.pick(["title", "id", "md5Checksum"]))(file_list)


@R.curry
def _get_file_list_from_drive_instance(folder_id: str, drive: GoogleDrive) -> List[GoogleDrive]:
    return drive.ListFile({"q": f"parents in '{folder_id}'"}).GetList()


def _assert_that_files_are_present(file_list: List) -> List[str]:
    if len(file_list) > 0:
        return file_list
    else:
        raise ValueError("No files returned from server. Make sure, folder id is correct and folder is shared.")


@R.curry
def _set_column_names(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={"md5Checksum": "hash"})
