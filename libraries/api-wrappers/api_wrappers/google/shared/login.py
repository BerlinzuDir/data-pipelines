import os
import pathlib
import gspread
import ramda as R
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


def connect_to_service_account(file_name: str) -> gspread.Client:
    return gspread.service_account(os.path.join(_get_path_of_file(), file_name))


def connect_do_drive_with_service_account(
    file_name: str = "api-credentials.json",
) -> GoogleDrive:
    return R.pipe(
        _get_service_account_credentials,
        _authenticate_with_google_with_service_account,
        _create_google_drive_connection_with_authentication,
    )(file_name)


def _get_service_account_credentials(file_name: str) -> ServiceAccountCredentials:
    return ServiceAccountCredentials.from_json_keyfile_name(
        os.path.join(_get_path_of_file(), file_name),
        scopes="https://www.googleapis.com/auth/drive",
    )


def _get_path_of_file() -> str:
    return str(pathlib.Path(__file__).parent.resolve())


def _authenticate_with_google_with_service_account(auth) -> GoogleAuth:
    gauth = GoogleAuth()
    gauth.credentials = auth
    return gauth


def _create_google_drive_connection_with_authentication(
    auth: GoogleAuth,
) -> GoogleDrive:
    return GoogleDrive(auth)
