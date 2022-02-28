from .sync_images import (
    _load_sftp_credentials_from_env,
    _connect_to_sftp,
    load_files_from_google_to_sftp,
)


def test_load_files_from_google_to_sftp():
    load_files_from_google_to_sftp(CONFIG)

    assert file_exists_on_sftp("1.png")
    assert file_exists_on_sftp("2.jpeg")


def file_exists_on_sftp(filename):
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as client:
        exists = filename in client.listdir(f"bzd/{STORE_ID}")
        if exists:
            client.remove(f"bzd/{STORE_ID}/{filename}")
    return exists


STORE_ID = f"{287}_test"
FOLDER_ID = "1lQ2dyF3bschhZIl4MdMZ-Bn0VmbEz5Qv"

CONFIG = {
    "trader_id": STORE_ID,
    "google_drive_id": FOLDER_ID,
    "google_sheets_id": "",
    "ftp_endpoint": "http://s739086489.online.de/bzd-bilder",
}
