from .sync_images import (
    _load_sftp_credentials_from_env,
    _connect_to_sftp,
    load_files_from_google_to_sftp,
)


def test_load_files_from_google_to_sftp():
    load_files_from_google_to_sftp(STORE_ID, FOLDER_ID)

    assert file_exists_on_sftp('1.png')
    assert file_exists_on_sftp('2.jpeg')


def file_exists_on_sftp(filename):
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as client:
        exists = filename in client.listdir(f"bzd/{STORE_ID}")
        if exists:
            client.remove(f"bzd/{STORE_ID}/{filename}")
    return exists


STORE_ID = '287_test'
FOLDER_ID = "1lQ2dyF3bschhZIl4MdMZ-Bn0VmbEz5Qv"
