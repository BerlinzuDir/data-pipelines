import os.path
import pandas as pd

from dags.helpers.decorators import cwd_cleanup
from .sync_images import (
    _download,
    _load_single_image_to_sftp,
    _load_sftp_credentials_from_env,
    _download_all_files,
    _connect_to_sftp,
    load_files_from_google_to_sftp,
)


def test_load_files_from_google_to_sftp():
    load_files_from_google_to_sftp(STORE_ID, FOLDER_ID)

    assert file_exists_on_sftp(GOOGLE_FILENAME)


@cwd_cleanup
def test_download_all():
    file_list = _download_all_files(FILE_LIST)

    pd.testing.assert_frame_equal(file_list, FILE_LIST)
    assert os.path.isfile(FILENAME1)
    assert os.path.isfile(FILENAME2)


@cwd_cleanup
def test_download_file():
    _download(IMAGE_URL1, FILENAME1)
    assert os.path.isfile(FILENAME1)


@cwd_cleanup
def test_load_to_sftp():
    _download(IMAGE_URL1, FILENAME1)
    assert os.path.isfile(FILENAME1)

    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as session:
        _load_single_image_to_sftp(session, STORE_ID, FILENAME1)

    assert file_exists_on_sftp(FILENAME1)


def file_exists_on_sftp(filename):
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as client:
        exists = filename in client.listdir(f"bzd/{STORE_ID}")
        if exists:
            client.remove(f"bzd/{STORE_ID}/{filename}")
    return exists


STORE_ID = 1234
FOLDER_ID = "1lQ2dyF3bschhZIl4MdMZ-Bn0VmbEz5Qv"

IMAGE_URL1 = "http://localhost:80/static/images/1.jpeg"
HASH1 = "1lakskdfklasdf"
FILENAME1 = "1.jpeg"

IMAGE_URL2 = "http://localhost:80/static/images/2.jpg"
HASH2 = "1lakskdasdfuiobjkdasldkj"
FILENAME2 = "2.jpg"

GOOGLE_URL = "https://drive.google.com/uc?id=1ym44i-TWgTHu5Ncd9XIQjS4UIKdCAOfa&export=download"
GOOGLE_FILENAME = "2.jpeg"

FILE_LIST = pd.DataFrame(
    columns=["link", "title", "hash"],
    data=[[IMAGE_URL1, FILENAME1, HASH1], [IMAGE_URL2, FILENAME2, HASH2]],
)
