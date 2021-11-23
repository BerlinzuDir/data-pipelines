import os.path
from shutil import rmtree
import pytest
import numpy as np
import pandas as pd
from api_wrappers.google import get_file_list_from_drive
from sync_images import (
    download,
    load_single_image_to_ftp,
    load_ftp_credentials_from_env,
    download_all,
    load_all_files_to_ftp,
    connect_to_ftp,
)


def test_load_files_from_google(clean_cwd):
    print(clean_cwd)
    file_list = get_file_list_from_drive(FOLDER_ID)

    print(file_list.values)

    file_list = download_all(file_list)

    assert os.path.isfile(GOOGLE_FILENAME)

    load_all_files_to_ftp(STORE_ID, file_list)
    assert file_exists_on_ftp(GOOGLE_FILENAME)


def test_download_all(clean_cwd):
    file_list = download_all(FILE_LIST)

    pd.testing.assert_frame_equal(file_list, FILE_LIST)
    assert os.path.isfile(FILENAME1)
    assert os.path.isfile(FILENAME2)


def test_download_file(clean_cwd):
    download(IMAGE_URL1, FILENAME1)
    assert os.path.isfile(FILENAME1)


def test_load_all_files_to_ftp(clean_cwd):

    file_list = load_all_files_to_ftp(STORE_ID, FILE_LIST)
    pd.testing.assert_frame_equal(file_list, FILE_LIST)
    assert file_exists_on_ftp(FILENAME1)
    assert file_exists_on_ftp(FILENAME2)


def test_load_to_ftp(clean_cwd):
    download(IMAGE_URL1, FILENAME1)
    assert os.path.isfile(FILENAME1)

    credentials = load_ftp_credentials_from_env()
    with connect_to_ftp(credentials) as session:
        load_single_image_to_ftp(session, STORE_ID, FILENAME1)

    assert file_exists_on_ftp(FILENAME1)


def file_exists_on_ftp(filename):
    credentials = load_ftp_credentials_from_env()
    with connect_to_ftp(credentials) as session:
        session.cwd(f"/{STORE_ID}")
        exists = filename in session.nlst()
        if exists:
            session.delete(filename)
    return exists


@pytest.fixture
def clean_cwd():
    directory = "dir" + str(np.random.randint(10000, 99999))
    os.mkdir(directory)
    os.chdir(directory)
    try:
        yield directory
    finally:
        os.chdir("..")
        rmtree(directory)


STORE_ID = 123
FOLDER_ID = "1lQ2dyF3bschhZIl4MdMZ-Bn0VmbEz5Qv"

IMAGE_URL1 = "http://static-files/static/images/1.jpeg"
HASH1 = "1lakskdfklasdf"
FILENAME1 = "1.jpeg"

IMAGE_URL2 = "http://static-files/static/images/2.jpg"
HASH2 = "1lakskdasdfuiobjkdasldkj"
FILENAME2 = "2.jpg"

GOOGLE_URL = "https://drive.google.com/uc?id=1ym44i-TWgTHu5Ncd9XIQjS4UIKdCAOfa&export=download"
GOOGLE_FILENAME = "2.jpeg"

FILE_LIST = pd.DataFrame(
    columns=["link", "title", "hash"],
    data=[[IMAGE_URL1, FILENAME1, HASH1], [IMAGE_URL2, FILENAME2, HASH2]],
)
