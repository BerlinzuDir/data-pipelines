from unittest import TestCase

import pytest

from dags.shop_407.sync_images import sync_images
from .sync_images import _load_sftp_credentials_from_env, _connect_to_sftp, load_images_to_sftp

from dags.shop_407.sync import TRADER_ID


TESTCASE = TestCase()
STORE_ID = f"{TRADER_ID}_test"
FOLDER_ID = "1lQ2dyF3bschhZIl4MdMZ-Bn0VmbEz5Qv"


def test_load_images_to_sftp(_decorate_load_product_data, _sftp_cleanup):
    products = load_images_to_sftp(STORE_ID)

    assert len(products) == 4
    assert file_exists_on_sftp('1330.jpg')
    assert file_exists_on_sftp("1459.jpg")
    assert file_exists_on_sftp('1578.jpg')
    assert file_exists_on_sftp("1593.jpg")


@pytest.fixture
def _decorate_load_product_data():
    """Cut the return dataframe of _load_product_data to shorten test run."""

    def dec(func):
        def inner():
            return func()[:4]

        return inner

    sync_images._load_product_data = dec(sync_images._load_product_data)


def file_exists_on_sftp(filename):
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as client:
        exists = filename in client.listdir(f"bzd/{STORE_ID}")
        if exists:
            client.remove(f"bzd/{STORE_ID}/{filename}")
    return exists


@pytest.fixture
def _sftp_cleanup():
    yield None
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as client:
        for file in client.listdir(f"bzd/{STORE_ID}"):
            client.remove(f"bzd/{STORE_ID}/{file}")
        client.rmdir(f"bzd/{STORE_ID}")
