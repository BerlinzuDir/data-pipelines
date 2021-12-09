from unittest.mock import patch

import pytest

from unittest import TestCase

from dags.shop_405.sync_images import sync_images
from dags.shop_405.sync import TRADER_ID
from .sync_images import (
    load_images_to_sftp,
    _load_product_data,
    _file_list_sftp,
    _load_sftp_credentials_from_env,
    _connect_to_sftp,
)

TESTCASE = TestCase()
STORE_ID = f"{TRADER_ID}_test"
EXISTING_IMAGE_ON_SFTP = "4850001270355.jpg"


@pytest.mark.vcr
def test_load_images_to_sftp(_decorate_load_product_data, _expected_columns, _sftp_cleanup):
    with patch("dags.shop_405.sync_images.sync_images._file_list_sftp") as _mock_file_list_sftp:
        _mock_file_list_sftp.return_value = [EXISTING_IMAGE_ON_SFTP]
        products = load_images_to_sftp(STORE_ID)

    file_ids_sftp = _file_ids_on_sftp()
    TESTCASE.assertListEqual(list(products.columns), _expected_columns)
    assert len(products) == 4
    assert len(file_ids_sftp) == 3
    assert set(products["id"].astype(str).values).difference(set(file_ids_sftp)) == {
        EXISTING_IMAGE_ON_SFTP.replace(".jpg", "")
    }


def test_file_list_sftp():
    # TODO
    # Has to be tested since is mocked in the public method
    assert True


@pytest.fixture
def _decorate_load_product_data():
    """Cut the return dataframe of _load_product_data to shorten test run."""

    def dec(func):
        def inner():
            return func()[:4]

        return inner

    sync_images._load_product_data = dec(_load_product_data)


def _file_ids_on_sftp():
    return [file.replace(".jpg", "") for file in _file_list_sftp(STORE_ID)]


@pytest.fixture
def _expected_columns():
    yield [
        "id",
        "name",
        "ru name",
        "price",
        "tax",
        "weight",
        "unit",
        "category",
        "tags",
        "supplier",
        "pic",
        "description",
    ]


@pytest.fixture
def _sftp_cleanup():
    yield None
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as client:
        for file in client.listdir(f"bzd/{STORE_ID}"):
            client.remove(f"bzd/{STORE_ID}/{file}")
        client.rmdir(f"bzd/{STORE_ID}")
