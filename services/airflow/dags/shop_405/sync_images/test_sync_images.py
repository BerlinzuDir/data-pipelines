import pytest

from unittest import TestCase

from dags.shop_405.sync_images import sync_images
from .sync_images import load_images_to_sftp, _load_sftp_credentials_from_env, _connect_to_sftp, _load_product_data


TESTCASE = TestCase()
STORE_ID = 405


@pytest.mark.vcr
def test_load_images_to_sftp(_expected_columns):
    _decorate_load_product_data()
    products = load_images_to_sftp(STORE_ID)

    file_list = _file_list_sftp()

    assert len(file_list) == 4
    assert f'{products["id"].iloc[0]}' + ".jpg" in file_list
    TESTCASE.assertListEqual(list(products.columns), _expected_columns)
    assert len(products) == 4


def _decorate_load_product_data():
    """Cut the return dataframe of _load_product_data to shorten test run."""

    def dec(func):
        def inner():
            return func()[:4]

        return inner

    sync_images._load_product_data = dec(_load_product_data)


def _file_list_sftp():
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as client:
        file_list = client.listdir(f"bzd/{STORE_ID}")
        for filename in file_list:
            client.remove(f"bzd/{STORE_ID}/{filename}")
    return file_list


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
