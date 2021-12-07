from unittest.mock import patch

import pytest

from unittest import TestCase

from dags.shop_405.sync_images import sync_images
from .sync_images import load_images_to_sftp, _load_product_data, _file_list_sftp


TESTCASE = TestCase()
STORE_ID = 405
EXISTING_IMAGE_ON_SFTP = "4850001270355.jpg"


@pytest.mark.vcr
def test_load_images_to_sftp(_decorate_load_product_data, _file_ids_on_sftp, _expected_columns):
    with patch("dags.shop_405.sync_images.sync_images._file_list_sftp") as _mock_file_list_sftp:
        _mock_file_list_sftp.return_value = [EXISTING_IMAGE_ON_SFTP]
        products = load_images_to_sftp(STORE_ID)

    TESTCASE.assertListEqual(list(products.columns), _expected_columns)
    assert len(products) == 4
    assert len(_file_ids_on_sftp) == 3
    assert set(products["id"].astype(str).values).difference(set(_file_ids_on_sftp)) == {
        EXISTING_IMAGE_ON_SFTP.replace(".jpg", "")
    }


@pytest.fixture
def _decorate_load_product_data():
    """Cut the return dataframe of _load_product_data to shorten test run."""

    def dec(func):
        def inner():
            return func()[:4]

        return inner

    sync_images._load_product_data = dec(_load_product_data)


@pytest.fixture
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
