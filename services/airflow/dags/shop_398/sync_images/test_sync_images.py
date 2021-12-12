from unittest import TestCase

import pytest

from dags.shop_398.sync_images import sync_images
from .sync_images import _load_sftp_credentials_from_env, _connect_to_sftp, load_images_to_sftp

from dags.shop_398.sync import TRADER_ID


TESTCASE = TestCase()
STORE_ID = f"{TRADER_ID}_test"


@pytest.mark.vcr
def test_load_images_to_sftp(_decorate_load_product_data, _sftp_cleanup):
    products = load_images_to_sftp(STORE_ID)

    file_list_sftp = _file_list_sftp(STORE_ID)
    file_list_products = [url.split("/")[-1] for url in products["Produktbild \n(Dateiname oder url)"].values]

    assert (
        products["Produktbild \n(Dateiname oder url)"].iloc[0]
        == "http://s739086489.online.de/bzd-bilder/bzd/398/1.jpg"
    )
    assert len(file_list_sftp) == 3
    assert set(file_list_products).difference(set(file_list_sftp)) == {'3.jpg'}


@pytest.fixture
def _decorate_load_product_data():
    """Cut the return dataframe of _load_product_data to shorten test run."""

    def dec(func):
        def inner():
            return func()[:4]

        return inner

    sync_images._load_product_data = dec(sync_images._load_product_data)


def _file_list_sftp(store_id: str):
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as sftp_client:
        if "bzd" not in sftp_client.listdir():
            return []
        if store_id not in sftp_client.listdir("bzd"):
            return []
        file_list = sftp_client.listdir(f"bzd/{store_id}")
    return file_list


@pytest.fixture
def _sftp_cleanup():
    yield None
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as client:
        for file in client.listdir(f"bzd/{STORE_ID}"):
            client.remove(f"bzd/{STORE_ID}/{file}")
        client.rmdir(f"bzd/{STORE_ID}")
