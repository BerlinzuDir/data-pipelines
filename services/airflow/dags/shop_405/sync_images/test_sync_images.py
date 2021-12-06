import os
from shutil import rmtree

import numpy as np
import pandas as pd
import pytest

from dags.shop_405.sync_images import sync_images
from .sync_images import load_images_to_sftp, _load_sftp_credentials_from_env, _connect_to_sftp, _load_product_data

STORE_ID = 405


@pytest.mark.vcr
def test_load_images_to_sftp(clean_cwd):
    _decorate_load_product_data()
    products = load_images_to_sftp(STORE_ID)

    file_list = _file_list_sftp()

    assert len(file_list) == 4
    assert f'{products["id"].iloc[0]}' + ".jpg" in file_list
    assert isinstance(products, pd.DataFrame)


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
def clean_cwd():
    directory = "dir" + str(np.random.randint(10000, 99999))
    os.mkdir(directory)
    os.chdir(directory)
    try:
        yield directory
    finally:
        os.chdir("../")
        rmtree(directory)
