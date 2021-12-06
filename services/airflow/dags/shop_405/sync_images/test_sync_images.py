import os

import pandas as pd

from .sync_images import load_images_to_sftp, _load_sftp_credentials_from_env, _connect_to_sftp


def test_load_images_to_sftp():
    products = load_images_to_sftp(PRODUCTS, STORE_ID)
    assert os.path.isfile(ID_1 + '.jpg')
    assert _file_exists_on_sftp(ID_1 + '.jpg')
    assert os.path.isfile(ID_2 + '.jpg')
    assert _file_exists_on_sftp(ID_2 + '.jpg')
    pd.testing.assert_frame_equal(products, PRODUCTS)


def _file_exists_on_sftp(filename):
    credentials = _load_sftp_credentials_from_env()
    with _connect_to_sftp(credentials) as client:
        exists = filename in client.listdir(f"bzd/{STORE_ID}")
        if exists:
            client.remove(f"bzd/{STORE_ID}/{filename}")
    return exists


STORE_ID = 405

ID_1 = "id_1"
ID_2 = "id_2"

IMAGE_URL1 = "http://static-files/static/images/1.jpeg"
IMAGE_URL2 = "http://static-files/static/images/2.jpg"

PRODUCTS = pd.DataFrame(
    columns=["Produktbild \n(Dateiname oder url)", "ID"],
    data=[[IMAGE_URL1, ID_1], [IMAGE_URL2, ID_2]],
)
