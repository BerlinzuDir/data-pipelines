import pandas as pd
import pytest
import requests
from pydrive2.files import ApiRequestError

from api_wrappers.google.google_drive import get_file_list_from_drive

FOLDER_ID = "1lQ2dyF3bschhZIl4MdMZ-Bn0VmbEz5Qv"
FOLDER_ID_NOT_SHARED = "0B6Eu5C6m7kAtcmlHVXpqTmpJUk0"


def test_get_file_list_from_drive():
    file_list = get_file_list_from_drive(FOLDER_ID)

    # return value is a data frame
    assert type(file_list) == pd.DataFrame

    # file list has correct length
    assert len(file_list) == 2

    # download links are working
    assert requests.get(file_list.link[0]).status_code == 200
    assert requests.get(file_list.link[1]).status_code == 200

    # hash contains hash
    assert file_list.hash[0] == "15bdf97b0e2de0293ec02720e25144ec"


def test_get_file_list_with_wrong_or_restricted_folder():

    with pytest.raises(ApiRequestError) as err:
        _ = get_file_list_from_drive("not_the_right_folder_id")

    with pytest.raises(ValueError) as err:
        _ = get_file_list_from_drive(FOLDER_ID_NOT_SHARED)

    assert "No files returned from server. Make sure, folder id is correct and folder is shared." in str(err)
