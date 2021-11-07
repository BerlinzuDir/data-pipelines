from .login import connect_do_drive_with_service_account

folder_id = "1lQ2dyF3bschhZIl4MdMZ-Bn0VmbEz5Qv"


def test_login_is_successful():

    drive = connect_do_drive_with_service_account()

    file_list = drive.ListFile({"q": f"parents in '{folder_id}'"}).GetList()
    assert type(file_list) is list
