from typing import TypedDict


class DagConfig(TypedDict):
    trader_id: str
    ftp_endpoint: str
    google_sheets_id: str
    google_drive_id: str
