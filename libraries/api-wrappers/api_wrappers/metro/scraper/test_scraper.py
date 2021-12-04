import os
from unittest import TestCase

import pytest
import pandas as pd

from api_wrappers.metro.scraper import get_products_from_metro


STORE_ID = "0032"
CATEGORY = "food"
BRANDS = ["Bionade", "Teekanne"]
QUERY = "bio"


class TestScraper(TestCase):

    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.tmp_path = os.path.join(dir_path, "tmp")
        os.mkdir(self.tmp_path)

    def tearDown(self) -> None:
        for file in os.listdir(self.tmp_path):
            os.remove(os.path.join(self.tmp_path, file))
        os.rmdir(self.tmp_path)

    @pytest.mark.block_network
    @pytest.mark.vcr
    def test_get_products_from_metro(self):
        products = get_products_from_metro(
            path=self.tmp_path,
            store_id=STORE_ID,
            category=CATEGORY,
            brands=BRANDS,
            rows=5,
            query=QUERY,
        )
        assert len(os.listdir(self.tmp_path)) == 8
        assert len(products) == 9
        pd.testing.assert_frame_equal(
            products.take([2, 8]),
            self._expected_dataframe,
            check_like=True,
            check_dtype=False,
        )

    @property
    def _expected_dataframe(self):
        return pd.DataFrame(
            {
                "Titel": [
                    "Teekanne Bio Gastro Luxury Cup 20 Stk. English Breakfast",
                    "Bionade Holunder PET Einweg - 1 x 500 ml Flasche",
                ],
                "Id": ["BTY-X38886000320021", "BTY-X34884800320021"],
                "Marke": ["TEEKANNE", "BIONADE"],
                "Beschreibung": ["", ""],
                "Bruttopreis": [1.61, 1.42],
                "Mehrwertsteuer": [7, 19],
                "Maßeinheit": ["GRAM", "ML"],
                "Verpackungsgröße": [40, 500],
                "Kategorie": [
                    "Food / Getränke / Tee, Kaffee & Kakao / Tee",
                    "Food / Getränke / Alkoholfreie Getränke / Säfte & Saftgetränke"
                ],
                "Produktbild": [
                    "https://cdn.metro-group.com/de/de_pim_388818001001_01.png?format=jpg&quality=80&dpi=72",
                    "https://cdn.metro-group.com/de/de_pim_348806001001_01.png?format=jpg&quality=80&dpi=72",
                ],
                "gtins/eans": [[9001475012391], [4014472005049]],
            },
            index=[2, 8],
        )
