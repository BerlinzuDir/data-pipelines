import logging
import os
import datetime
from pathlib import Path

from scraper import get_products_from_metro
from post_processor.filter_products import post_process_product_data

# Setup logging
dir_path = os.path.dirname(os.path.realpath(__file__))
log_dir = os.path.join(Path(dir_path).parent, 'data/logs', f'{datetime.date.today()}')
if not os.path.isdir(log_dir):
    os.mkdir(log_dir)
log_file = os.path.join(log_dir, f'{datetime.datetime.now()}.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
)


STORE_ID = "0032"
CATEGORIES_ALL = [
    "food/obst-gemüse",
    "food/trockensortiment",
    "food/convenience/suppen",
    "food/convenience/pasta-fertiggerichte",
    "food/convenience/konserven",
    "food/getränke/alkoholfreie-getränke",
    "food/getränke/tee-kaffee-kakao",
    "food/getränke/wein-sekt-prosecco/sekt-prosecco-champagner",
    "non-food/non-food/pflege-kosmetik-babynahrung",
    "non-food/non-food/reinigung",
]

RESTRICTION = "18a94965-6d24-3396-ae3a-61af860565d1"  # TODO
QUERIES = ["bio", "gesunde+Ernährung", "veggie", "Frühstück", "Weihnachten"]  # TODO
QUERY = "bio"

for category in CATEGORIES_ALL:
    path = 'api_wrappers/metro/data/'
    products_df = get_products_from_metro(store_id=STORE_ID, category=category, path=path, rows=20, page=1)


EXCLUDED_BRANDS = ["METRO Chef", "aro", "METRO Chef Bio", "METRO Premium", "METRO Chef Gourvenience"]
DATA_DIRECTORY = 'api_wrappers/metro/data/'
post_process_product_data(DATA_DIRECTORY, excluded_brands=EXCLUDED_BRANDS)
