import json
import os
from typing import List

import pandas as pd
import requests
import slate3k
import uuid

PRODUCTS_ENDPOINT = "https://produkte.metro.de/explore.articlesearch.v1/"
PRODUCT_DETAIL_ENDPOINT = 'https://produkte.metro.de/evaluate.article.v1/'
PRODUCT_PATH = 'https://produkte.metro.de/shop/pv/'


def get_products_from_metro(store_id, brands: List[str], categories: List[str]) -> pd.DataFrame:
    products_endpoint = (
        PRODUCTS_ENDPOINT +
        f'search?storeId={store_id}' +
        f"&language=de-DE" +
        f"&country=DE" +
        f"&query=*" +
        f"&profile=boostRopoTopsellers" +
        f"&facets=true" +
        f"&categories=true" +
        ''.join(
            [f"&filter=brand%3A{brand}" for brand in brands] +
            [f"&filter=category%3A{category}" for category in categories]
        )
    )
    products_response = requests.get(products_endpoint)
    products = json.loads(products_response.content)
    article_ids = products["resultIds"]
    products_dict = {
        "Titel": [],
        "Beschreibung": [],
        "Bruttopreis": [],
        "Mehrwertsteuer": [],
        "Maßeinheit": [],
        "Verpackungsgröße": [],
        "Kategorie": [],
        "Produktbild": [],
    }
    for article_id in article_ids:
        betty_article_id = article_id[:-4]
        product_detail_endpoint = (
            PRODUCT_DETAIL_ENDPOINT +
            f'betty-articles?ids={betty_article_id}' +
            '&country=DE' +
            '&locale=de-DE' +
            f'&storeIds={store_id}' +
            '&details=true'
        )

        product_response = requests.get(
            product_detail_endpoint,
            headers={"CallTreeId": str(uuid.uuid4())}
        )
        product = json.loads(product_response.content)
        bundles = product["result"][betty_article_id]["variants"][store_id]["bundles"]
        for bundle in bundles:
            products_dict["Titel"].append(bundles[bundle]["description"])
            products_dict["Beschreibung"].append("")
            products_dict["Bruttopreis"].append(bundles[bundle]["stores"]["00032"]["sellingPriceInfo"]["finalPrice"])
            products_dict["Mehrwertsteuer"].append(int(bundles[bundle]["stores"]["00032"]["sellingPriceInfo"]["vatPercent"] * 100))
            net_piece_unit = list(bundles[bundle]["contentData"].keys())[0]
            products_dict["Maßeinheit"].append("stk" if int(bundles[bundle]["bundleSize"]) > 1 else bundles[bundle]["contentData"][net_piece_unit]["uom"])
            products_dict["Verpackungsgröße"].append(bundles[bundle]["bundleSize"] if int(bundles[bundle]["bundleSize"]) > 1 else bundles[bundle]["contentData"][net_piece_unit]["value"])
            products_dict["Kategorie"].append(bundles[bundle]["categories"][0]["name"])
            products_dict["Produktbild"].append(bundles[bundle]["imageUrl"])
            pdf_url = bundles[bundle]["details"]["media"]["documents"][0]["url"]
            response = requests.get(pdf_url)
            filename = "my_pdf.pdf"
            with open(filename, 'wb') as my_data:
                my_data.write(response.content)
            with open(filename, 'rb') as pdf_file:
                pdf_content = slate3k.PDF(pdf_file)
            os.remove(filename)
            keyword = "GTIN / EAN : "
            gtin_ean_index = pdf_content[0].find(keyword)
            zutat_index = pdf_content[0].find("\n\nZutat\n")
            gtin_ean = pdf_content[0][int(gtin_ean_index + len(keyword)):zutat_index]
            if ',' in gtin_ean:
                gtin_eans = gtin_ean.split(',')
            else:
                try:
                    gtin_eans = [int(gtin_ean)]
                except Exception:
                    gtin_eans = []
                    print("could not  parse gtin from file")
            print("ean:", gtin_eans)
    return pd.DataFrame.from_dict(products_dict)


if __name__ == '__main__':
    STORE_ID = "0032"
    CATEGORIES = ['food']
    BRANDS = ["Bionade"]
    get_products_from_metro(store_id=STORE_ID, categories=CATEGORIES, brands=BRANDS)
