import json
import os

import pandas as pd
import requests
import slate3k
import uuid

PRODUCTS_ENDPOINT = "https://produkte.metro.de/explore.articlesearch.v1/"
PRODUCT_DETAIL_ENDPOINT = "https://produkte.metro.de/evaluate.article.v1/"


def get_products_from_metro(store_id, **kwargs) -> pd.DataFrame:
    products_endpoint = _get_products_endpoint(store_id, **kwargs)
    products = _get_products(products_endpoint)
    return _scrape_products(products, store_id)


def _get_products_endpoint(store_id: str, **kwargs) -> str:
    products_endpoint = (
        PRODUCTS_ENDPOINT
        + f"search?storeId={store_id}"
        + "&language=de-DE"
        + "&country=DE"
        + "&profile=boostRopoTopsellers"
        + "&facets=true"
        + "&categories=true"
    )

    for key, value in kwargs.items():
        if key in ["categories", "brands"]:
            continue
        products_endpoint += f"&{key}={value}"
    if "categories" in kwargs:
        products_endpoint += "".join([f"&filter=category%3A{category}" for category in kwargs["categories"]])
    if "brands" in kwargs:
        products_endpoint += "".join([f"&filter=brand%3A{brand}" for brand in kwargs["brands"]])
    return products_endpoint


def _get_products(products_endpoint: str) -> dict:
    products_response = requests.get(products_endpoint)
    products_response.raise_for_status()
    return json.loads(products_response.content)


def _scrape_products(products: dict, store_id: str) -> pd.DataFrame:
    products_dict = {
        "Titel": [],
        "Beschreibung": [],
        "Bruttopreis": [],
        "Mehrwertsteuer": [],
        "Maßeinheit": [],
        "Verpackungsgröße": [],
        "Kategorie": [],
        "Produktbild": [],
        "gtins/eans": [],
    }
    for article_id in products["resultIds"]:
        if not products["results"][article_id]["isAvailable"]:
            continue
        betty_article_id = article_id[:-4]
        product_detail_endpoint = _get_product_detail_endpoint(betty_article_id, store_id)
        product_detail = _get_product_detail(product_detail_endpoint)
        try:
            # TODO: make this for the whole product and log error
            bundles = product_detail["result"][betty_article_id]["variants"][store_id]["bundles"]
        except Exception as error:
            print(error)
            continue
        for bundle in bundles:
            products_dict["Titel"].append(bundles[bundle]["description"])
            products_dict["Beschreibung"].append("")
            products_dict["Bruttopreis"].append(bundles[bundle]["stores"]["00032"]["sellingPriceInfo"]["finalPrice"])
            products_dict["Mehrwertsteuer"].append(
                int(bundles[bundle]["stores"]["00032"]["sellingPriceInfo"]["vatPercent"] * 100)
            )
            net_piece_unit = list(bundles[bundle]["contentData"].keys())[0]
            products_dict["Maßeinheit"].append(
                "stk"
                if int(bundles[bundle]["bundleSize"]) > 1
                else bundles[bundle]["contentData"][net_piece_unit]["uom"]
            )
            products_dict["Verpackungsgröße"].append(
                bundles[bundle]["bundleSize"]
                if int(bundles[bundle]["bundleSize"]) > 1
                else bundles[bundle]["contentData"][net_piece_unit]["value"]
            )
            products_dict["Kategorie"].append(bundles[bundle]["categories"][0]["name"])
            products_dict["Produktbild"].append(bundles[bundle]["imageUrl"])
            try:
                pdf_url = bundles[bundle]["details"]["media"]["documents"][0]["url"]
            except IndexError:
                pdf_url = ""
            if pdf_url:
                response = requests.get(pdf_url)
                filename = "my_pdf.pdf"
                with open(filename, "wb") as my_data:
                    my_data.write(response.content)
                with open(filename, "rb") as pdf_file:
                    pdf_content = slate3k.PDF(pdf_file)
                os.remove(filename)
                keyword = "GTIN / EAN : "
                gtin_ean_index = pdf_content[0].find(keyword)
                zutat_index = pdf_content[0].find("\n\nZutat\n")
                gtin_ean = pdf_content[0][int(gtin_ean_index + len(keyword)): zutat_index]
                if "," in gtin_ean:
                    gtin_eans = gtin_ean.split(",")
                else:
                    try:
                        gtin_eans = [int(gtin_ean)]
                    except Exception:
                        gtin_eans = []
                        print("could not  parse gtin from file")
                products_dict["gtins/eans"].append(gtin_eans)
            else:
                products_dict["gtins/eans"].append("")
    return pd.DataFrame.from_dict(products_dict)


def _get_product_detail_endpoint(betty_article_id: str, store_id: str) -> str:
    return (
        PRODUCT_DETAIL_ENDPOINT
        + f"betty-articles?ids={betty_article_id}"
        + "&country=DE"
        + "&locale=de-DE"
        + f"&storeIds={store_id}"
        + "&details=true"
    )


def _get_product_detail(product_detail_endpoint: str) -> dict:
    product_response = requests.get(product_detail_endpoint, headers={"CallTreeId": str(uuid.uuid4())})
    product_response.raise_for_status()
    return json.loads(product_response.content)


if __name__ == "__main__":
    STORE_ID = "0032"
    CATEGORIES = ["food/obst-gemüse"]
    BRANDS = [""]
    RESTRICTION = "18a94965-6d24-3396-ae3a-61af860565d1"
    products_df = get_products_from_metro(store_id=STORE_ID, restriction=RESTRICTION, rows=20, page=1)
