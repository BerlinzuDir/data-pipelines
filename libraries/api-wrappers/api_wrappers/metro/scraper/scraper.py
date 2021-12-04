import json
import os
import traceback

import pandas as pd
import requests
import slate3k
import uuid
import logging

from api_wrappers.metro.scraper.generators import get_proxy, generate_header


PRODUCTS_ENDPOINT = "https://produkte.metro.de/explore.articlesearch.v1/"
PRODUCT_DETAIL_ENDPOINT = "https://produkte.metro.de/evaluate.article.v1/"


def get_products_from_metro(store_id, path="./", **kwargs) -> pd.DataFrame:
    logging.info(f'Processing store({store_id}) with arguments: {kwargs}')
    products_df_list = []
    while True:
        products_endpoint = _get_products_endpoint(store_id, **kwargs)
        products = _get_products(products_endpoint)
        logging.info(f"Processing page {products['page']}/{products['totalPages']}")
        try:
            products_df = _scrape_products(products, store_id)
            filename = f'{path}/products_{kwargs["category"].split("/")[-1]}_{products["page"]}'
            _store(products_df, filename, config=kwargs)
            products_df_list.append(products_df)
        except Exception:
            logging.error(f"Stop processing page {kwargs['page']} due to:\n {traceback.format_exc()}")
        if products["nextPage"]:
            kwargs["page"] = products["nextPage"]
        else:
            return pd.concat(products_df_list, ignore_index=True)


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
        if key in ["category", "brands"]:
            continue
        products_endpoint += f"&{key}={value}"
    if "category" in kwargs:
        products_endpoint += f"&filter=category%3A{kwargs['category']}"
    if "brands" in kwargs:
        products_endpoint += "".join([f"&filter=brand%3A{brand}" for brand in kwargs["brands"]])
    return products_endpoint


def _get_products(products_endpoint: str) -> dict:
    count = 3
    while count:
        try:
            headers = generate_header()
            proxies = get_proxy()
            products_response = requests.get(
                products_endpoint, proxies=proxies, headers=headers, timeout=15
            )
            products_response.raise_for_status()
            return json.loads(products_response.content)
        except Exception:
            logging.error(f"Failed requesting products due to:\n {traceback.format_exc()}")
            count -= 1
    raise Exception


def _scrape_products(products: dict, store_id: str) -> pd.DataFrame:
    products_dict = {
        "Id": [],
        "Marke": [],
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
        try:
            products_dict = _scrape_article_id(products_dict, article_id, store_id)
        except Exception:
            logging.error(f"Failed requesting article({article_id}) due to:\n {traceback.format_exc()}")
            continue
    return pd.DataFrame.from_dict(products_dict)


def _scrape_article_id(products_dict: dict, article_id: str, store_id: str) -> dict:
    betty_article_id = article_id[:-4]
    product_detail_endpoint = _get_product_detail_endpoint(betty_article_id, store_id)
    try:
        product_detail = _get_product_detail(product_detail_endpoint)
    except Exception:
        logging.error(f"Failed requesting article detail of {article_id} due to:\n {traceback.format_exc()}")
    try:
        bundles = product_detail["result"][betty_article_id]["variants"][store_id]["bundles"]
        for bundle in bundles:
            if bundles[bundle]["stores"]["00032"]["sellingPriceInfo"]["applicablePromos"]:
                continue
            if int(bundles[bundle]["bundleSize"]) > 1:
                continue
            products_dict["Id"].append(bundles[bundle]["bundleId"]["bettyBundleId"])
            products_dict["Marke"].append(bundles[bundle]["brandName"])
            products_dict["Titel"].append(bundles[bundle]["description"])
            products_dict["Beschreibung"].append("")
            products_dict["Bruttopreis"].append(bundles[bundle]["stores"]["00032"]["sellingPriceInfo"]["finalPrice"])
            products_dict["Mehrwertsteuer"].append(
                int(bundles[bundle]["stores"]["00032"]["sellingPriceInfo"]["vatPercent"] * 100)
            )
            net_piece_unit = list(bundles[bundle]["contentData"].keys())[0]
            products_dict["Maßeinheit"].append(bundles[bundle]["contentData"][net_piece_unit]["uom"])
            products_dict["Verpackungsgröße"].append(bundles[bundle]["contentData"][net_piece_unit]["value"])
            products_dict["Kategorie"].append(bundles[bundle]["categories"][0]["name"])
            products_dict["Produktbild"].append(bundles[bundle]["imageUrl"])
            try:
                pdf_endpoint = bundles[bundle]["details"]["media"]["documents"][0]["url"]
            except IndexError:
                pdf_endpoint = ""
            if pdf_endpoint:
                try:
                    eans = _get_eans(pdf_endpoint)
                    products_dict["gtins/eans"].append(eans)
                except Exception:
                    products_dict["gtins/eans"].append("")
                    logging.error(f"Failed getting eans due to:\n {traceback.format_exc()}")
            else:
                products_dict["gtins/eans"].append("")
    except Exception:
        logging.error(f"Failed scraping article({article_id} details due to:\n {traceback.format_exc()}")
    return products_dict


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
    count = 3
    while count:
        try:
            proxies = get_proxy()
            headers = generate_header()
            headers["CallTreeId"] = str(uuid.uuid4())
            product_response = requests.get(
                product_detail_endpoint,
                proxies=proxies,
                headers=headers,
                timeout=15,
            )
            product_response.raise_for_status()
            return json.loads(product_response.content)
        except Exception:
            count -= 1
    raise Exception


def _get_eans(pdf_endpoint: str):
    response = _get_pdf(pdf_endpoint)
    filename = "my_pdf.pdf"
    with open(filename, "wb") as my_data:
        my_data.write(response.content)
    with open(filename, "rb") as pdf_file:
        pdf_content = slate3k.PDF(pdf_file)
    os.remove(filename)
    keyword = "GTIN / EAN : "
    gtin_ean_index = pdf_content[0].find(keyword)
    zutat_index = pdf_content[0].find("\n\nZutat\n")
    gtin_start_index = int(gtin_ean_index + len(keyword))
    gtin_ean = pdf_content[0][gtin_start_index:zutat_index]
    if "," in gtin_ean:
        gtin_eans = gtin_ean.split(",")
    else:
        try:
            gtin_eans = [int(gtin_ean)]
        except Exception:
            gtin_eans = []
            logging.error(f"Failed parsing gtin from file due to:\n {traceback.format_exc()}")
    return gtin_eans


def _get_pdf(pdf_endpoint: str) -> requests.Response:
    count = 3
    while count:
        try:
            proxies = get_proxy()
            headers = generate_header()
            response = requests.get(pdf_endpoint, proxies=proxies, headers=headers, timeout=15)
            response.raise_for_status()
            return response
        except Exception:
            logging.error(f"Failed requesting pdf due to:\n {traceback.format_exc()}")

            count -= 1
    raise Exception


def _store(df: pd.DataFrame, filepath: str, config: dict):
    with open(filepath + '_config' + '.json', 'w') as fp:
        json.dump(config, fp)
    df.to_csv(filepath + '.csv', index=False)


if __name__ == "__main__":
    STORE_ID = "0032"
    CATEGORIES = ["food/obst-gemüse"]
    BRANDS = [""]
    RESTRICTION = "18a94965-6d24-3396-ae3a-61af860565d1"
    products_df = get_products_from_metro(store_id=STORE_ID, restriction=RESTRICTION, rows=20, page=1)
