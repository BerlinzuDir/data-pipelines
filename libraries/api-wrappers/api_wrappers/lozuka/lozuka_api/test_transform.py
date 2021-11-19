import json

import pandas as pd
from jsondiff import diff

from api_wrappers.lozuka.lozuka_api.transform import transform_articles


def test_transform_articles():
    transformed_articles = json.loads(_sample_input_products_df().pipe(transform_articles))
    example = {"data": {"articles": [EXAMPLE_PRODUCT]}}
    assert diff(transformed_articles, example) == {}


def _sample_input_products_df() -> pd.DataFrame:
    articles_df = pd.DataFrame()
    articles_df["Titel"] = ["ACRE cardigan, organic wool"]
    articles_df["ID"] = [5909136867478]
    articles_df["Beschreibung"] = [
        "Der ACRE cardigan von format hat capeartige Ärmel und einen lange Form. "
        + "Vorne ist er durch einen Knopf aus recycling Leder zu schließen. Er ist"
        + " aus leichtem, warmen Wollfleece (100% Wolle) genäht. Er passt allen großen"
        + " und kleinen, schmalen und breiteren Menschen. Der ökologische Stoff aus dem dieses"
        + " Kleidungsstück gefertigt wurde, ist von einem zertifizierten Betrieb hergestellt worden."
    ]
    articles_df["Bruttopreis"] = [200.00]
    articles_df["Mehrwertsteuer prozent"] = [19]
    articles_df["Maßeinheit"] = ["stk"]
    articles_df["Verpackungsgröße"] = [1]
    articles_df["Kategorie"] = [25]
    articles_df["Rückgabe Möglich"] = [True]
    articles_df["Produktbild \n(Dateiname oder url)"] = [
        [
            "https://cdn.shopify.com/s/files/1/0512/9224/2070/products/acre-wofldb-"
            + "format_d9c63d1f-0a5a-46c2-8832-bdce26caa164.jpg?v=1605864705",
            "https://cdn.shopify.com/s/files/1/0512/9224/2070/products/acre-wofllg-"
            + "format_2b986ab0-d989-45ee-be1f-49ee2c3d831b.jpg?v=1605864705",
        ]
    ]
    articles_df["Kühlpflichtig"] = [""]
    articles_df["Bestand"] = [3]
    articles_df["Maßeinheit \nfür Bestand"] = ["stk"]
    articles_df["GTIN/EAN"] = [""]
    articles_df["ISBN"] = [""]
    articles_df["SEO \nkeywords"] = [""]
    articles_df["SEO \nBeschreibungstext"] = [""]
    articles_df["SEO \nSeitentitel"] = [""]
    return articles_df


EXAMPLE_PRODUCT = {
    "name": "ACRE cardigan, organic wool",
    "itemNumber": "5909136867478",
    "category": 25,
    "priceBrutto": 200.00,
    "priceNetto": 168.07,
    "description": "Der ACRE cardigan von format hat capeartige Ärmel und einen lange Form. "
    + "Vorne ist er durch einen Knopf aus recycling Leder zu schließen. Er ist "
    + "aus leichtem, warmen Wollfleece (100% Wolle) genäht. Er passt allen großen "
    + "und kleinen, schmalen und breiteren Menschen. Der ökologische Stoff aus "
    + "dem dieses Kleidungsstück gefertigt wurde, ist von einem zertifizierten Betrieb hergestellt worden.",
    "vat": "19",
    "images": [
        "https://cdn.shopify.com/s/files/1/0512/9224/2070/products/acre-wofldb-format_d9c63d1f-0a5a-46c2-8832-"
        + "bdce26caa164.jpg?v=1605864705",
        "https://cdn.shopify.com/s/files/1/0512/9224/2070/products/acre-wofllg-format_2b986ab0-d989-45ee-be1f-"
        + "49ee2c3d831b.jpg?v=1605864705",
    ],
    "stock": 3,
    "unitSection": {
        "weightUnit": "stk",
        "weight": "1",
        "priceSection": {"price": 200.00, "vat": "19"},
        "variantSection": [],
        "ean": "",
    },
}
