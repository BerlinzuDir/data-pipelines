import json

import pandas as pd
import ramda as R


def transform_articles(articles: pd.DataFrame, variants: list = None) -> json:
    return R.pipe(
        _transform_to_json,
        R.map(_transform_json(variants)),
        lambda x: {"data": {"articles": x}},
        lambda x: json.dumps(x),
    )(articles)


def _transform_to_json(articles: pd.DataFrame) -> json:
    return articles.to_dict("records")


@R.curry
def _transform_json(variants: list, raw: dict) -> dict:
    return R.apply_spec(
        {
            "name": R.prop("Titel"),
            "itemNumber": R.pipe(R.prop("ID"), lambda x: str(x)),
            "category": R.prop("Kategorie"),
            "priceBrutto": R.prop("Bruttopreis"),
            "priceNetto": _calc_net_price_from_raw_product,
            "description": R.prop("Beschreibung"),
            "vat": R.pipe(R.prop("Mehrwertsteuer prozent"), lambda x: str(x)),
            "images": R.prop("Produktbild \n(Dateiname oder url)"),
            "stock": R.prop("Bestand"),
            "unitSection": {
                "weightUnit": R.prop("Maßeinheit"),
                "weight": R.pipe(R.prop("Verpackungsgröße"), lambda x: str(x)),
                "priceSection": {
                    "price": R.prop("Bruttopreis"),
                    "vat": R.pipe(R.prop("Mehrwertsteuer prozent"), lambda x: str(x)),
                },
                "variantSection": _variant_section(variants),
                "ean": R.prop("GTIN/EAN"),
            },
        }
    )(raw)


def _calc_net_price_from_raw_product(raw: dict) -> float:
    return R.converge(
        _calc_net,
        [R.prop("Bruttopreis"), R.prop("Mehrwertsteuer prozent")],
    )(raw)


def _calc_net(brutto: float, taxrate: float) -> float:
    return round(brutto / (1 + float(taxrate) / 100.0), 2)


@R.curry
def _variant_section(variants: list, raw: dict) -> list:
    if not variants:
        return []
    variant_section = []
    for variant in variants:
        section = {"variantName": variant["name"], "variantValueSection": []}
        for variant_value in variant["variant_values"]:
            if not raw[variant_value["price_column"]]:
                continue
            section["variantValueSection"].append({
                            "VariantValueName": variant_value["name"],
                            "VariantValuePrice": raw[variant_value["price_column"]],
                        })
        if not section["variantValueSection"]:
            continue
        variant_section.append(section)
    return variant_section
