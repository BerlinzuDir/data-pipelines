import pandas as pd
import pandas.api.types as ptypes

from .google_sheets import get_product_data_from_sheets, get_default_category_mapping


def test_get_product_data_from_sheets():
    products = get_product_data_from_sheets(sheet_address)
    assert isinstance(products, pd.DataFrame)

    example_df = pd.DataFrame(columns=columns, data=data)
    assert products.equals(example_df)


def test_get_default_category_mapping():
    mapping = get_default_category_mapping()

    # types are good
    assert ptypes.is_string_dtype(mapping["category_name"])
    assert ptypes.is_integer_dtype(mapping["category_id"])

    # values match for first row
    assert mapping.iloc[0][0] == 51
    assert mapping.iloc[0][1] == "Alkoholhaltige Getränke"


sheet_address = "1th9FMMnpng9OL7zpB2Trc85BnWBhq9LgBnr6DBwbvQM"

columns = [
    "ID",
    "Titel",
    "Beschreibung",
    "Bruttopreis",
    "Mehrwertsteuer prozent",
    "Maßeinheit",
    "Verpackungsgröße",
    "Kategorie",
    "Rückgabe Möglich",
    "Kühlpflichtig",
    "Produktbild \n(Dateiname oder url)",
    "Bestand",
    "Maßeinheit \nfür Bestand",
    "GTIN/EAN",
    "ISBN",
    "SEO \nkeywords",
    "SEO \nBeschreibungstext",
    "SEO \nSeitentitel",
]
data = [
    [
        1,
        "Apfel - Pink Lady",
        "Süß und Knackig\nHerkunft: Spanien,\n\nTip: für längere Haltbarkeit, Äpfel und Bananen getrennt lagern.",
        "€0,59",
        7,
        "stk",
        1,
        "Obst  Gemüse",
        "nein",
        "Raumtemperatur: 15-25°C",
        *(8 * [""]),  # Note the empty string for empty cells
    ]
]
