# tests/test_transformer.py

import pandas as pd

from source.etl.etl_transformer import ETLTransformer, MISSING_VALUE


def test_clean_price_basic():
    tr = ETLTransformer()

    assert tr._cleanPrice("Rp 450.000.000") == 450000000.0
    assert tr._cleanPrice("450000000") == 450000000.0

    # MISSING_VALUE should become NA
    assert pd.isna(tr._cleanPrice(MISSING_VALUE))


def test_parse_year_mileage_range():
    tr = ETLTransformer()

    series = tr._parseYearMileage("2018 - 70.000-75.000 km")
    year, lower_km, upper_km = series.tolist()

    assert year == 2018
    assert lower_km == 70000.0
    assert upper_km == 75000.0


def test_estimate_installment_formula():
    tr = ETLTransformer()
    price = 500_000_000

    est = tr._estimateInstallment(price)

    # Hitung manual pakai formula yang sama, lalu bandingkan
    down_payment = 0.3 * price
    other_costs = 0.11 * price
    loan = (price - down_payment) + other_costs
    interest = 0.20 * loan
    tenor = 36
    expected = round((loan + interest) / tenor, 2)

    assert est == expected
    assert est > 0


def test_full_transform_one_row(tmp_path):
    """
    End-to-end test:
    Parsed DF 1 baris -> transform -> cek kolom & nilai penting.
    """
    tr = ETLTransformer(base_url="https://www.olx.co.id")

    df_parsed = pd.DataFrame(
        [
            {
                "title": "Toyota Calya",
                "price": "Rp 150.000.000",
                "listing_url": "/item/toyota-calya-2018-iid-123",
                "location": "Duren Sawit, Jakarta Timur",
                "posted_time": "26 Nov",
                "installment": MISSING_VALUE,  # akan di-impute
                "year_mileage": "2018 - 70.000-75.000 km",
            }
        ]
    )

    out_path = tmp_path / "calya_transformed.csv"
    tr.transform(df_parsed, str(out_path))

    assert out_path.exists()

    df_out = pd.read_csv(out_path)

    # Pastikan kolom utama ada
    for col in [
        "title",
        "price",
        "listing_url",
        "location",
        "posted_time",
        "year",
        "lower_km",
        "upper_km",
        "installment",
        "installment_imputed",
    ]:
        assert col in df_out.columns

    row = df_out.iloc[0]

    # Cek parsing year & km
    assert row["year"] == 2018
    assert row["lower_km"] == 70000.0
    assert row["upper_km"] == 75000.0

    # Cek URL sudah diprefix base_url
    assert row["listing_url"].startswith("https://www.olx.co.id")

    # Installment di-impute dan > 0
    assert row["installment"] > 0
    assert bool(row["installment_imputed"]) is True
