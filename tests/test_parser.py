# tests/test_parser.py

import pandas as pd

from source.etl.etl_parser import parse_html


def test_parse_html_basic(tmp_path):
    """
    Parsir 1 listing dummy dan pastikan kolom utama terisi benar.
    """

    html = """
    <html>
      <body>
        <ul>
          <li data-aut-id="itemBox">
            <a href="/item/toyota-calya-2018-iid-123"></a>

            <span data-aut-id="itemTitle">Toyota Calya</span>
            <span data-aut-id="itemPrice">Rp 150.000.000</span>

            <span data-aut-id="item-location">Duren Sawit, Jakarta Timur</span>
            <span><span>26 Nov</span></span>

            <span data-aut-id="itemInstallment">8,9jt-an/bln</span>
            <span data-aut-id="itemSubTitle">2018 - 70.000-75.000 km</span>
          </li>
        </ul>
      </body>
    </html>
    """

    out_csv = tmp_path / "parsed.csv"
    parse_html(html, str(out_csv))

    assert out_csv.exists()

    df = pd.read_csv(out_csv)
    assert len(df) == 1

    row = df.iloc[0]

    assert row["title"] == "Toyota Calya"
    assert "150.000.000" in row["price"]
    assert "Duren Sawit" in row["location"]
    assert row["posted_time"] == "26 Nov"
    assert "8,9" in row["installment"]
    assert "2018" in row["year_mileage"]
