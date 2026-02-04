import textwrap
from pathlib import Path

import pytest

from fixed_momox_isbn_checker import (
    OfferResult,
    collect_isbns,
    filter_expensive,
    load_isbns_from_file,
    normalize_isbn,
    parse_offer_page,
)


def test_normalize_isbn():
    assert normalize_isbn("978-2-07-036822-8") == "9782070368228"
    assert normalize_isbn(" 9782070368228 ") == "9782070368228"
    assert normalize_isbn("ISBN 0-321-14653-0") == "0321146530"


def test_load_isbns_from_file(tmp_path: Path):
    content = textwrap.dedent(
        """
        9782070368228, 9782253006329
        9782749948571
        """
    ).strip()
    path = tmp_path / "isbns.txt"
    path.write_text(content, encoding="utf-8")
    assert load_isbns_from_file(path) == [
        "9782070368228",
        "9782253006329",
        "9782749948571",
    ]


def test_collect_isbns_from_args():
    class Args:
        isbn_file = None
        isbns = ["978-2-07-036822-8", "9782253006329"]

    assert collect_isbns(Args) == ["9782070368228", "9782253006329"]


def test_filter_expensive():
    results = [
        OfferResult("1", "url1", True, 7.5, "A", "OK"),
        OfferResult("2", "url2", True, 8.01, "B", "OK"),
        OfferResult("3", "url3", True, 12.0, "C", "OK"),
    ]
    filtered = filter_expensive(results, 8.0)
    assert [r.isbn for r in filtered] == ["3", "2"]


def test_parse_offer_page_price_from_jsonld():
    pytest.importorskip("bs4")
    html = """
    <html>
      <head><title>Test</title></head>
      <body>
        <script type="application/ld+json">
          {"offers": {"price": "9.50"}}
        </script>
      </body>
    </html>
    """
    result = parse_offer_page(html, "9782070368228", "https://example.test")
    assert result.prix_eur == pytest.approx(9.50)
    assert result.statut == "OK"
