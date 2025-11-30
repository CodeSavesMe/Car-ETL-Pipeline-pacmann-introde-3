# source/etl/etl_parser.py

import os
import re
from datetime import date, timedelta
from typing import List, Dict, Optional

import pandas as pd
from bs4 import BeautifulSoup, NavigableString
from loguru import logger

from source.etl.utils.etl_selector import ITEM, BASE_URL

MISSING_VALUE = "data not found"


def _convert_to_date(raw_text: str) -> str:
    """
    Convert relative Indonesian date text into a normalized date string.

    Supports:
      - "hari ini" -> today's date (DD Mon)
      - "kemarin" -> yesterday (DD Mon)
      - "N hari yang lalu" -> N days ago (DD Mon)
    Returns raw_text if pattern is not recognized.
    """
    if not raw_text:
        return raw_text

    lower = raw_text.lower()
    MONTH_MAP = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "Mei",
        6: "Jun",
        7: "Jul",
        8: "Agu",
        9: "Sep",
        10: "Okt",
        11: "Nov",
        12: "Des",
    }

    today = date.today()

    # Handle relative date patterns
    if "hari ini" in lower:
        target_date = today
    elif "kemarin" in lower:
        target_date = today - timedelta(days=1)
    else:
        match = re.search(r"(\d+)\s+hari yang lalu", lower)
        if match:
            n = int(match.group(1))
            target_date = today - timedelta(days=n)
        else:
            # Not a relative pattern: return as-is (e.g. '18 Nov')
            return raw_text

    return f"{target_date.day} {MONTH_MAP[target_date.month]}"


def parse_html(html_data: str, parsed_path: str) -> None:
    """
    Parse OLX search result HTML into a structured CSV file.

    Parameters
    ----------
    html_data : str
        Raw HTML content from OLX search results.
    parsed_path : str
        Path to save the CSV output.

    Output columns:
        - title
        - price
        - listing_url
        - location
        - posted_time
        - installment
        - year_mileage
    Notes
    -----
    This schema is expected by etl_ransformer.transform().
    """

    logger.info(f"[Parse] Start parsing HTML -> {parsed_path}")

    # --- 1) Parse HTML with BeautifulSoup ---
    soup = BeautifulSoup(html_data, "html.parser")

    # Select all listing elements using CSS selector
    listings = soup.select(ITEM)  # ITEM = selector 'li[data-aut-id="itemBox"]', etc.
    logger.info(f"[Parse] Found {len(listings)} listing elements with selector ITEM")

    # List to hold parsed dictionary for each listing
    parsed: List[Dict[str, Optional[str]]] = []

    # --- 2) 2: Loop through each listing ---
    for idx, item in enumerate(listings):
        # a) --- Extract title ---
        title_tag = item.find(attrs={"data-aut-id": "itemTitle"})
        title = title_tag.get_text(strip=True) if title_tag else MISSING_VALUE
        if not title_tag:
            logger.debug(f"[Parse] Listing #{idx}: missing title")

        # b) --- Extract price ---
        price_tag = item.find(attrs={"data-aut-id": "itemPrice"})
        price = price_tag.get_text(strip=True) if price_tag else MISSING_VALUE
        if not price_tag:
            logger.debug(f"[Parse] Listing #{idx}: missing price")

        # c) --- Extract listing URL --- (path + domain)
        url_tag = item.find("a", href=True)
        if url_tag:
            href = url_tag["href"]
            listing_url = f"{BASE_URL}{href}"
        else:
            listing_url = MISSING_VALUE
            logger.debug(f"[Parse] Listing #{idx}: missing URL <a href>")

        # d) --- Extract location and posted_time ---
        location = MISSING_VALUE
        posted_time = MISSING_VALUE

        # --- Pattern A (new layout) ---
        # Example HTML:
        # <span data-aut-id="item-location">Jetis, Yogyakarta Kota</span>
        # <span><span>18 Nov</span></span>
        loc_tag = item.find(attrs={"data-aut-id": "item-location"})
        if loc_tag:
            # Extract location text
            loc_text = loc_tag.get_text(strip=True)
            location = loc_text if loc_text else MISSING_VALUE

            # Posted date is usually in the next sibling <span>
            sibling = loc_tag.find_next_sibling()
            if sibling:
                inner_span = sibling.find("span") or sibling
                if inner_span:
                    raw_time = inner_span.get_text(strip=True)
                    posted_time = _convert_to_date(raw_time)
        else:
            # --- Pattern B (old layout) ---
            # Example HTML:
            # <div data-aut-id="itemDetails">Kuta Alam<span>Hari ini</span></div>
            details_tag = item.find(attrs={"data-aut-id": "itemDetails"})
            if details_tag and details_tag.contents:
                # Extract location from the first content node
                first = details_tag.contents[0]
                if isinstance(first, NavigableString):
                    loc_text = first.strip()
                    location = loc_text if loc_text else MISSING_VALUE
                else:
                    # Fallback: take all text (e.g., "Kuta Alam Hari ini")
                    full_text = details_tag.get_text(" ", strip=True)
                    location = full_text if full_text else MISSING_VALUE

                # Span inside contains time info: "Hari ini", "18 Nov", "4 hari yang lalu"
                span_tag = details_tag.find("span")
                if span_tag:
                    raw_time = span_tag.get_text(strip=True)
                    posted_time = _convert_to_date(raw_time)
            else:
                # No location or posted_time found, retain default MISSING_VALUE
                location = MISSING_VALUE
                posted_time = MISSING_VALUE

        # --- Extract installment info ---
        installment_tag = item.find(attrs={"data-aut-id": "itemInstallment"})
        installment = (
            installment_tag.get_text(strip=True) if installment_tag else MISSING_VALUE
        )

        # --- Extract year and mileage ---
        ym_tag = item.find(attrs={"data-aut-id": "itemSubTitle"})
        year_mileage = ym_tag.get_text(" ", strip=True) if ym_tag else MISSING_VALUE

        # --- Add parsed data to the list ---
        parsed.append(
            {
                "title": title,
                "price": price,
                "listing_url": listing_url,
                "location": location,
                "posted_time": posted_time,
                "installment": installment,
                "year_mileage": year_mileage,
            }
        )

    # --- 3) Convert parsed list into DataFrame ---
    df = pd.DataFrame(parsed)

    # --- 4) Ensure the destination folder exists ---
    dir_name = os.path.dirname(parsed_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)

    # --- 5) Save DataFrame to CSV ---
    df.to_csv(parsed_path, index=False, encoding="utf-8")

    logger.info(f"[Parse] Parsing done. {len(df)} rows written to: {parsed_path}")
