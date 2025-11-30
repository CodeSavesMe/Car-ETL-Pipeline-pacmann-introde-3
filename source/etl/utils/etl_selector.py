# source/etl/utils/etl_selector.py

"""
Selectors & constants for OLX scraping/parsing.
Used by:
- Playwright scraper
- BeautifulSoup parser
"""

# Base URL OLX
BASE_URL = "https://www.olx.co.id"

# CSS selectors for Playwright (and can be reused by BeautifulSoup .select)
LOCATION_INPUT = "div[data-aut-id='locationBox'] input"
LOCATION = "div[data-aut-id='locationItem'] b"
ITEM = "li[data-aut-id='itemBox']"
FIRST_ITEM_LINK = f"{ITEM}:first-child a"
LOAD_MORE_BUTTON = "button[data-aut-id='btnLoadMore']"
