# engine.py
"""
This module orchestrates the end-to-end ETL pipeline:

1. Scrape    : Playwright → HTML
2. Parse     : HTML → CSV (parsed)
3. Transform : CSV parsed → CSV transformed
4. Load      : CSV transformed → PostgreSQL + JSON inserted

Public functions:
- scrape_html(...)
- parse_html_file(...)
- transform_parsed_file(...)
- load_transformed_file(...)
- run_full_etl(...)

Example usage:

    from engine import run_full_etl

    run_full_etl(
        keyword="BMW 3 Series",
        html_path="data/raw_html/bmw.html",
        parsed_path="data/parsed/bmw.csv",
        transformed_path="data/transformed/bmw_transformed.csv",
        inserted_path="data/inserted/bmw_inserted.json",
        table_name="scrape_data",
        db_url=None,  # use DB_URL / POSTGRES_* from .env
    )
"""

import asyncio
import os
from typing import Optional

from dotenv import load_dotenv
from loguru import logger
from playwright.async_api import async_playwright

from source.etl.etl_scraper import olx_scraper
from source.etl.etl_parser import parse_html
from source.etl.etl_transformer import ETLTransformer
from source.etl.db_loader import load_data

from source.logging_config import configure_logging

# --- Global logging configuration for engine entrypoint ---
configure_logging()
logger.info("[Engine] Logging configured from engine.py")

# --- load .env
load_dotenv()


# --- 1) SCRAPE: Playwright → HTML ---
async def scrape_html_async(
    keyword: str,
    html_path: str,
    location: str = "Indonesia",
    headless: bool = False,
    goto_timeout_ms: int = 60_000,
) -> None:
    """
    Asynchronous scraping function.

    Parameters
    ----------
    keyword : str
        Search keyword, for example "BMW 3 Series".
    html_path : str
        Target file path where the scraped HTML will be saved.
    location : str, optional
        OLX location filter, default is "Indonesia".
    headless : bool, optional
        Whether to run the browser in headless mode. Default False for easier
        debugging.
    goto_timeout_ms : int, optional
        Timeout for page.goto() in milliseconds. Default is 60_000.
    """
    # --- Ensure output directory exists ---
    out_dir = os.path.dirname(html_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    async with async_playwright() as p:
        await olx_scraper(
            playwright=p,
            keyword=keyword,
            html_path=html_path,
            location=location,
            headless=headless,
            goto_timeout_ms=goto_timeout_ms,
        )


def scrape_html(
    keyword: str,
    html_path: str,
    location: str = "Indonesia",
    headless: bool = False,
    goto_timeout_ms: int = 60_000,
) -> None:
    """
    Synchronous wrapper for the scraping step.

    This wraps `scrape_html_async` using `asyncio.run()` so it can be called
    from regular (non-async) scripts.

    Parameters
    ----------
    keyword : str
        Search keyword, for example "BMW 3 Series".
    html_path : str
        Target file path where the scraped HTML will be saved.
    location : str, optional
        OLX location filter, default is "Indonesia".
    headless : bool, optional
        Whether to run the browser in headless mode. Default False.
    goto_timeout_ms : int, optional
        Timeout for page.goto() in milliseconds. Default is 60_000.
    """
    logger.info(f"[Engine] Start SCRAPE for keyword='{keyword}' → {html_path}")
    asyncio.run(
        scrape_html_async(
            keyword=keyword,
            html_path=html_path,
            location=location,
            headless=headless,
            goto_timeout_ms=goto_timeout_ms,
        )
    )
    logger.info(f"[Engine] SCRAPE done: HTML saved to {html_path}")


# --- 2) PARSE: HTML → CSV (parsed) ---
def parse_html_file(
    html_path: str,
    parsed_path: str,
) -> None:
    """
    Parse an HTML file produced by the scraper into a structured CSV.

    Parameters
    ----------
    html_path : str
        Path to the HTML file to parse.
    parsed_path : str
        Path to the CSV file to be written with parsed data.
    """
    logger.info(f"[Engine] Start PARSE: {html_path} → {parsed_path}")

    if not os.path.exists(html_path):
        raise FileNotFoundError(f"[Engine] HTML file not found: {html_path}")

    # --- Ensure output directory exists ---
    out_dir = os.path.dirname(parsed_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(html_path, "r", encoding="utf-8") as f:
        html_data = f.read()

    parse_html(html_data=html_data, parsed_path=parsed_path)

    logger.info(f"[Engine] PARSE done: CSV saved to {parsed_path}")


# --- 3) TRANSFORM: CSV parsed → CSV transformed ---
def transform_parsed_file(
    parsed_path: str,
    transformed_path: str,
) -> None:
    """
    Transform parsed CSV data into a cleaned/enriched CSV ready for loading.

    Parameters
    ----------
    parsed_path : str
        Path to the parsed CSV file (output of parse_html_file).
    transformed_path : str
        Path to the transformed CSV file (ready to be loaded into DB).
    """
    logger.info(
        f"[Engine] Start TRANSFORM: parsed={parsed_path} → transformed={transformed_path}"
    )

    if not os.path.exists(parsed_path):
        raise FileNotFoundError(f"[Engine] Parsed CSV not found: {parsed_path}")

    transformer = ETLTransformer()

    # --- Delegate detailed transformation to ETLTransformer ---
    transformer.transform(
        parsed_data=parsed_path,
        transformed_path=transformed_path,
    )

    logger.info(f"[Engine] TRANSFORM done: CSV saved to {transformed_path}")


# --- 4) LOAD: CSV transformed → PostgreSQL + JSON inserted ---
def load_transformed_file(
    transformed_path: str,
    inserted_path: str,
    table_name: str = "scrape_data",
    db_url: Optional[str] = None,
) -> None:
    """
    Load transformed CSV data into PostgreSQL and export inserted records to JSON.

    Parameters
    ----------
    transformed_path : str
        Path to the transformed CSV file.
    inserted_path : str
        Path to the JSON file where inserted records will be stored.
    table_name : str, optional
        Name of the target table in the database. Default is "scrape_data".
    db_url : str | None, optional
        Database URL. If None, it will be resolved from environment variables:
        - DB_URL, or
        - POSTGRES_* (POSTGRES_USER, POSTGRES_PASSWORD, etc.).
    """
    logger.info(
        f"[Engine] Start LOAD: transformed={transformed_path} → table='{table_name}'"
    )

    # --- Ensure output directory for JSON exists ---
    out_dir = os.path.dirname(inserted_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    load_data(
        transformed_data=transformed_path,
        inserted_path=inserted_path,
        table_name=table_name,
        db_url=db_url,
    )

    logger.info(
        f"[Engine] LOAD done: records inserted into '{table_name}', JSON={inserted_path}"
    )


# --- 5) Helper: run full ETL pipeline in a single call ---
def run_full_etl(
    keyword: str,
    html_path: str,
    parsed_path: str,
    transformed_path: str,
    inserted_path: str,
    table_name: str = "scrape_data",
    db_url: Optional[str] = None,
    location: str = "Indonesia",
    headless: bool = False,
    goto_timeout_ms: int = 60_000,
) -> None:
    """
    Run the complete ETL pipeline in one call:
    SCRAPE → PARSE → TRANSFORM → LOAD.

    Parameters
    ----------
    keyword : str
        Car search keyword, e.g. "BMW 3 Series".
    html_path : str
        Path for the scraped HTML file.
    parsed_path : str
        Path for the parsed CSV file.
    transformed_path : str
        Path for the transformed CSV file.
    inserted_path : str
        Path for the JSON file containing inserted records.
    table_name : str, optional
        Target table name in the database, default "scrape_data".
    db_url : str | None, optional
        Database URL; if None, it will be derived from environment variables.
    location : str, optional
        OLX location filter, default "Indonesia".
    headless : bool, optional
        Whether to run the browser in headless mode. Default False.
    goto_timeout_ms : int, optional
        Timeout for page.goto() in milliseconds. Default is 60_000.
    """
    logger.info(f"[Engine] ===== START FULL ETL for keyword='{keyword}' =====")

    # --- Step 1: SCRAPE ---
    scrape_html(
        keyword=keyword,
        html_path=html_path,
        location=location,
        headless=headless,
        goto_timeout_ms=goto_timeout_ms,
    )

    # --- Step 2: PARSE ---
    parse_html_file(
        html_path=html_path,
        parsed_path=parsed_path,
    )

    # --- Step 3: TRANSFORM ---
    transform_parsed_file(
        parsed_path=parsed_path,
        transformed_path=transformed_path,
    )

    # --- Step 4: LOAD ---
    load_transformed_file(
        transformed_path=transformed_path,
        inserted_path=inserted_path,
        table_name=table_name,
        db_url=db_url,
    )

    logger.info(f"[Engine] ===== FULL ETL DONE for keyword='{keyword}' =====")


if __name__ == "__main__":
    # --- Simple CLI entry point (optional) ---
    #
    # Example:
    #   python engine.py "BMW 3 Series"
    #
    import sys

    if len(sys.argv) < 2:
        print("Usage: python engine.py '<keyword>'")
        sys.exit(1)

    kw = sys.argv[1]

    base = kw.replace(" ", "_").lower()
    html = f"data/raw_html/{base}.html"
    parsed = f"data/parsed/{base}.csv"
    transformed = f"data/transformed/{base}_transformed.csv"
    inserted = f"data/inserted/{base}_inserted.json"

    run_full_etl(
        keyword=kw,
        html_path=html,
        parsed_path=parsed,
        transformed_path=transformed,
        inserted_path=inserted,
        table_name="scrape_data",
        db_url=None,
    )
