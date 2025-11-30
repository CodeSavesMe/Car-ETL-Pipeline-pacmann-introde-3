# scraps.py

"""
Luigi ETL pipeline for OLX scraping:

Workflow:
1. Scrape    : Playwright → HTML
2. Parse     : HTML → CSV (parsed)
3. Transform : CSV parsed → CSV transformed
4. Load      : CSV transformed → PostgreSQL + JSON inserted

Example run:

python scraps.py Load \
  --keyword "Mitsubishi Pajero Sport" \
  --html-path data/raw_html/pajero.html \
  --parsed-path data/parsed/pajero.csv \
  --transformed-path data/transformed/pajero_transformed.csv \
  --inserted-path data/inserted/pajero_inserted.json
"""

import os
import asyncio

import luigi
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from playwright.async_api import async_playwright

from source.logging_config import configure_logging

from source.etl.etl_scraper import olx_scraper
from source.etl.etl_parser import parse_html
from source.etl.etl_transformer import ETLTransformer
from source.etl.db_loader import load_data

# Load environment variables (.env) for DB_URL, POSTGRES_*, etc.
load_dotenv()

# Simple console logger
logger.add(lambda msg: print(msg, end=""), level="INFO")

# File logger for detailed debugging
configure_logging()


class Scrape(luigi.Task):
    """
    Luigi Task: Scrape OLX search page using Playwright.

    Parameters:
    - keyword: search keyword
    - html_path: output HTML file path
    """

    keyword = luigi.Parameter()
    html_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.html_path)

    def run(self):
        os.makedirs(os.path.dirname(self.html_path), exist_ok=True)

        async def _run_scrape():
            async with async_playwright() as p:
                await olx_scraper(p, self.keyword, self.html_path)

        logger.info(f"[Scrape] Start scraping for keyword='{self.keyword}'")
        asyncio.run(_run_scrape())

        # Check if HTML file was successfully created
        if not os.path.exists(self.html_path):
            raise RuntimeError(
                f"[Scrape] HTML not found at {self.html_path}, scraping failed"
            )

        logger.info(f"[Scrape] HTML saved to {self.html_path}")


# --- 2) Parse Task
class Parse(luigi.Task):
    """
    Luigi Task: Parse HTML → CSV (parsed).

    Parameters:
    - html_path: HTML file from Scrape task
    - parsed_path: CSV output file path
    """

    keyword = luigi.Parameter()
    html_path = luigi.Parameter()
    parsed_path = luigi.Parameter()

    def requires(self):
        # Depends on Scrape task
        return Scrape(keyword=self.keyword, html_path=self.html_path)

    def output(self):
        return luigi.LocalTarget(self.parsed_path)

    def run(self):
        os.makedirs(os.path.dirname(self.parsed_path), exist_ok=True)

        logger.info(f"[Parse] Reading HTML from {self.input().path}")
        with self.input().open("r") as f:
            html_data = f.read()

        # Parse HTML into CSV
        parse_html(html_data, self.output().path)
        logger.info(f"[Parse] Parsed CSV saved to {self.output().path}")


# --- 3) Tranform Task
class Transform(luigi.Task):
    """
    Transform CSV parsed menjadi CSV siap load ke DB.
    - parsed_path: file CSV hasil Parse (data/parsed/...)
    - transformed_path: file CSV hasil Transform (data/transformed/...)
    """

    keyword = luigi.Parameter()
    html_path = luigi.Parameter()
    parsed_path = luigi.Parameter()
    transformed_path = luigi.Parameter()

    def requires(self):
        # Depends on Parse task
        return Parse(
            keyword=self.keyword,
            html_path=self.html_path,
            parsed_path=self.parsed_path,
        )

    def output(self):
        return luigi.LocalTarget(self.transformed_path)

    def run(self):
        input_csv_path = self.input().path
        logger.info(f"[Transform] Transforming parsed CSV: {input_csv_path}")

        # Load parsed CSV
        df_parsed = pd.read_csv(input_csv_path)

        # Transform parsed DataFrame
        transformer = ETLTransformer()
        transformer.transform(
            parsed_data=df_parsed,  # << kirim DataFrame, bukan path string
            transformed_path=self.output().path,  # << lokasi output Luigi
        )

        logger.info(f"[Transform] Done. Saved transformed CSV to {self.output().path}")


# --- 4) Load Task
class Load(luigi.Task):
    """
    Luigi Task: Load transformed CSV → PostgreSQL + JSON inserted.

    Parameters:
    - transformed_path: CSV from Transform task
    - inserted_path: JSON file to save inserted records
    """

    keyword = luigi.Parameter()
    html_path = luigi.Parameter()
    parsed_path = luigi.Parameter()
    transformed_path = luigi.Parameter()
    inserted_path = luigi.Parameter()

    def requires(self):
        # Depends on Transform task
        return Transform(
            keyword=self.keyword,
            html_path=self.html_path,
            parsed_path=self.parsed_path,
            transformed_path=self.transformed_path,
        )

    def output(self):
        return luigi.LocalTarget(self.inserted_path)

    def run(self):
        os.makedirs(os.path.dirname(self.inserted_path), exist_ok=True)

        logger.info(f"[Load] Loading data into DB from {self.input().path}")
        # db_url=None → will use DB_URL / POSTGRES_* from .env
        load_data(
            transformed_data=self.input().path,
            inserted_path=self.output().path,
            table_name="scrape_data",
            db_url=None,
        )
        logger.info(f"[Load] Inserted data JSON saved to {self.output().path}")


if __name__ == "__main__":
    # Run Luigi CLI
    luigi.run()
