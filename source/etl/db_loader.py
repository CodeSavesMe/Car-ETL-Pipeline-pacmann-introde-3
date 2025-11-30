import os
import json
from typing import Union

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError


def get_db_url(db_url: str | None = None) -> str:
    """
    Determine the database URL to use, following priority:

    1) Explicit function argument
    2) Environment variable DB_URL
    3) Build from individual POSTGRES_* environment variables
    """

    # --- 1) Use explicit function argument if provided ---
    if db_url:
        logger.info("[DB] Using db_url from function argument")
        return db_url

    # --- 2) Use DB_URL from environment variable ---
    env_db_url = os.getenv("DB_URL")
    if env_db_url:
        logger.info("[DB] Using DB_URL from environment")
        return env_db_url

    # --- 3) Build URL from individual POSTGRES_* env vars ---
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db = os.getenv("POSTGRES_DB", "scrape-olx")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5435")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    safe_url = f"postgresql+psycopg2://{user}:***@{host}:{port}/{db}"
    logger.info(f"[DB] Built DB URL from env: {safe_url}")
    return url


class DBLoader:
    """
    Database Loader for inserting transformed OLX data into PostgreSQL.

    Responsibilities:
    - Load transformed CSV or DataFrame
    - Clean data (NaN → NULL, year → int)
    - Drop helper columns not present in DB schema
    - Insert records using SQLAlchemy with transaction support
    - Save inserted rows to a JSON file for auditing
    """

    def __init__(self, db_url: str | None = None):
        """
        Initialize DBLoader.

        Parameters
        ----------
        db_url : str | None
            Optional explicit database URL. If None, get_db_url() will resolve.
        """
        self.db_url = get_db_url(db_url)

    @staticmethod
    def _normalize_record(raw: dict) -> dict:
        """
        Normalize a row dict before inserting into DB.

        Rules:
        - pd.NA / NaN → None
        - 'year' column → cast to int, if fail set to None
        """

        def _norm(key: str, value):
            # --- Treat explicit None as is ---
            if value is None:
                return None

            # --- Pandas missing markers (pd.NA, NaN) -> None ---
            if value is pd.NA or (isinstance(value, float) and pd.isna(value)):
                return None

            # --- Special handling for 'year' (INT column in DB) ---
            if key == "year":
                try:
                    # allow float 2015.0 or string "2015"
                    return int(float(value))
                except (TypeError, ValueError, OverflowError) as e:
                    logger.warning(
                        f"[Load] Invalid 'year' value {value!r} "
                        f"({type(value)}): {e}. Stored as NULL."
                    )
                    return None

            # --- Default: keep original value ---
            return value

        return {k: _norm(k, v) for k, v in raw.items()}

    def load(
        self,
        transformed_data: Union[str, pd.DataFrame],
        inserted_path: str,
        table_name: str,
    ) -> None:
        """
        Load transformed data into PostgreSQL table.

        Parameters
        ----------
        transformed_data : str | pd.DataFrame
            File path to CSV or in-memory DataFrame
        inserted_path : str
            JSON file path to save inserted records
        table_name : str
            Name of the target database table
        """

        # --- 1) Load transformed data ---
        if isinstance(transformed_data, pd.DataFrame):
            df = transformed_data.copy()
            logger.info(
                f"[Load] Using in-memory DataFrame with {len(df)} rows "
                f"to insert into '{table_name}'"
            )
        else:
            if not os.path.exists(transformed_data):
                raise FileNotFoundError(f"[Load] File not found: {transformed_data}")
            logger.info(
                f"[Load] Reading transformed CSV from '{transformed_data}' "
                f"for table '{table_name}'"
            )
            df = pd.read_csv(transformed_data)

        # --- 2) Skip if no data ---
        if df.empty:
            logger.info("[Load] No data to insert. Skipping DB insert.")
            _ensure_dir(inserted_path)
            with open(inserted_path, "w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            return

        logger.debug(f"[Load] DataFrame dtypes before cleaning:\n{df.dtypes}")

        # --- 3) Drop helper columns not in DB schema ---
        if "installment_imputed" in df.columns:
            logger.info(
                "[Load] Dropping helper column 'installment_imputed' before insert"
            )
            df_for_db = df.drop(columns=["installment_imputed"])
        else:
            df_for_db = df

        # --- 4) Convert each row to a normalized Python dict ---
        records: list[dict] = []
        for idx, (_, row) in enumerate(df_for_db.iterrows()):
            raw = row.to_dict()
            rec = self._normalize_record(raw)
            records.append(rec)

            # Optional: debug log for first few rows
            if idx < 3:
                logger.debug(f"[Load] Sample normalized row {idx}: {rec}")

        logger.info(
            f"[Load] Prepared {len(records)} normalized rows "
            f"to insert into '{table_name}'"
        )

        if not records:
            logger.info("[Load] No records after conversion. Skipping DB insert.")
            _ensure_dir(inserted_path)
            with open(inserted_path, "w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            return

        # --- 5) Insert into database using SQLAlchemy ---
        engine = create_engine(self.db_url)
        metadata = MetaData()

        try:
            # Use transaction context – auto commit/rollback
            with engine.begin() as conn:
                # Reflect database schema
                metadata.reflect(bind=conn)

                table: Table | None = metadata.tables.get(table_name)
                if table is None:
                    raise RuntimeError(
                        f"[Load] Table '{table_name}' not found in database."
                    )

                try:
                    conn.execute(table.insert(), records)
                except SQLAlchemyError as e:
                    logger.error(
                        f"[Load] Error while inserting into '{table_name}': {e}"
                    )
                    raise RuntimeError(
                        f"Error while inserting into '{table_name}': {e}"
                    ) from e

            # --- 6) Save inserted records to JSON ---
            _ensure_dir(inserted_path)
            with open(inserted_path, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)

            logger.info(
                f"[Load] Inserted {len(records)} rows into '{table_name}'. "
                f"JSON saved to: {inserted_path}"
            )

        finally:
            engine.dispose()


def _ensure_dir(path: str) -> None:
    """
    Ensure directory exists for given file path.
    """
    dir_name = os.path.dirname(path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)


def load_data(
    transformed_data: Union[str, pd.DataFrame],
    inserted_path: str,
    table_name: str,
    db_url: str | None = None,
) -> None:
    """
    Convenience function to load data into DB using DBLoader.

    Parameters
    ----------
    transformed_data : str | pd.DataFrame
        CSV path or DataFrame to insert
    inserted_path : str
        JSON path to save inserted records
    table_name : str
        Target table name
    db_url : str | None
        Optional database URL
    """
    loader = DBLoader(db_url=db_url)
    loader.load(
        transformed_data=transformed_data,
        inserted_path=inserted_path,
        table_name=table_name,
    )
