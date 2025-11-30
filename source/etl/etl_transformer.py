# source/etl/etl_transformer.py

import os
import re

import pandas as pd
from loguru import logger

from source.etl.utils.etl_selector import BASE_URL

MISSING_VALUE = "data not found"


class ETLTransformer:
    """
    ETL Transformer for OLX scraped data.

    Responsibilities:
    - Clean and normalize prices, installments, year/mileage, location, posted time
    - Ensure URLs are absolute
    - Impute missing installment values based on price
    - Output transformed data to CSV
    """

    def __init__(self, base_url: str = BASE_URL, missing_value: str = MISSING_VALUE):
        self.base_url = base_url
        self.missing_value = missing_value

    # --- Helper functions for individual transformations ---
    def _cleanPrice(self, val: str | float) -> float | pd.NA:
        """
        Normalize price string like 'Rp 450.000.000' -> 450000000.0.

        Parameters
        ----------
        val : str | float
            Raw price value from scraped data

        Returns
        -------
        float | pd.NA
        """
        if pd.isna(val):
            return pd.NA

        s = re.sub(r"[^\d]", "", str(val))
        if not s:
            logger.debug(f"[_cleanPrice] Failed to convert value: {val!r}")
            return pd.NA
        try:
            return float(s)
        except ValueError:
            logger.debug(f"[_cleanPrice] ValueError converting: {val!r}")
            return pd.NA

    def _parseYearMileage(self, val: str) -> pd.Series:
        """
        Parse string like "2018 • 70.000-75.000 km" into three numeric columns:
        year, lower_km, upper_km.
        """

        year, lower_km, upper_km = pd.NA, pd.NA, pd.NA

        if pd.isna(val) or val in ["-", self.missing_value]:
            return pd.Series([year, lower_km, upper_km])

        text = str(val)
        nums = re.findall(r"\d[\d\.]*", text)  # extract all numbers

        if not nums:
            logger.debug(f"[_parseYearMileage] No numbers found in value: {val!r}")
            return pd.Series([year, lower_km, upper_km])

        # First number is year
        try:
            year = int(nums[0])
        except ValueError:
            logger.debug(f"[_parseYearMileage] Invalid year token: {nums[0]!r}")
            year = pd.NA

        # Remaining numbers are mileage
        km_vals: list[float] = []
        for num in nums[1:]:
            num_clean = num.replace(".", "")
            try:
                km_vals.append(float(num_clean))
            except ValueError:
                logger.debug(f"[_parseYearMileage] Invalid km value: {num!r}")
                continue

        if len(km_vals) == 1:
            lower_km = upper_km = km_vals[0]
        elif len(km_vals) >= 2:
            lower_km, upper_km = km_vals[:2]

        return pd.Series([year, lower_km, upper_km])

    def _enrichURL(self, val: str) -> str | pd.NA:
        """
        Ensure URL is absolute by prefixing BASE_URL if needed.
        """
        if pd.isna(val) or val == self.missing_value:
            return pd.NA

        s = str(val).strip()
        if not s:
            logger.debug(f"[_enrichURL] Empty URL value: {val!r}")
            return pd.NA
        if s.startswith(("http", "https")):
            return s
        return f"{self.base_url}{s}"

    def _cleanLocation(self, val: str) -> str | pd.NA:
        """
        Simplify location to first segment (e.g., 'Jakarta' from 'Jakarta - Selatan').
        """
        if pd.isna(val) or val == self.missing_value:
            return pd.NA
        s = str(val).strip()

        s = re.split(r"\.| \| | - ", s)[0]
        return s.strip()

    def _cleanInstallments(self, val: str | float) -> float | pd.NA:
        """
        Normalize installment string to float value in IDR.

        Handles:
        - 'Rp 8,9 jt/bulan' -> 8.9e6
        - '8.900.000' -> 8.9e6
        """
        if pd.isna(val) or val == self.missing_value:
            return pd.NA

        s = re.sub(r"[^0-9,\.]", "", str(val).lower())
        if not s:
            logger.debug(f"[_cleanInstallments] Failed to convert value: {val!r}")
            return pd.NA

        # Handle both "8,9" and "8.9" style
        if "," in s and "." in s:
            # Likely thousand + decimal combo, assume "." is thousand sep
            s = s.replace(".", "").replace(",", ".")
        else:
            # Single separator, treat comma as decimal
            s = s.replace(",", ".")

        try:
            base = float(s)
            return base * 1_000_000
        except ValueError:
            logger.debug(f"[_cleanInstallments] ValueError converting: {val!r}")
            return pd.NA

    # --
    def _estimateInstallment(self, price: float | pd.NA) -> float | pd.NA:
        """
        Estimate monthly installment based on price:
        - 30% down payment, 11% other costs, 20% interest, 36 months tenor
        """
        if pd.isna(price):
            return pd.NA

        try:
            price_val = float(price)
        except (TypeError, ValueError):
            logger.debug(f"[_estimateInstallment] Invalid price: {price}")
            return pd.NA

        if price_val <= 0:
            return pd.NA

        down_payment = 0.3 * price_val
        other_costs = 0.11 * price_val
        loan = (price_val - down_payment) + other_costs
        interest = 0.20 * loan
        tenor = 36

        installment = (loan + interest) / tenor
        est = round(installment, 2)

        logger.debug(
            f"[_estimateInstallment] price={price_val:.0f} → estimated_installment={est:.2f}"
        )
        return est

    def _transformPostedTime(self, val: str) -> str | pd.NA:
        """
        Validate posted_time string (max 7 chars).
        """
        if pd.isna(val) or val == self.missing_value:
            return pd.NA
        s = str(val).strip()

        if len(s) > 7:
            logger.debug(f"[_transformPostedTime] Invalid range: {val!r}")
            return pd.NA
        return s

    # --- Main transform workflow ---
    def transform(self, parsed_data: pd.DataFrame | str, transformed_path: str) -> None:
        """
        Full ETL transformation workflow:
        1) Load input CSV or DataFrame
        2) Apply cleaning & enrichment
        3) Impute missing installments based on price
        4) Save transformed CSV
        """
        logger.info("[Transform] Start transform")

        try:
            # --- 1) Load input ---
            if isinstance(parsed_data, pd.DataFrame):
                df = parsed_data.copy()
                logger.debug("[Transform] Input is a DataFrame")
            else:
                if not os.path.exists(parsed_data):
                    raise FileNotFoundError(f"[Transform] No file {parsed_data}")
                logger.debug(f"[Transform] Reading parsed CSV from: {parsed_data}")
                df = pd.read_csv(parsed_data)

            logger.debug(f"[Transform] Initial columns: {list(df.columns)}")

            # Ensure all required columns exist
            required_columns = [
                "price",
                "year_mileage",
                "listing_url",
                "location",
                "installment",
                "posted_time",
            ]
            missing_columns = [c for c in required_columns if c not in df.columns]
            if missing_columns:
                raise KeyError(
                    f"[_transformPostedTime] Missing required columns: {missing_columns}"
                )

            # --- 2) Apply transformations ---
            df["price"] = df["price"].apply(self._cleanPrice)

            # Extract year, lower_km, upper_km from year_mileage
            year_km_df = df["year_mileage"].apply(self._parseYearMileage)
            year_km_df.columns = ["year", "lower_km", "upper_km"]
            df[["year", "lower_km", "upper_km"]] = year_km_df

            df = df.drop(columns=["year_mileage"])

            df["listing_url"] = df["listing_url"].apply(self._enrichURL)
            df["location"] = df["location"].apply(self._cleanLocation)
            df["installment"] = df["installment"].apply(self._cleanInstallments)
            df["posted_time"] = df["posted_time"].apply(self._transformPostedTime)

            # --- 3) Impute missing installments based on price ---
            # default flags: all False (not imputed)
            df["installment_imputed"] = False
            missing_mask = df["installment"].isna()

            if missing_mask.any():
                # Compute installment for missing rows
                df.loc[missing_mask, "installment"] = df.loc[
                    missing_mask, "price"
                ].apply(self._estimateInstallment)

                # Update flag only for rows successfully imputed
                imputed_ok = missing_mask & df["installment"].notna()
                df.loc[imputed_ok, "installment_imputed"] = True

                n_imputed = int(df["installment_imputed"].sum())
                logger.info(
                    f"[Transform] Imputed installment for {n_imputed} rows based on price"
                )
            else:
                logger.info("[Transform] No missing installment; nothing to impute")

            logger.debug(f"[Transform] Dtypes after transform:\n{df.dtypes}")

            # --- 4) Save transformed CSV ---
            out_dir = os.path.dirname(transformed_path)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)

            df.to_csv(transformed_path, index=False, encoding="utf-8")

            logger.info(
                f"[Transform] Completed: {len(df)} rows saved to {transformed_path}"
            )

        except Exception as e:
            logger.error(f"[Transform] Error during transform: {e}")
            raise
