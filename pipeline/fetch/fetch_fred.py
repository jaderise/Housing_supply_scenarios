"""
Fetches all required FRED series via API.
"""

import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

from pipeline.utils.cbsa_utils import load_pipeline_config

logger = logging.getLogger("pipeline.fetch.fred")

FRED_API_URL = "https://api.fred.stlouisfed.org/series/observations"

SERIES = [
    "HOUST",         # Housing Starts: Total (national), Monthly
    "HOUSTNE",       # Housing Starts: Northeast, Monthly
    "HOUSTMW",       # Housing Starts: Midwest, Monthly
    "HOUSTS",        # Housing Starts: South, Monthly
    "HOUSTW",        # Housing Starts: West, Monthly
    "COMPUTSA",      # Housing Completions: Total (national), Monthly
    "RVACRATE",      # Rental Vacancy Rate, Quarterly
    "HCOVACRATE",    # Homeowner Vacancy Rate, Quarterly
    "TTLHHLD",       # Total Households (CPS), Annual
    "MORTGAGE30US",  # 30-Year Fixed Mortgage Rate, Weekly
    "MSPUS",         # Median Sales Price of Houses Sold, Quarterly
    "CPIAUCSL",      # CPI for All Urban Consumers (for inflation adjustment)
]


def fetch_series(series_id: str, api_key: str, start_date: str = "2000-01-01") -> pd.DataFrame:
    """Fetch a single FRED series."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
    }

    logger.info(f"Fetching FRED series: {series_id}")
    resp = requests.get(FRED_API_URL, params=params, timeout=60)

    if resp.status_code != 200:
        logger.error(f"FRED API returned {resp.status_code} for {series_id}")
        return pd.DataFrame()

    data = resp.json()
    observations = data.get("observations", [])

    if not observations:
        logger.warning(f"FRED series {series_id}: no observations returned")
        return pd.DataFrame()

    df = pd.DataFrame(observations)
    df = df[["date", "value"]].copy()
    df["series_id"] = series_id

    # Replace "." with null
    df["value"] = df["value"].replace(".", None)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df["pull_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

    return df


def run(config: dict = None) -> dict:
    """Main fetch entry point."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "fred")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    api_key = config["fetch"].get("fred_api_key", "")
    delay = config["fetch"]["request_delay_seconds"]

    if not api_key:
        logger.error("FRED_API_KEY not set")
        return {"source": "fred", "status": "FAILED", "files_written": 0, "rows_fetched": 0}

    files_written = 0
    total_rows = 0

    for series_id in SERIES:
        out_path = os.path.join(raw_dir, f"fred_{series_id}.csv")

        try:
            df = fetch_series(series_id, api_key)
            if not df.empty:
                # DQ: check observation count
                if len(df) < 100 and series_id not in ("TTLHHLD",):
                    logger.warning(f"FRED {series_id}: only {len(df)} observations (expected > 100)")

                # DQ: check null rate
                null_rate = df["value"].isnull().mean()
                if null_rate > 0.10:
                    logger.warning(f"FRED {series_id}: {null_rate:.1%} null values")

                df.to_csv(out_path, index=False)
                files_written += 1
                total_rows += len(df)

            time.sleep(delay)
        except Exception as e:
            logger.error(f"Failed to fetch FRED {series_id}: {e}")

    return {
        "source": "fred",
        "status": "SUCCESS" if files_written == len(SERIES) else "PARTIAL",
        "files_written": files_written,
        "rows_fetched": total_rows,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
