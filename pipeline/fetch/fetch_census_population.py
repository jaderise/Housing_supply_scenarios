"""
Fetches Population Estimates Program data via Census API.
One call per vintage year.
"""

import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

from pipeline.utils.cbsa_utils import load_pipeline_config, get_cbsa_codes

logger = logging.getLogger("pipeline.fetch.census_population")

PEP_BASE_URL = "https://api.census.gov/data"


def fetch_pep_vintage(vintage_year: int, api_key: str, delay: float = 0.5) -> pd.DataFrame:
    """Fetch population data for a single PEP vintage year."""
    url = f"{PEP_BASE_URL}/{vintage_year}/pep/population"

    params = {
        "get": "NAME,POP,DOMESTICMIG,INTERNATIONALMIG,NATURALINC",
        "for": "metropolitan statistical area/micropolitan statistical area:*",
    }
    if api_key:
        params["key"] = api_key

    logger.info(f"Fetching PEP vintage {vintage_year}")
    resp = requests.get(url, params=params, timeout=60)

    if resp.status_code != 200:
        logger.warning(f"PEP API returned {resp.status_code} for vintage {vintage_year}")
        return pd.DataFrame()

    data = resp.json()
    if len(data) <= 1:
        return pd.DataFrame()

    headers = data[0]
    records = [dict(zip(headers, row)) for row in data[1:]]
    df = pd.DataFrame(records)

    # Standardize column names
    col_map = {
        "NAME": "cbsa_name",
        "POP": "population",
        "DOMESTICMIG": "domestic_migration_net",
        "INTERNATIONALMIG": "international_migration_net",
        "NATURALINC": "natural_increase",
    }
    df = df.rename(columns=col_map)

    # Extract CBSA code from the geo column
    geo_col = [c for c in df.columns if "metropolitan" in c.lower() or c == "us"]
    if geo_col:
        df["cbsa_code"] = df[geo_col[0]].astype(str).str.zfill(5)
    else:
        df["cbsa_code"] = ""

    df["pep_vintage"] = vintage_year

    # Convert numeric columns
    for col in ["population", "domestic_migration_net", "international_migration_net", "natural_increase"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    time.sleep(delay)
    return df


def run(config: dict = None) -> dict:
    """Main fetch entry point."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "census_population")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    api_key = config["fetch"].get("census_api_key", "")
    delay = config["fetch"]["request_delay_seconds"]
    target_cbsas = set(get_cbsa_codes())

    files_written = 0
    files_existing = 0
    total_rows = 0

    # PEP vintages available: 2010-2023 (adjust as newer vintages become available)
    for vintage in range(2010, 2025):
        out_path = os.path.join(raw_dir, f"census_population_metro_{vintage}.csv")
        if os.path.exists(out_path):
            logger.info(f"Skipping vintage {vintage}, file exists")
            files_existing += 1
            continue

        try:
            df = fetch_pep_vintage(vintage, api_key, delay)
            if not df.empty:
                # DQ: population must be > 0
                invalid = df[df["population"] <= 0]
                if not invalid.empty:
                    logger.warning(f"Vintage {vintage}: {len(invalid)} rows with population <= 0")

                # DQ: check all 50 target CBSAs
                present = set(df["cbsa_code"].unique())
                missing = target_cbsas - present
                if missing:
                    logger.warning(f"Vintage {vintage}: missing CBSAs: {missing}")

                df.to_csv(out_path, index=False)
                files_written += 1
                total_rows += len(df)
        except Exception as e:
            logger.error(f"Failed to fetch PEP vintage {vintage}: {e}")

    return {
        "source": "census_population",
        "status": "SUCCESS" if (files_written + files_existing) > 0 else "FAILED",
        "files_written": files_written,
        "rows_fetched": total_rows,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
