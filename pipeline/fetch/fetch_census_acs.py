"""
Fetches ACS 1-year estimates for housing variables via Census API.
"""

import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

from pipeline.utils.cbsa_utils import load_pipeline_config, get_cbsa_codes

logger = logging.getLogger("pipeline.fetch.census_acs")

ACS_BASE_URL = "https://api.census.gov/data"

ACS_VARIABLES = {
    "B25001_001E": "total_housing_units",
    "B25002_002E": "occupied_units",
    "B25002_003E": "vacant_units",
    "B25003_002E": "owner_occupied",
    "B25003_003E": "renter_occupied",
    "B25010_001E": "avg_household_size",
    "B19013_001E": "median_hh_income",
    "B25077_001E": "median_home_value",
    "B25064_001E": "median_gross_rent",
}


def fetch_acs_year(year: int, api_key: str, delay: float = 0.5) -> pd.DataFrame:
    """Fetch ACS 1-year data for a single year."""
    url = f"{ACS_BASE_URL}/{year}/acs/acs1"

    var_list = ",".join(["NAME"] + list(ACS_VARIABLES.keys()))
    params = {
        "get": var_list,
        "for": "metropolitan statistical area/micropolitan statistical area:*",
    }
    if api_key:
        params["key"] = api_key

    logger.info(f"Fetching ACS 1-year for {year}")
    resp = requests.get(url, params=params, timeout=60)

    if resp.status_code != 200:
        logger.warning(f"ACS API returned {resp.status_code} for year {year}")
        return pd.DataFrame()

    data = resp.json()
    if len(data) <= 1:
        return pd.DataFrame()

    headers = data[0]
    records = [dict(zip(headers, row)) for row in data[1:]]
    df = pd.DataFrame(records)

    # Rename variables
    rename_map = {"NAME": "cbsa_name"}
    rename_map.update(ACS_VARIABLES)
    df = df.rename(columns=rename_map)

    # Extract CBSA code
    geo_col = [c for c in df.columns if "metropolitan" in c.lower()]
    if geo_col:
        df["cbsa_code"] = df[geo_col[0]].astype(str).str.zfill(5)

    df["year"] = year
    df["acs_type"] = "1yr"

    # Convert numeric columns
    numeric_cols = list(ACS_VARIABLES.values())
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    time.sleep(delay)
    return df


def run(config: dict = None) -> dict:
    """Main fetch entry point."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "census_acs")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    api_key = config["fetch"].get("census_api_key", "")
    delay = config["fetch"]["request_delay_seconds"]
    end_year = config["fetch"]["end_year"]
    target_cbsas = set(get_cbsa_codes())

    files_written = 0
    files_existing = 0
    total_rows = 0

    # ACS 1-year available from 2005, skip 2020 (not released)
    for year in range(2005, end_year + 1):
        if year == 2020:
            logger.info("Skipping ACS 2020 (not released due to COVID)")
            continue

        out_path = os.path.join(raw_dir, f"census_acs_metro_{year}.csv")
        if os.path.exists(out_path):
            logger.info(f"Skipping {year}, file exists")
            files_existing += 1
            continue

        try:
            df = fetch_acs_year(year, api_key, delay)
            if not df.empty:
                # DQ: check non-null for target CBSAs
                target_rows = df[df["cbsa_code"].isin(target_cbsas)]
                null_count = target_rows[list(ACS_VARIABLES.values())].isnull().sum().sum()
                if null_count > 0:
                    logger.warning(f"ACS {year}: {null_count} null values for target CBSAs")

                # DQ: occupied + vacant ≈ total (within 2%)
                if "total_housing_units" in df.columns and "occupied_units" in df.columns:
                    check = df.dropna(subset=["total_housing_units", "occupied_units", "vacant_units"])
                    if not check.empty:
                        diff = abs(check["occupied_units"] + check["vacant_units"] - check["total_housing_units"])
                        pct_diff = diff / check["total_housing_units"]
                        bad = check[pct_diff > 0.02]
                        if not bad.empty:
                            logger.warning(f"ACS {year}: {len(bad)} rows with occupied+vacant != total")

                # DQ: median income sanity
                if "median_hh_income" in df.columns:
                    low_income = df[df["median_hh_income"] <= 20000].dropna(subset=["median_hh_income"])
                    if not low_income.empty:
                        logger.warning(f"ACS {year}: {len(low_income)} rows with median income <= $20k")

                df.to_csv(out_path, index=False)
                files_written += 1
                total_rows += len(df)
        except Exception as e:
            logger.error(f"Failed to fetch ACS for {year}: {e}")

    return {
        "source": "census_acs",
        "status": "SUCCESS" if (files_written + files_existing) > 0 else "FAILED",
        "files_written": files_written,
        "rows_fetched": total_rows,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
