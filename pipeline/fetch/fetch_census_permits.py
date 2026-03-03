"""
Fetches building permit data from Census BPS text files (historical)
and Census API (recent). Outputs one CSV per year to /data/raw/census_permits/.
"""

import io
import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

from pipeline.utils.cbsa_utils import load_pipeline_config, get_cbsa_codes

logger = logging.getLogger("pipeline.fetch.census_permits")

BPS_BASE_URL = "https://www.census.gov/construction/bps/txt"
CENSUS_API_URL = "https://api.census.gov/data/timeseries/eits/bps"
GEOCODES_URL = f"{BPS_BASE_URL}/geocodes.txt"


def fetch_geocodes(raw_dir: str) -> pd.DataFrame:
    """Download BPS-to-CBSA geocode crosswalk."""
    out_path = os.path.join(raw_dir, "bps_geocodes.csv")
    if os.path.exists(out_path):
        age_days = (time.time() - os.path.getmtime(out_path)) / 86400
        if age_days < 90:
            logger.info("Geocodes file is recent, reusing")
            return pd.read_csv(out_path, dtype=str)

    logger.info("Downloading BPS geocodes crosswalk")
    resp = requests.get(GEOCODES_URL, timeout=60)
    resp.raise_for_status()

    lines = resp.text.strip().split("\n")
    records = []
    for line in lines:
        parts = line.split("|") if "|" in line else line.split(",")
        if len(parts) >= 4:
            records.append({
                "bps_code": parts[0].strip(),
                "cbsa_code": parts[1].strip() if len(parts) > 1 else "",
                "state_code": parts[2].strip() if len(parts) > 2 else "",
                "area_name": parts[3].strip() if len(parts) > 3 else "",
            })

    df = pd.DataFrame(records)
    df.to_csv(out_path, index=False)
    return df


def fetch_annual_text_file(year: int, raw_dir: str) -> pd.DataFrame:
    """Download and parse an annual BPS text file."""
    url = f"{BPS_BASE_URL}/tb3u{year}.txt"
    logger.info(f"Fetching BPS annual file: {url}")

    resp = requests.get(url, timeout=120)
    if resp.status_code == 404:
        logger.warning(f"BPS file not found for year {year}")
        return pd.DataFrame()
    resp.raise_for_status()

    lines = resp.text.strip().split("\n")
    records = []
    for line in lines:
        parts = line.split(",") if "," in line else line.split("|")
        if len(parts) >= 7:
            try:
                records.append({
                    "cbsa_code": parts[0].strip(),
                    "cbsa_name": parts[1].strip(),
                    "year": year,
                    "permits_total": int(parts[2].strip()) if parts[2].strip() else 0,
                    "permits_sf": int(parts[3].strip()) if parts[3].strip() else 0,
                    "permits_mf_small": int(parts[4].strip()) if parts[4].strip() else 0,
                    "permits_mf_large": int(parts[5].strip()) if parts[5].strip() else 0,
                })
            except (ValueError, IndexError):
                continue

    return pd.DataFrame(records)


def fetch_permits_api(year: int, raw_dir: str, api_key: str, delay: float = 0.5) -> pd.DataFrame:
    """Fetch permit data via Census API for recent years."""
    logger.info(f"Fetching permits via Census API for {year}")
    records = []

    for month in range(1, 13):
        params = {
            "get": "PERMITS,UNITS",
            "for": "metropolitan statistical area/micropolitan statistical area:*",
            "time": f"{year}-{month:02d}",
        }
        if api_key:
            params["key"] = api_key

        try:
            resp = requests.get(CENSUS_API_URL, params=params, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                if len(data) > 1:
                    headers = data[0]
                    for row in data[1:]:
                        record = dict(zip(headers, row))
                        records.append(record)
            time.sleep(delay)
        except Exception as e:
            logger.warning(f"API call failed for {year}-{month:02d}: {e}")
            continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    return df


def run(config: dict = None) -> dict:
    """Main fetch entry point. Returns status dict."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "census_permits")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    api_key = config["fetch"].get("census_api_key", "")
    start_year = config["fetch"]["start_year"]
    end_year = config["fetch"]["end_year"]
    delay = config["fetch"]["request_delay_seconds"]
    target_cbsas = set(get_cbsa_codes())

    files_written = 0
    total_rows = 0

    # Fetch geocodes crosswalk
    try:
        fetch_geocodes(raw_dir)
    except Exception as e:
        logger.error(f"Failed to fetch geocodes: {e}")

    # Fetch annual text files for historical years
    for year in range(start_year, end_year - 1):
        out_path = os.path.join(raw_dir, f"census_permits_metro_annual_{year}.csv")
        if os.path.exists(out_path):
            logger.info(f"Skipping {year}, file exists")
            continue

        try:
            df = fetch_annual_text_file(year, raw_dir)
            if not df.empty:
                df["cbsa_code"] = df["cbsa_code"].astype(str).str.zfill(5)
                # DQ: row count check
                if len(df) < 50:
                    logger.warning(f"Year {year}: only {len(df)} rows (expected > 300)")
                # DQ: no negative permits
                neg = df[df["permits_total"] < 0]
                if not neg.empty:
                    logger.warning(f"Year {year}: {len(neg)} rows with negative permits")

                df.to_csv(out_path, index=False)
                files_written += 1
                total_rows += len(df)
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Failed to fetch permits for {year}: {e}")

    # Fetch recent years via API
    for year in range(max(end_year - 1, start_year), end_year + 1):
        out_path = os.path.join(raw_dir, f"census_permits_metro_annual_{year}.csv")
        if os.path.exists(out_path):
            logger.info(f"Skipping {year}, file exists")
            continue

        try:
            df = fetch_permits_api(year, raw_dir, api_key, delay)
            if not df.empty:
                df.to_csv(out_path, index=False)
                files_written += 1
                total_rows += len(df)
        except Exception as e:
            logger.error(f"Failed to fetch permits API for {year}: {e}")

    return {
        "source": "census_permits",
        "status": "SUCCESS" if files_written > 0 else "FAILED",
        "files_written": files_written,
        "rows_fetched": total_rows,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
