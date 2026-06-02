"""
Fetches building permit data from Census BPS files (historical)
and Census API (recent). Outputs one CSV per year to /data/raw/census_permits/.

URL patterns by era:
  - Pre-2019:  txt at census.gov/construction/bps/txt/tb3u{year}.txt
  - 2019-2023: xls at census.gov/construction/bps/xls/msaannual_{year}99.xls
  - 2024+:     xls at census.gov/construction/bps/xls/cbsaannual_{year}99.xls
  - Fallback:  Census EITS API for all years (requires CENSUS_API_KEY)
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

BPS_BASE_URL = "https://www.census.gov/construction/bps"
CENSUS_API_URL = "https://api.census.gov/data/timeseries/eits/bps"
GEOCODES_URL = f"{BPS_BASE_URL}/txt/geocodes.txt"


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


def fetch_annual_text_file(year: int) -> pd.DataFrame:
    """Download and parse a pre-2019 annual BPS metro text file."""
    url = f"{BPS_BASE_URL}/txt/tb3u{year}.txt"
    logger.info(f"Fetching BPS annual text file: {url}")

    resp = requests.get(url, timeout=120)
    if resp.status_code == 404:
        logger.warning(f"BPS text file not found for year {year}")
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


def fetch_annual_excel_file(year: int) -> pd.DataFrame:
    """Download and parse a 2019+ annual BPS metro Excel file."""
    if year >= 2024:
        urls = [
            f"{BPS_BASE_URL}/xls/cbsaannual_{year}99.xls",
            f"{BPS_BASE_URL}/xls/msaannual_{year}99.xls",
        ]
    else:
        urls = [
            f"{BPS_BASE_URL}/xls/msaannual_{year}99.xls",
            f"{BPS_BASE_URL}/xls/msaannual_{year}prelim.xls",
        ]

    for url in urls:
        logger.info(f"Trying BPS Excel file: {url}")
        try:
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200 and len(resp.content) > 1000:
                df = pd.read_excel(io.BytesIO(resp.content), dtype=str)
                df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

                cbsa_col = None
                for candidate in ["cbsa_code", "csa", "code", "cbsa"]:
                    if candidate in df.columns:
                        cbsa_col = candidate
                        break
                if not cbsa_col:
                    cbsa_col = df.columns[0]

                name_col = None
                for candidate in ["cbsa_name", "name", "area_name", "cbsa_title"]:
                    if candidate in df.columns:
                        name_col = candidate
                        break
                if not name_col and len(df.columns) > 1:
                    name_col = df.columns[1]

                total_col = None
                for candidate in ["total", "units", "total_units", "permits"]:
                    if candidate in df.columns:
                        total_col = candidate
                        break

                result = pd.DataFrame({
                    "cbsa_code": df[cbsa_col].astype(str).str.strip(),
                    "cbsa_name": df[name_col].astype(str).str.strip() if name_col else "",
                    "year": year,
                    "permits_total": pd.to_numeric(
                        df[total_col] if total_col else df.iloc[:, 2],
                        errors="coerce"
                    ).fillna(0).astype(int),
                })
                return result
        except Exception as e:
            logger.debug(f"Excel URL failed: {url} - {e}")
            continue

    logger.warning(f"BPS Excel file not found for year {year}")
    return pd.DataFrame()


def fetch_permits_api(year: int, api_key: str, delay: float = 0.5) -> pd.DataFrame:
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

    return pd.DataFrame(records)


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

    files_written = 0
    files_existing = 0
    total_rows = 0

    try:
        fetch_geocodes(raw_dir)
    except Exception as e:
        logger.error(f"Failed to fetch geocodes: {e}")

    for year in range(start_year, end_year + 1):
        out_path = os.path.join(raw_dir, f"census_permits_metro_annual_{year}.csv")
        if os.path.exists(out_path):
            logger.info(f"Skipping {year}, file exists")
            files_existing += 1
            continue

        df = pd.DataFrame()

        # Try the right source for the era
        try:
            if year < 2019:
                df = fetch_annual_text_file(year)
            else:
                df = fetch_annual_excel_file(year)
        except Exception as e:
            logger.warning(f"File fetch failed for {year}: {e}")

        # Fall back to Census API if file fetch failed
        if df.empty and api_key:
            try:
                df = fetch_permits_api(year, api_key, delay)
            except Exception as e:
                logger.error(f"API fetch also failed for {year}: {e}")

        if not df.empty:
            if "cbsa_code" in df.columns:
                df["cbsa_code"] = df["cbsa_code"].astype(str).str.zfill(5)
            if len(df) < 50:
                logger.warning(f"Year {year}: only {len(df)} rows (expected > 300)")
            if "permits_total" in df.columns:
                neg = df[df["permits_total"] < 0]
                if not neg.empty:
                    logger.warning(f"Year {year}: {len(neg)} rows with negative permits")

            df.to_csv(out_path, index=False)
            files_written += 1
            total_rows += len(df)

        time.sleep(delay)

    return {
        "source": "census_permits",
        "status": "SUCCESS" if (files_written + files_existing) > 0 else "FAILED",
        "files_written": files_written,
        "rows_fetched": total_rows,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
