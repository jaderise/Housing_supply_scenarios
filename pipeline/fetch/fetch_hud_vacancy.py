"""
Downloads HUD USPS vacancy files and zip-to-CBSA crosswalk files.
Parses Excel files and writes CSVs.
"""

import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

from pipeline.utils.cbsa_utils import load_pipeline_config

logger = logging.getLogger("pipeline.fetch.hud_vacancy")

HUD_VACANCY_BASE = "https://www.huduser.gov/portal/datasets/usps"
HUD_CROSSWALK_BASE = "https://www.huduser.gov/portal/datasets/usps_crosswalk"


def fetch_vacancy_quarter(year: int, quarter: int, raw_dir: str, delay: float = 0.5) -> bool:
    """Download and parse a single quarter's vacancy data."""
    out_path = os.path.join(raw_dir, f"hud_vacancy_zip_{year}Q{quarter}.csv")
    if os.path.exists(out_path):
        logger.info(f"Skipping {year}Q{quarter} vacancy, file exists")
        return True

    # Try common URL patterns
    urls = [
        f"{HUD_VACANCY_BASE}/USPS_Vacancy_{year}Q{quarter}.xlsx",
        f"{HUD_VACANCY_BASE}/USPS_ZCTA_CITY_AP_{year}{quarter}.xlsx",
        f"{HUD_VACANCY_BASE}/TABLE3_ALLSTATESANDUS_QTR{quarter}_{year}.xlsx",
    ]

    for url in urls:
        try:
            logger.info(f"Trying vacancy URL: {url}")
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200:
                df = pd.read_excel(
                    pd.io.common.BytesIO(resp.content),
                    dtype={"zip": str, "ZIP": str, "Zip Code": str},
                )
                # Normalize column names
                df.columns = df.columns.str.lower().str.replace(" ", "_")

                # Standardize zip column
                for col in ["zip", "zip_code", "zipcode"]:
                    if col in df.columns:
                        df = df.rename(columns={col: "zip"})
                        break

                df.to_csv(out_path, index=False)
                logger.info(f"Wrote {len(df)} rows for {year}Q{quarter}")
                time.sleep(delay)
                return True
        except Exception as e:
            logger.debug(f"URL failed: {url} - {e}")
            continue

    logger.warning(f"Could not fetch vacancy data for {year}Q{quarter}")
    return False


def fetch_crosswalk_quarter(year: int, quarter: int, raw_dir: str, delay: float = 0.5) -> bool:
    """Download zip-to-CBSA crosswalk for a quarter."""
    out_path = os.path.join(raw_dir, f"hud_cbsa_crosswalk_{year}Q{quarter}.csv")
    if os.path.exists(out_path):
        logger.info(f"Skipping {year}Q{quarter} crosswalk, file exists")
        return True

    urls = [
        f"{HUD_CROSSWALK_BASE}/ZIP_CBSA_{year}Q{quarter}.xlsx",
        f"{HUD_CROSSWALK_BASE}/ZIP_CBSA_{year}{quarter}.xlsx",
    ]

    for url in urls:
        try:
            logger.info(f"Trying crosswalk URL: {url}")
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200:
                df = pd.read_excel(
                    pd.io.common.BytesIO(resp.content),
                    dtype=str,
                )
                df.columns = df.columns.str.lower().str.replace(" ", "_")
                df.to_csv(out_path, index=False)
                logger.info(f"Wrote crosswalk with {len(df)} rows for {year}Q{quarter}")
                time.sleep(delay)
                return True
        except Exception as e:
            logger.debug(f"Crosswalk URL failed: {url} - {e}")
            continue

    logger.warning(f"Could not fetch crosswalk for {year}Q{quarter}")
    return False


def run(config: dict = None) -> dict:
    """Main fetch entry point."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "hud_vacancy")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    delay = config["fetch"]["request_delay_seconds"]

    files_written = 0
    total_rows = 0

    # Fetch from 2015 Q1 to current
    for year in range(2015, config["fetch"]["end_year"] + 1):
        for quarter in range(1, 5):
            vac_ok = fetch_vacancy_quarter(year, quarter, raw_dir, delay)
            xw_ok = fetch_crosswalk_quarter(year, quarter, raw_dir, delay)
            if vac_ok:
                files_written += 1
            if xw_ok:
                files_written += 1

    return {
        "source": "hud_vacancy",
        "status": "SUCCESS" if files_written > 0 else "PARTIAL",
        "files_written": files_written,
        "rows_fetched": total_rows,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
