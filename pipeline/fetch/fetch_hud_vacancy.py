"""
Downloads HUD USPS vacancy data via the HUD User API.
Requires HUD_API_KEY (Bearer token from huduser.gov).

API docs: https://www.huduser.gov/portal/dataset/uspsncwm-api.html
"""

import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

from pipeline.utils.cbsa_utils import load_pipeline_config, get_cbsa_codes

logger = logging.getLogger("pipeline.fetch.hud_vacancy")

HUD_API_BASE = "https://www.huduser.gov/hudapi/public"


def _hud_get(endpoint: str, api_key: str, params: dict = None, timeout: int = 60) -> dict | None:
    """Make an authenticated GET request to the HUD API."""
    url = f"{HUD_API_BASE}/{endpoint}"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        logger.warning(f"HUD API {endpoint}: HTTP {resp.status_code}")
        return None
    except Exception as e:
        logger.warning(f"HUD API {endpoint} failed: {e}")
        return None


def fetch_vacancy_by_cbsa(cbsa_code: str, year: int, quarter: int,
                          api_key: str, delay: float = 0.5) -> pd.DataFrame:
    """Fetch vacancy data for a single CBSA via the HUD USPS API."""
    data = _hud_get("usps", api_key, params={
        "type": "3",
        "query": cbsa_code,
        "year": str(year),
        "quarter": str(quarter),
    })

    if not data:
        return pd.DataFrame()

    records = data if isinstance(data, list) else data.get("data", [])
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["cbsa_code"] = cbsa_code
    df["year"] = year
    df["quarter"] = quarter
    time.sleep(delay)
    return df


def run(config: dict = None) -> dict:
    """Main fetch entry point."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "hud_vacancy")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    api_key = config["fetch"].get("hud_api_key", "")
    delay = config["fetch"]["request_delay_seconds"]
    end_year = config["fetch"]["end_year"]

    if not api_key:
        logger.warning("HUD_API_KEY not set — skipping vacancy fetch")
        return {
            "source": "hud_vacancy",
            "status": "PARTIAL",
            "files_written": 0,
            "rows_fetched": 0,
        }

    cbsa_codes = get_cbsa_codes()
    files_written = 0
    total_rows = 0

    for year in range(2015, end_year + 1):
        for quarter in range(1, 5):
            out_path = os.path.join(raw_dir, f"hud_vacancy_{year}Q{quarter}.csv")
            if os.path.exists(out_path):
                logger.info(f"Skipping {year}Q{quarter} vacancy, file exists")
                files_written += 1
                continue

            all_dfs = []
            for cbsa in cbsa_codes:
                df = fetch_vacancy_by_cbsa(cbsa, year, quarter, api_key, delay)
                if not df.empty:
                    all_dfs.append(df)

            if all_dfs:
                combined = pd.concat(all_dfs, ignore_index=True)
                combined.to_csv(out_path, index=False)
                files_written += 1
                total_rows += len(combined)
                logger.info(f"Wrote {len(combined)} rows for {year}Q{quarter}")
            else:
                logger.warning(f"No vacancy data for {year}Q{quarter}")

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
