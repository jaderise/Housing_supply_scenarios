"""
Downloads HUD Fair Market Rent data via the HUD User API.
Requires HUD_API_KEY (Bearer token from huduser.gov).

API docs: https://www.huduser.gov/portal/dataset/fmr-api.html
"""

import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

from pipeline.utils.cbsa_utils import load_pipeline_config, get_cbsa_codes

logger = logging.getLogger("pipeline.fetch.hud_fmr")

HUD_API_BASE = "https://www.huduser.gov/hudapi/public"


def _hud_get(endpoint: str, api_key: str, timeout: int = 60) -> dict | None:
    """Make an authenticated GET request to the HUD API."""
    url = f"{HUD_API_BASE}/{endpoint}"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        logger.warning(f"HUD FMR API {endpoint}: HTTP {resp.status_code}")
        return None
    except Exception as e:
        logger.warning(f"HUD FMR API {endpoint} failed: {e}")
        return None


def fetch_fmr_for_cbsa(cbsa_code: str, fiscal_year: int,
                       api_key: str, delay: float = 0.5) -> pd.DataFrame:
    """Fetch FMR data for a single CBSA via the HUD API."""
    entity_id = f"METRO{cbsa_code}M{cbsa_code}"
    data = _hud_get(f"fmr/data/{entity_id}?year={fiscal_year}", api_key)

    if not data:
        data = _hud_get(f"fmr/data/{cbsa_code}99999?year={fiscal_year}", api_key)

    if not data:
        return pd.DataFrame()

    basic = data.get("data", {}).get("basicdata", data.get("data", {}))
    if isinstance(basic, list):
        df = pd.DataFrame(basic)
    elif isinstance(basic, dict):
        df = pd.DataFrame([basic])
    else:
        return pd.DataFrame()

    df["cbsa_code"] = cbsa_code
    df["fiscal_year"] = fiscal_year
    time.sleep(delay)
    return df


def run(config: dict = None) -> dict:
    """Main fetch entry point."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "hud_fmr")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    api_key = config["fetch"].get("hud_api_key", "")
    delay = config["fetch"]["request_delay_seconds"]
    end_year = config["fetch"]["end_year"]

    if not api_key:
        logger.warning("HUD_API_KEY not set — skipping FMR fetch")
        return {
            "source": "hud_fmr",
            "status": "PARTIAL",
            "files_written": 0,
            "rows_fetched": 0,
        }

    cbsa_codes = get_cbsa_codes()
    files_written = 0
    total_rows = 0

    for fy in range(2015, end_year + 2):
        out_path = os.path.join(raw_dir, f"hud_fmr_fy{fy}.csv")
        if os.path.exists(out_path):
            logger.info(f"Skipping FMR FY{fy}, file exists")
            files_written += 1
            continue

        all_dfs = []
        for cbsa in cbsa_codes:
            df = fetch_fmr_for_cbsa(cbsa, fy, api_key, delay)
            if not df.empty:
                all_dfs.append(df)

        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)

            fmr2_cols = [c for c in combined.columns if "fmr" in c.lower() and "2" in c]
            if fmr2_cols:
                fmr_vals = pd.to_numeric(combined[fmr2_cols[0]], errors="coerce")
                bad = fmr_vals[(fmr_vals < 200) | (fmr_vals > 10000)].dropna()
                if not bad.empty:
                    logger.warning(f"FMR FY{fy}: {len(bad)} values outside $200-$10000")

            combined.to_csv(out_path, index=False)
            files_written += 1
            total_rows += len(combined)
            logger.info(f"Wrote FMR data for FY{fy}: {len(combined)} rows")
        else:
            logger.warning(f"No FMR data for FY{fy}")

    return {
        "source": "hud_fmr",
        "status": "SUCCESS" if files_written > 0 else "PARTIAL",
        "files_written": files_written,
        "rows_fetched": total_rows,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
