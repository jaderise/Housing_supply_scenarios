"""
Downloads HUD Fair Market Rent data.

Primary: direct Excel download (no auth required)
  URL: huduser.gov/portal/datasets/fmr/fmr{YYYY}/FY{YY}_FMRs.xlsx
Fallback: HUD User API (requires HUD_API_KEY Bearer token)
  API docs: https://www.huduser.gov/portal/dataset/fmr-api.html
"""

import io
import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

from pipeline.utils.cbsa_utils import load_pipeline_config, get_cbsa_codes

logger = logging.getLogger("pipeline.fetch.hud_fmr")

HUD_FMR_BASE = "https://www.huduser.gov/portal/datasets/fmr"
HUD_API_BASE = "https://www.huduser.gov/hudapi/public"


def fetch_fmr_excel(fiscal_year: int) -> pd.DataFrame:
    """Download FMR data from the public Excel file."""
    short_yr = str(fiscal_year)[-2:]
    urls = [
        f"{HUD_FMR_BASE}/fmr{fiscal_year}/FY{short_yr}_FMRs_revised.xlsx",
        f"{HUD_FMR_BASE}/fmr{fiscal_year}/FY{short_yr}_FMRs.xlsx",
        f"{HUD_FMR_BASE}/fmr{fiscal_year}/FY{short_yr}_FMRs_rev.xlsx",
    ]

    for url in urls:
        try:
            logger.info(f"Trying FMR Excel: {url}")
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200 and len(resp.content) > 1000:
                df = pd.read_excel(io.BytesIO(resp.content), dtype=str)
                df.columns = df.columns.str.lower().str.replace(" ", "_")
                df["fiscal_year"] = fiscal_year
                logger.info(f"FMR FY{fiscal_year}: {len(df)} rows from Excel")
                return df
        except Exception as e:
            logger.debug(f"FMR Excel failed: {url} - {e}")
            continue

    return pd.DataFrame()


def _hud_api_get(endpoint: str, api_key: str, timeout: int = 60) -> dict | None:
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


def fetch_fmr_api(cbsa_codes: list, fiscal_year: int,
                  api_key: str, delay: float = 0.5) -> pd.DataFrame:
    """Fetch FMR data for all CBSAs via the HUD API."""
    all_dfs = []
    for cbsa in cbsa_codes:
        entity_id = f"METRO{cbsa}M{cbsa}"
        data = _hud_api_get(f"fmr/data/{entity_id}?year={fiscal_year}", api_key)

        if not data:
            data = _hud_api_get(f"fmr/data/{cbsa}99999?year={fiscal_year}", api_key)

        if not data:
            continue

        basic = data.get("data", {}).get("basicdata", data.get("data", {}))
        if isinstance(basic, list):
            df = pd.DataFrame(basic)
        elif isinstance(basic, dict):
            df = pd.DataFrame([basic])
        else:
            continue

        df["cbsa_code"] = cbsa
        df["fiscal_year"] = fiscal_year
        all_dfs.append(df)
        time.sleep(delay)

    if not all_dfs:
        return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)


def run(config: dict = None) -> dict:
    """Main fetch entry point."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "hud_fmr")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    api_key = config["fetch"].get("hud_api_key", "")
    delay = config["fetch"]["request_delay_seconds"]
    end_year = config["fetch"]["end_year"]
    cbsa_codes = get_cbsa_codes()

    files_written = 0
    total_rows = 0

    for fy in range(2015, end_year + 2):
        out_path = os.path.join(raw_dir, f"hud_fmr_fy{fy}.csv")
        if os.path.exists(out_path):
            logger.info(f"Skipping FMR FY{fy}, file exists")
            files_written += 1
            continue

        # Try direct Excel download first (no auth required)
        df = fetch_fmr_excel(fy)

        # Fall back to API if Excel failed and key is available
        if df.empty and api_key:
            df = fetch_fmr_api(cbsa_codes, fy, api_key, delay)

        if not df.empty:
            fmr2_cols = [c for c in df.columns if "fmr" in c.lower() and "2" in c]
            if fmr2_cols:
                fmr_vals = pd.to_numeric(df[fmr2_cols[0]], errors="coerce")
                bad = fmr_vals[(fmr_vals < 200) | (fmr_vals > 10000)].dropna()
                if not bad.empty:
                    logger.warning(f"FMR FY{fy}: {len(bad)} values outside $200-$10000")

            df.to_csv(out_path, index=False)
            files_written += 1
            total_rows += len(df)
            logger.info(f"Wrote FMR data for FY{fy}: {len(df)} rows")
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
