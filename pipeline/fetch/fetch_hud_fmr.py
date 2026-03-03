"""
Downloads HUD Fair Market Rent files.
"""

import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

from pipeline.utils.cbsa_utils import load_pipeline_config

logger = logging.getLogger("pipeline.fetch.hud_fmr")

HUD_FMR_BASE = "https://www.huduser.gov/portal/datasets/fmr"


def fetch_fmr_year(fiscal_year: int, raw_dir: str, delay: float = 0.5) -> bool:
    """Download FMR data for a single fiscal year."""
    out_path = os.path.join(raw_dir, f"hud_fmr_fy{fiscal_year}.csv")
    if os.path.exists(out_path):
        logger.info(f"Skipping FMR FY{fiscal_year}, file exists")
        return True

    # Try multiple URL patterns
    urls = [
        f"{HUD_FMR_BASE}/FY{fiscal_year}_FMRs.xlsx",
        f"{HUD_FMR_BASE}/fy{fiscal_year}_safmrs.xlsx",
        f"{HUD_FMR_BASE}/FY{fiscal_year}_4050_RevFinal.xlsx",
        f"{HUD_FMR_BASE}/FY{fiscal_year}_FMR_4050_Rev.xlsx",
    ]

    for url in urls:
        try:
            logger.info(f"Trying FMR URL: {url}")
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200:
                df = pd.read_excel(pd.io.common.BytesIO(resp.content), dtype=str)
                df.columns = df.columns.str.lower().str.replace(" ", "_")

                # Look for FMR 2-bedroom column
                fmr2_cols = [c for c in df.columns if "fmr" in c and "2" in c]
                if not fmr2_cols:
                    fmr2_cols = [c for c in df.columns if "fmr2" in c]

                df["fiscal_year"] = fiscal_year
                df.to_csv(out_path, index=False)
                logger.info(f"Wrote FMR data for FY{fiscal_year}: {len(df)} rows")

                # DQ: FMR sanity check
                if fmr2_cols:
                    fmr_vals = pd.to_numeric(df[fmr2_cols[0]], errors="coerce")
                    bad = fmr_vals[(fmr_vals < 200) | (fmr_vals > 10000)].dropna()
                    if not bad.empty:
                        logger.warning(f"FMR FY{fiscal_year}: {len(bad)} values outside $200-$10000")

                time.sleep(delay)
                return True
        except Exception as e:
            logger.debug(f"FMR URL failed: {url} - {e}")
            continue

    logger.warning(f"Could not fetch FMR for FY{fiscal_year}")
    return False


def run(config: dict = None) -> dict:
    """Main fetch entry point."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "hud_fmr")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    delay = config["fetch"]["request_delay_seconds"]
    end_year = config["fetch"]["end_year"]

    files_written = 0

    for fy in range(2000, end_year + 2):  # FY can be one year ahead
        if fetch_fmr_year(fy, raw_dir, delay):
            files_written += 1

    return {
        "source": "hud_fmr",
        "status": "SUCCESS" if files_written > 0 else "PARTIAL",
        "files_written": files_written,
        "rows_fetched": 0,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
