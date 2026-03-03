"""
Maps HUD FMR data to CBSA codes via HUD-to-CBSA crosswalk.
"""

import logging
import os
from pathlib import Path

import pandas as pd

from pipeline.utils.cbsa_utils import load_pipeline_config, get_cbsa_codes, filter_to_top50

logger = logging.getLogger("pipeline.transform.fmr")


def run(config: dict = None) -> dict:
    """Transform raw FMR data into a processed CBSA-level annual file."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "hud_fmr")
    processed_dir = config["data_paths"]["processed"]
    Path(processed_dir).mkdir(parents=True, exist_ok=True)

    all_frames = []
    rows_flagged = 0

    raw_files = sorted(Path(raw_dir).glob("hud_fmr_fy*.csv"))
    if not raw_files:
        logger.warning("No raw FMR files found")
        return {"source": "fmr", "status": "PARTIAL", "rows_out": 0}

    for fpath in raw_files:
        try:
            df = pd.read_csv(fpath, dtype=str)
            df.columns = df.columns.str.lower().str.replace(" ", "_")

            # Look for CBSA/metro code column
            cbsa_col = None
            for col in df.columns:
                if "cbsa" in col or "fmrdd" in col or "metro" in col:
                    cbsa_col = col
                    break

            # Look for FMR 2BR column
            fmr2_col = None
            for col in df.columns:
                if "fmr" in col and "2" in col:
                    fmr2_col = col
                    break
                if col == "fmr2":
                    fmr2_col = col
                    break

            if not cbsa_col or not fmr2_col:
                logger.warning(f"Cannot identify columns in {fpath.name}: {list(df.columns)}")
                continue

            # Extract fiscal year from filename
            fy = int(fpath.stem.replace("hud_fmr_fy", ""))

            result = pd.DataFrame({
                "cbsa_code": df[cbsa_col].astype(str).str.zfill(5),
                "cbsa_name": df.get("areaname", df.get("area_name", "")),
                "fiscal_year": fy,
                "fmr_2br": pd.to_numeric(df[fmr2_col], errors="coerce"),
            })

            all_frames.append(result)
        except Exception as e:
            logger.error(f"Failed to process {fpath}: {e}")

    if not all_frames:
        return {"source": "fmr", "status": "PARTIAL", "rows_out": 0}

    combined = pd.concat(all_frames, ignore_index=True)
    rows_in = len(combined)

    # Filter to top 50
    combined = filter_to_top50(combined)

    # DQ: FMR range check
    combined["dq_flag"] = None
    bad_fmr = combined[
        (combined["fmr_2br"] < 200) | (combined["fmr_2br"] > 10000)
    ].dropna(subset=["fmr_2br"]).index
    combined.loc[bad_fmr, "dq_flag"] = "OUT_OF_RANGE"
    rows_flagged += len(bad_fmr)

    out_cols = ["cbsa_code", "cbsa_name", "fiscal_year", "fmr_2br", "dq_flag"]
    combined = combined[out_cols].sort_values(["cbsa_code", "fiscal_year"])

    out_path = os.path.join(processed_dir, "fmr_metro_annual.csv")
    combined.to_csv(out_path, index=False)

    logger.info(f"FMR: wrote {len(combined)} rows to {out_path}")
    return {
        "source": "fmr",
        "status": "SUCCESS",
        "rows_in": rows_in,
        "rows_out": len(combined),
        "rows_flagged": rows_flagged,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
