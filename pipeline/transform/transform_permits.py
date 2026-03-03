"""
Reads all annual permit CSVs, normalizes, filters to top 50 CBSAs.
"""

import logging
import os
from pathlib import Path

import pandas as pd

from pipeline.utils.cbsa_utils import load_pipeline_config, get_cbsa_codes, filter_to_top50
from pipeline.utils.dq_checks import check_completeness, check_non_negative, check_yoy_change

logger = logging.getLogger("pipeline.transform.permits")


def run(config: dict = None) -> dict:
    """Transform raw permit CSVs into a single processed file."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "census_permits")
    processed_dir = config["data_paths"]["processed"]
    Path(processed_dir).mkdir(parents=True, exist_ok=True)

    cbsa_codes = get_cbsa_codes()
    all_frames = []
    rows_flagged = 0

    # Read all annual permit files
    raw_files = sorted(Path(raw_dir).glob("census_permits_metro_annual_*.csv"))
    if not raw_files:
        logger.error("No raw permit files found")
        return {"source": "permits", "status": "FAILED", "rows_out": 0}

    for fpath in raw_files:
        try:
            df = pd.read_csv(fpath, dtype={"cbsa_code": str})
            df["cbsa_code"] = df["cbsa_code"].astype(str).str.zfill(5)
            all_frames.append(df)
        except Exception as e:
            logger.error(f"Failed to read {fpath}: {e}")

    if not all_frames:
        return {"source": "permits", "status": "FAILED", "rows_out": 0}

    combined = pd.concat(all_frames, ignore_index=True)
    rows_in = len(combined)

    # Filter to top 50 CBSAs
    combined = filter_to_top50(combined)

    # Ensure required columns exist
    for col in ["permits_total", "permits_sf", "permits_mf_small", "permits_mf_large"]:
        if col not in combined.columns:
            combined[col] = 0

    # Convert numeric
    for col in ["permits_total", "permits_sf", "permits_mf_small", "permits_mf_large"]:
        combined[col] = pd.to_numeric(combined[col], errors="coerce")

    # Initialize DQ flag
    combined["dq_flag"] = None

    # DQ: check non-negative
    neg_rows = check_non_negative(combined, ["permits_total", "permits_sf"])
    if not neg_rows.empty:
        combined.loc[neg_rows.index, "dq_flag"] = "OUT_OF_RANGE"
        rows_flagged += len(neg_rows)
        logger.warning(f"Permits: {len(neg_rows)} rows with negative values")

    # DQ: check YoY change > 200%
    implausible = check_yoy_change(combined, "permits_total", max_increase_pct=2.0, max_decrease_pct=0.75)
    if not implausible.empty:
        combined.loc[combined.index.isin(implausible.index), "dq_flag"] = "IMPLAUSIBLE_CHANGE"
        rows_flagged += len(implausible)
        logger.warning(f"Permits: {len(implausible)} rows with implausible YoY changes")

    # DQ: check completeness per year
    for year in combined["year"].unique():
        year_df = combined[combined["year"] == year]
        missing = check_completeness(year_df, cbsa_codes, year, "permits")
        if missing:
            logger.warning(f"Permits {year}: missing CBSAs: {missing}")
            rows_flagged += len(missing)

    # Fill single-year gaps with interpolation
    combined = combined.sort_values(["cbsa_code", "year"])
    combined["permits_total"] = combined.groupby("cbsa_code")["permits_total"].transform(
        lambda s: s.interpolate(method="linear", limit=1)
    )

    # Add data_source column
    combined["data_source"] = "census_bps"

    # Select final columns
    out_cols = [
        "cbsa_code", "cbsa_name", "year", "permits_total", "permits_sf",
        "permits_mf_small", "permits_mf_large", "data_source", "dq_flag",
    ]
    for col in out_cols:
        if col not in combined.columns:
            combined[col] = None

    combined = combined[out_cols]

    out_path = os.path.join(processed_dir, "permits_metro_annual.csv")
    combined.to_csv(out_path, index=False)

    logger.info(f"Permits: wrote {len(combined)} rows to {out_path}")
    return {
        "source": "permits",
        "status": "SUCCESS",
        "rows_in": rows_in,
        "rows_out": len(combined),
        "rows_flagged": rows_flagged,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
