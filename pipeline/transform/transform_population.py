"""
Selects preferred PEP vintage for each year, normalizes, handles 2020 discontinuity.
"""

import logging
import os
from pathlib import Path

import pandas as pd

from pipeline.utils.cbsa_utils import load_pipeline_config, get_cbsa_codes, filter_to_top50

logger = logging.getLogger("pipeline.transform.population")


def run(config: dict = None) -> dict:
    """Transform raw population CSVs into a single processed file."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "census_population")
    processed_dir = config["data_paths"]["processed"]
    Path(processed_dir).mkdir(parents=True, exist_ok=True)

    cbsa_codes = get_cbsa_codes()
    all_frames = []
    rows_flagged = 0

    raw_files = sorted(Path(raw_dir).glob("census_population_metro_*.csv"))
    if not raw_files:
        logger.error("No raw population files found")
        return {"source": "population", "status": "FAILED", "rows_out": 0}

    for fpath in raw_files:
        try:
            df = pd.read_csv(fpath, dtype={"cbsa_code": str})
            df["cbsa_code"] = df["cbsa_code"].astype(str).str.zfill(5)
            all_frames.append(df)
        except Exception as e:
            logger.error(f"Failed to read {fpath}: {e}")

    if not all_frames:
        return {"source": "population", "status": "FAILED", "rows_out": 0}

    combined = pd.concat(all_frames, ignore_index=True)
    rows_in = len(combined)

    # Convert numeric
    for col in ["population", "domestic_migration_net", "international_migration_net", "natural_increase"]:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce")

    # Filter to top 50
    combined = filter_to_top50(combined)

    # Select preferred vintage: use most recent vintage for each calendar year
    if "pep_vintage" in combined.columns and "year" in combined.columns:
        combined = combined.sort_values("pep_vintage", ascending=False)
        combined = combined.drop_duplicates(subset=["cbsa_code", "year"], keep="first")

    # Initialize DQ flag
    combined["dq_flag"] = None

    # DQ: population must be > 0
    invalid = combined[combined["population"] <= 0]
    if not invalid.empty:
        combined.loc[invalid.index, "dq_flag"] = "OUT_OF_RANGE"
        rows_flagged += len(invalid)

    # DQ: flag 2020 Census rebase discontinuity
    combined.loc[combined["year"] == 2020, "dq_flag"] = combined.loc[
        combined["year"] == 2020, "dq_flag"
    ].fillna("VINTAGE_MISMATCH")

    # DQ: plausible YoY change
    combined = combined.sort_values(["cbsa_code", "year"])
    combined["_pop_prev"] = combined.groupby("cbsa_code")["population"].shift(1)
    combined["_pop_pct_change"] = (
        (combined["population"] - combined["_pop_prev"]) / combined["_pop_prev"]
    )
    implausible = combined[
        (combined["_pop_pct_change"] > 0.10) | (combined["_pop_pct_change"] < -0.05)
    ]
    if not implausible.empty:
        combined.loc[implausible.index, "dq_flag"] = combined.loc[
            implausible.index, "dq_flag"
        ].fillna("IMPLAUSIBLE_CHANGE")
        rows_flagged += len(implausible)
        logger.warning(f"Population: {len(implausible)} rows with implausible change")

    combined = combined.drop(columns=["_pop_prev", "_pop_pct_change"], errors="ignore")

    # Ensure required columns
    out_cols = [
        "cbsa_code", "cbsa_name", "year", "population",
        "domestic_migration_net", "international_migration_net",
        "natural_increase", "pep_vintage", "dq_flag",
    ]
    for col in out_cols:
        if col not in combined.columns:
            combined[col] = None

    combined = combined[out_cols].sort_values(["cbsa_code", "year"])

    out_path = os.path.join(processed_dir, "population_metro_annual.csv")
    combined.to_csv(out_path, index=False)

    logger.info(f"Population: wrote {len(combined)} rows to {out_path}")
    return {
        "source": "population",
        "status": "SUCCESS",
        "rows_in": rows_in,
        "rows_out": len(combined),
        "rows_flagged": rows_flagged,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
