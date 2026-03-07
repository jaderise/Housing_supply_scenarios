"""
Cleans ACS data, validates internal consistency, handles 2020 gap.
"""

import logging
import os
from pathlib import Path

import pandas as pd

from pipeline.utils.cbsa_utils import load_pipeline_config, get_cbsa_codes, filter_to_top50

logger = logging.getLogger("pipeline.transform.acs")


def run(config: dict = None) -> dict:
    """Transform raw ACS CSVs into a single processed file."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "census_acs")
    processed_dir = config["data_paths"]["processed"]
    Path(processed_dir).mkdir(parents=True, exist_ok=True)

    all_frames = []
    rows_flagged = 0

    raw_files = sorted(Path(raw_dir).glob("census_acs_metro_*.csv"))
    if not raw_files:
        logger.error("No raw ACS files found")
        return {"source": "acs", "status": "FAILED", "rows_out": 0}

    for fpath in raw_files:
        try:
            df = pd.read_csv(fpath, dtype={"cbsa_code": str})
            df["cbsa_code"] = df["cbsa_code"].astype(str).str.zfill(5)
            all_frames.append(df)
        except Exception as e:
            logger.error(f"Failed to read {fpath}: {e}")

    if not all_frames:
        return {"source": "acs", "status": "FAILED", "rows_out": 0}

    combined = pd.concat(all_frames, ignore_index=True)
    rows_in = len(combined)

    # Filter to top 50
    combined = filter_to_top50(combined)

    # Convert numeric columns
    numeric_cols = [
        "total_housing_units", "occupied_units", "vacant_units",
        "owner_occupied", "renter_occupied", "avg_household_size",
        "median_hh_income", "median_home_value", "median_gross_rent",
    ]
    for col in numeric_cols:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce")

    # Initialize DQ flag
    combined["dq_flag"] = None

    # DQ: occupied + vacant ≈ total (within 2%)
    if all(c in combined.columns for c in ["total_housing_units", "occupied_units", "vacant_units"]):
        check = combined.dropna(subset=["total_housing_units", "occupied_units", "vacant_units"])
        if not check.empty:
            diff = abs(check["occupied_units"] + check["vacant_units"] - check["total_housing_units"])
            pct = diff / check["total_housing_units"]
            bad_idx = check[pct > 0.02].index
            combined.loc[bad_idx, "dq_flag"] = "OUT_OF_RANGE"
            rows_flagged += len(bad_idx)

    # DQ: income plausibility
    if "median_hh_income" in combined.columns:
        bad_income = combined[combined["median_hh_income"] <= 0].index
        combined.loc[bad_income, "dq_flag"] = "OUT_OF_RANGE"
        rows_flagged += len(bad_income)

    # DQ: household size plausibility
    if "avg_household_size" in combined.columns:
        bad_hh = combined[
            (combined["avg_household_size"] < 1.5) | (combined["avg_household_size"] > 5.0)
        ].dropna(subset=["avg_household_size"]).index
        combined.loc[bad_hh, "dq_flag"] = combined.loc[bad_hh, "dq_flag"].fillna("OUT_OF_RANGE")
        rows_flagged += len(bad_hh)

    # Handle 2020 gap: interpolate from 2019 and 2021
    combined = combined.sort_values(["cbsa_code", "year"])
    cbsa_codes = combined["cbsa_code"].unique()

    rows_2020 = []
    for cbsa in cbsa_codes:
        cbsa_df = combined[combined["cbsa_code"] == cbsa]
        row_2019 = cbsa_df[cbsa_df["year"] == 2019]
        row_2021 = cbsa_df[cbsa_df["year"] == 2021]

        if not row_2019.empty and not row_2021.empty:
            row = row_2019.iloc[0].copy()
            row["year"] = 2020
            row["dq_flag"] = "IMPUTED_VALUE"
            row["acs_type"] = "interpolated"

            for col in numeric_cols:
                if col in row.index:
                    v2019 = row_2019.iloc[0].get(col)
                    v2021 = row_2021.iloc[0].get(col)
                    if pd.notna(v2019) and pd.notna(v2021):
                        row[col] = (v2019 + v2021) / 2

            rows_2020.append(row)

    if rows_2020:
        df_2020 = pd.DataFrame(rows_2020)
        # Only add if 2020 doesn't already exist
        existing_2020 = combined[combined["year"] == 2020]
        if existing_2020.empty:
            combined = pd.concat([combined, df_2020], ignore_index=True)
            logger.info(f"ACS: interpolated {len(rows_2020)} rows for 2020")

    combined = combined.sort_values(["cbsa_code", "year"])

    # Select output columns
    out_cols = [
        "cbsa_code", "cbsa_name", "year", "total_housing_units", "occupied_units",
        "vacant_units", "owner_occupied", "renter_occupied", "avg_household_size",
        "median_hh_income", "median_home_value", "median_gross_rent", "acs_type", "dq_flag",
    ]
    for col in out_cols:
        if col not in combined.columns:
            combined[col] = None

    combined = combined[out_cols]

    out_path = os.path.join(processed_dir, "acs_metro_annual.csv")
    combined.to_csv(out_path, index=False)

    logger.info(f"ACS: wrote {len(combined)} rows to {out_path}")
    return {
        "source": "acs",
        "status": "SUCCESS",
        "rows_in": rows_in,
        "rows_out": len(combined),
        "rows_flagged": rows_flagged,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
