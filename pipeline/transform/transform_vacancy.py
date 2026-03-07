"""
Aggregates zip-level HUD vacancy to CBSA level using weighted crosswalk.
"""

import logging
import os
from pathlib import Path

import pandas as pd

from pipeline.utils.cbsa_utils import load_pipeline_config, get_cbsa_codes, filter_to_top50

logger = logging.getLogger("pipeline.transform.vacancy")


def aggregate_quarter(vacancy_path: str, crosswalk_path: str, year: int, quarter: int) -> pd.DataFrame:
    """Aggregate zip-level vacancy to CBSA for one quarter."""
    try:
        vac = pd.read_csv(vacancy_path, dtype={"zip": str})
        xw = pd.read_csv(crosswalk_path, dtype=str)
    except Exception as e:
        logger.error(f"Failed to read files for {year}Q{quarter}: {e}")
        return pd.DataFrame()

    # Normalize column names
    vac.columns = vac.columns.str.lower().str.replace(" ", "_")
    xw.columns = xw.columns.str.lower().str.replace(" ", "_")

    # Standardize zip column
    for col in ["zip", "zip_code", "zipcode"]:
        if col in vac.columns and col != "zip":
            vac = vac.rename(columns={col: "zip"})
        if col in xw.columns and col != "zip":
            xw = xw.rename(columns={col: "zip"})

    if "zip" not in vac.columns or "zip" not in xw.columns:
        logger.error(f"No zip column found for {year}Q{quarter}")
        return pd.DataFrame()

    vac["zip"] = vac["zip"].astype(str).str.zfill(5)
    xw["zip"] = xw["zip"].astype(str).str.zfill(5)

    # Identify vacancy columns
    res_addr_col = None
    res_vac_col = None
    nostat_col = None

    for col in vac.columns:
        if "res" in col and "vadr" in col:
            res_addr_col = col
        elif "res" in col and "vacant" in col:
            res_vac_col = col
        elif "no_stat" in col or "nostat" in col:
            nostat_col = col

    if not res_addr_col or not res_vac_col:
        logger.warning(f"Cannot identify vacancy columns for {year}Q{quarter}: {list(vac.columns)}")
        return pd.DataFrame()

    vac[res_addr_col] = pd.to_numeric(vac[res_addr_col], errors="coerce").fillna(0)
    vac[res_vac_col] = pd.to_numeric(vac[res_vac_col], errors="coerce").fillna(0)
    if nostat_col:
        vac[nostat_col] = pd.to_numeric(vac[nostat_col], errors="coerce").fillna(0)

    # Identify CBSA and ratio columns in crosswalk
    cbsa_col = None
    ratio_col = None
    for col in xw.columns:
        if "cbsa" in col and "code" not in col:
            continue
        if "cbsa" in col:
            cbsa_col = col
        if "res_ratio" in col or "ratio" in col:
            ratio_col = col

    if not cbsa_col:
        cbsa_col = [c for c in xw.columns if "cbsa" in c]
        cbsa_col = cbsa_col[0] if cbsa_col else None

    if not cbsa_col:
        logger.error(f"No CBSA column in crosswalk for {year}Q{quarter}")
        return pd.DataFrame()

    if ratio_col:
        xw[ratio_col] = pd.to_numeric(xw[ratio_col], errors="coerce").fillna(0)
    else:
        xw["res_ratio"] = 1.0
        ratio_col = "res_ratio"

    # Join vacancy to crosswalk
    merged = vac.merge(xw[["zip", cbsa_col, ratio_col]], on="zip", how="inner")

    # Weight by res_ratio
    merged["weighted_addresses"] = merged[res_addr_col] * merged[ratio_col]
    merged["weighted_vacant"] = merged[res_vac_col] * merged[ratio_col]
    if nostat_col:
        merged["weighted_nostat"] = merged[nostat_col] * merged[ratio_col]

    # Aggregate to CBSA
    agg = merged.groupby(cbsa_col).agg(
        total_residential_addresses=("weighted_addresses", "sum"),
        vacant_addresses=("weighted_vacant", "sum"),
        zip_count=("zip", "nunique"),
    ).reset_index()

    if nostat_col:
        nostat_agg = merged.groupby(cbsa_col)["weighted_nostat"].sum().reset_index()
        nostat_agg.columns = [cbsa_col, "no_stat_addresses"]
        agg = agg.merge(nostat_agg, on=cbsa_col, how="left")
    else:
        agg["no_stat_addresses"] = 0

    agg = agg.rename(columns={cbsa_col: "cbsa_code"})
    agg["cbsa_code"] = agg["cbsa_code"].astype(str).str.zfill(5)
    agg["year"] = year
    agg["quarter"] = quarter

    # Calculate vacancy rate
    agg["vacancy_rate"] = agg["vacant_addresses"] / agg["total_residential_addresses"].replace(0, float("nan"))
    agg["vacancy_rate_incl_nostat"] = (
        (agg["vacant_addresses"] + agg["no_stat_addresses"])
        / agg["total_residential_addresses"].replace(0, float("nan"))
    )

    return agg


def run(config: dict = None) -> dict:
    """Transform raw HUD vacancy data into processed CBSA-level quarterly file."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "hud_vacancy")
    processed_dir = config["data_paths"]["processed"]
    Path(processed_dir).mkdir(parents=True, exist_ok=True)

    all_frames = []
    rows_flagged = 0

    end_year = config["fetch"]["end_year"]

    for year in range(2015, end_year + 1):
        for quarter in range(1, 5):
            vac_path = os.path.join(raw_dir, f"hud_vacancy_zip_{year}Q{quarter}.csv")
            xw_path = os.path.join(raw_dir, f"hud_cbsa_crosswalk_{year}Q{quarter}.csv")

            if not os.path.exists(vac_path) or not os.path.exists(xw_path):
                continue

            df = aggregate_quarter(vac_path, xw_path, year, quarter)
            if not df.empty:
                all_frames.append(df)

    if not all_frames:
        logger.warning("No vacancy data aggregated")
        return {"source": "vacancy", "status": "PARTIAL", "rows_out": 0}

    combined = pd.concat(all_frames, ignore_index=True)
    rows_in = len(combined)

    # Filter to top 50
    combined = filter_to_top50(combined)

    # DQ: vacancy rate bounds
    combined["dq_flag"] = None
    bad_rate = combined[
        (combined["vacancy_rate"] < 0.02) | (combined["vacancy_rate"] > 0.20)
    ].index
    combined.loc[bad_rate, "dq_flag"] = "OUT_OF_RANGE"
    rows_flagged += len(bad_rate)

    # Add CBSA name placeholder
    if "cbsa_name" not in combined.columns:
        combined["cbsa_name"] = ""

    out_cols = [
        "cbsa_code", "cbsa_name", "year", "quarter",
        "total_residential_addresses", "vacant_addresses", "no_stat_addresses",
        "vacancy_rate", "vacancy_rate_incl_nostat", "zip_count", "dq_flag",
    ]
    for col in out_cols:
        if col not in combined.columns:
            combined[col] = None

    combined = combined[out_cols].sort_values(["cbsa_code", "year", "quarter"])

    out_path = os.path.join(processed_dir, "vacancy_metro_quarterly.csv")
    combined.to_csv(out_path, index=False)

    logger.info(f"Vacancy: wrote {len(combined)} rows to {out_path}")
    return {
        "source": "vacancy",
        "status": "SUCCESS",
        "rows_in": rows_in,
        "rows_out": len(combined),
        "rows_flagged": rows_flagged,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
