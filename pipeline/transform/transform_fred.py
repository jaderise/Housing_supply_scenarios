"""
Converts FRED series to analytical format, handles frequency normalization.
"""

import logging
import os
from pathlib import Path

import pandas as pd

from pipeline.utils.cbsa_utils import load_pipeline_config

logger = logging.getLogger("pipeline.transform.fred")

# Series frequency mapping
SERIES_FREQ = {
    "HOUST": "monthly",
    "HOUSTNE": "monthly",
    "HOUSTMW": "monthly",
    "HOUSTS": "monthly",
    "HOUSTW": "monthly",
    "COMPUTSA": "monthly",
    "RVACRATE": "quarterly",
    "HCOVACRATE": "quarterly",
    "TTLHHLD": "annual",
    "MORTGAGE30US": "weekly",
    "MSPUS": "quarterly",
    "CPIAUCSL": "monthly",
}


def run(config: dict = None) -> dict:
    """Transform raw FRED CSVs into a single combined file."""
    if config is None:
        config = load_pipeline_config()

    raw_dir = os.path.join(config["data_paths"]["raw"], "fred")
    processed_dir = config["data_paths"]["processed"]
    Path(processed_dir).mkdir(parents=True, exist_ok=True)

    all_frames = []
    rows_flagged = 0

    for series_id, freq in SERIES_FREQ.items():
        fpath = os.path.join(raw_dir, f"fred_{series_id}.csv")
        if not os.path.exists(fpath):
            logger.warning(f"Missing FRED file: {fpath}")
            continue

        try:
            df = pd.read_csv(fpath)
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df["series_id"] = series_id

            # Frequency normalization
            if freq == "weekly":
                # Aggregate weekly to monthly average
                df["month_date"] = df["date"].dt.to_period("M").dt.to_timestamp()
                monthly = df.groupby(["series_id", "month_date"])["value"].mean().reset_index()
                monthly.columns = ["series_id", "date", "value"]
                monthly["frequency"] = "monthly"
                df = monthly
            else:
                df["frequency"] = freq

            # DQ: check for sudden jumps > 50% MoM
            if freq in ("monthly", "weekly"):
                df_sorted = df.sort_values("date")
                df_sorted["_prev"] = df_sorted["value"].shift(1)
                df_sorted["_pct"] = abs(
                    (df_sorted["value"] - df_sorted["_prev"]) / df_sorted["_prev"].replace(0, float("nan"))
                )
                bad = df_sorted[df_sorted["_pct"] > 0.50]
                if not bad.empty and series_id not in ("MORTGAGE30US",):
                    logger.warning(f"FRED {series_id}: {len(bad)} observations with >50% change")
                    rows_flagged += len(bad)
                df = df_sorted.drop(columns=["_prev", "_pct"], errors="ignore")

            if "pull_date" not in df.columns:
                df["pull_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

            # Keep only needed columns
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")
            df = df[["series_id", "date", "value", "frequency", "pull_date"]]

            all_frames.append(df)
        except Exception as e:
            logger.error(f"Failed to process FRED {series_id}: {e}")

    if not all_frames:
        return {"source": "fred", "status": "FAILED", "rows_out": 0}

    combined = pd.concat(all_frames, ignore_index=True)
    rows_in = len(combined)

    out_path = os.path.join(processed_dir, "fred_national_series.csv")
    combined.to_csv(out_path, index=False)

    logger.info(f"FRED: wrote {len(combined)} rows to {out_path}")
    return {
        "source": "fred",
        "status": "SUCCESS",
        "rows_in": rows_in,
        "rows_out": len(combined),
        "rows_flagged": rows_flagged,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
