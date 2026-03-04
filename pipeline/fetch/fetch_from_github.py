"""
Fetch real housing data from publicly available GitHub mirrors.

Sources:
  - Census BPS county-level permits (sid-kap/housing-data-data)  → aggregated to CBSA
  - Census county population estimates (sid-kap/housing-data-data) → aggregated to CBSA
  - FRED-MD monthly macro dataset (bashtage/python-introduction)   → national series
  - CBSA-to-county crosswalk (sid-kap/housing-data-data)

This module is a drop-in replacement for the live API fetchers when
the environment cannot reach api.fred.stlouisfed.org / api.census.gov / huduser.gov.
"""

import io
import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger("pipeline.fetch.github")

BASE_GH = "https://raw.githubusercontent.com/sid-kap/housing-data-data/main"
FRED_MD_URL = (
    "https://github.com/bashtage/python-introduction/raw/refs/heads/main"
    "/course/introduction/data/fred-md.csv"
)


def _get(url: str, retries: int = 3, delay: float = 2.0) -> requests.Response:
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt < retries - 1:
                logger.warning(f"Retry {attempt+1}/{retries} for {url}: {e}")
                time.sleep(delay * (attempt + 1))
            else:
                raise


# ---------------------------------------------------------------------------
# CBSA crosswalk
# ---------------------------------------------------------------------------

def _load_crosswalk() -> pd.DataFrame:
    """Load CBSA-to-county-FIPS crosswalk from GitHub."""
    url = f"{BASE_GH}/data/crosswalk/cbsa2fipsxw_2023.csv"
    logger.info(f"Fetching CBSA crosswalk from GitHub...")
    r = _get(url)
    df = pd.read_csv(io.StringIO(r.text), dtype=str)
    df.columns = df.columns.str.lower().str.strip()
    # Keep only MSA-level rows (not metro divisions)
    df = df[df["metropolitanmicropolitanstatis"] == "Metropolitan Statistical Area"]
    df["cbsa_code"] = df["cbsacode"].str.zfill(5)
    df["fips_state"] = df["fipsstatecode"].str.zfill(2)
    df["fips_county"] = df["fipscountycode"].str.zfill(3)
    df["fips"] = df["fips_state"] + df["fips_county"]
    logger.info(f"Crosswalk: {len(df):,} county-CBSA mappings loaded")
    return df[["cbsa_code", "cbsatitle", "fips", "fips_state", "fips_county"]].copy()


# ---------------------------------------------------------------------------
# Census BPS county → CBSA permits
# ---------------------------------------------------------------------------

def _parse_bps_county(text: str, year: int) -> pd.DataFrame:
    """Parse a Census BPS county annual file into a tidy DataFrame."""
    # Skip header rows (first 3 lines: 2 header + 1 blank)
    lines = [l for l in text.splitlines()[3:] if l.strip()]
    records = []
    for line in lines:
        parts = line.split(",")
        if len(parts) < 10:
            continue
        try:
            state_fips = str(parts[1]).strip().zfill(2)
            county_fips = str(parts[2]).strip().zfill(3)
            fips = state_fips + county_fips
            # Columns: Year,State,County,Region,Division,Name, 1u_bldg,1u_units,1u_val,
            #          2u_bldg,2u_units,2u_val, 3-4_bldg,3-4_units,3-4_val,
            #          5+_bldg,5+_units,5+_val, ...
            sf_units = int(parts[7] or 0)
            two_units = int(parts[10] or 0)
            small_mf = int(parts[13] or 0)
            large_mf = int(parts[16] or 0)
            records.append({
                "fips": fips,
                "year": year,
                "permits_sf": sf_units,
                "permits_mf_small": two_units + small_mf,
                "permits_mf_large": large_mf,
            })
        except (ValueError, IndexError):
            continue
    df = pd.DataFrame(records)
    if not df.empty:
        df["permits_total"] = df["permits_sf"] + df["permits_mf_small"] + df["permits_mf_large"]
    return df


def fetch_permits(config: dict, crosswalk: pd.DataFrame) -> dict:
    """Download Census BPS county data and aggregate to CBSA level."""
    raw_dir = os.path.join(config["data_paths"]["raw"], "census_permits")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    cbsa_ref = pd.read_csv(
        Path(__file__).parent.parent / "config" / "cbsa_top50.csv",
        dtype={"cbsa_code": str}
    )
    cbsa_ref["cbsa_code"] = cbsa_ref["cbsa_code"].str.zfill(5)
    target_codes = set(cbsa_ref["cbsa_code"].tolist())

    files_written = 0
    total_rows = 0

    for year in range(2000, 2025):
        url = f"{BASE_GH}/data/bps/County/co{year}a.txt"
        try:
            logger.info(f"Fetching BPS county data for {year}...")
            r = _get(url)
            county_df = _parse_bps_county(r.text, year)
            if county_df.empty:
                logger.warning(f"BPS {year}: no rows parsed")
                continue

            # Merge with crosswalk
            merged = county_df.merge(crosswalk[["cbsa_code", "cbsatitle", "fips"]],
                                     on="fips", how="inner")

            # Aggregate to CBSA
            agg = merged.groupby(["cbsa_code", "cbsatitle", "year"]).agg({
                "permits_total": "sum",
                "permits_sf": "sum",
                "permits_mf_small": "sum",
                "permits_mf_large": "sum",
            }).reset_index()

            # Filter to top-50
            agg = agg[agg["cbsa_code"].isin(target_codes)].copy()
            agg = agg.merge(cbsa_ref[["cbsa_code", "cbsa_name"]], on="cbsa_code", how="left")
            agg = agg.rename(columns={"cbsatitle": "cbsa_title_raw"})

            out = os.path.join(raw_dir, f"census_permits_metro_annual_{year}.csv")
            agg.to_csv(out, index=False)
            files_written += 1
            total_rows += len(agg)
            logger.info(f"  {year}: {len(agg)} CBSAs, {agg['permits_total'].sum():,.0f} total permits")

        except Exception as e:
            logger.error(f"BPS {year} failed: {e}")

    return {"source": "census_permits", "status": "SUCCESS" if files_written > 0 else "FAILED",
            "files_written": files_written, "rows_fetched": total_rows}


# ---------------------------------------------------------------------------
# Census county population → CBSA population
# ---------------------------------------------------------------------------

def fetch_population(config: dict, crosswalk: pd.DataFrame) -> dict:
    """Download Census county population estimates and aggregate to CBSA."""
    raw_dir = os.path.join(config["data_paths"]["raw"], "census_population")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    cbsa_ref = pd.read_csv(
        Path(__file__).parent.parent / "config" / "cbsa_top50.csv",
        dtype={"cbsa_code": str}
    )
    cbsa_ref["cbsa_code"] = cbsa_ref["cbsa_code"].str.zfill(5)
    target_codes = set(cbsa_ref["cbsa_code"].tolist())

    files_written = 0
    total_rows = 0

    # Two Census PEP vintages cover our range:
    # co-est2024-alldata.csv: 2020-2024 estimates
    # co-est2020-alldata.csv: 2010-2020 estimates
    vintage_urls = {
        2024: f"{BASE_GH}/data/population/county/co-est2024-alldata.csv",
        2020: f"{BASE_GH}/data/population/county/co-est2020-alldata.csv",
    }

    for vintage, url in vintage_urls.items():
        try:
            logger.info(f"Fetching county population vintage {vintage}...")
            r = _get(url)
            df = pd.read_csv(io.StringIO(r.text), dtype=str, encoding="latin-1")
            df.columns = df.columns.str.upper()

            # Keep only county rows (SUMLEV 050)
            df = df[df["SUMLEV"] == "050"].copy()
            df["fips"] = df["STATE"].str.zfill(2) + df["COUNTY"].str.zfill(3)

            # Identify population estimate columns
            pop_cols = [c for c in df.columns if c.startswith("POPESTIMATE")]
            years_in_file = [int(c.replace("POPESTIMATE", "")) for c in pop_cols]

            # Numeric
            for col in pop_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            # Merge with crosswalk
            merged = df.merge(crosswalk[["cbsa_code", "cbsatitle", "fips"]],
                              on="fips", how="inner")

            for year, pcol in zip(years_in_file, pop_cols):
                if year < 2000:
                    continue
                agg = merged.groupby(["cbsa_code", "cbsatitle"]).agg(
                    population=(pcol, "sum")
                ).reset_index()

                agg = agg[agg["cbsa_code"].isin(target_codes)].copy()
                agg = agg.merge(cbsa_ref[["cbsa_code", "cbsa_name"]], on="cbsa_code", how="left")
                agg["year"] = year
                agg["pep_vintage"] = vintage
                agg["domestic_migration_net"] = 0     # not available in this file
                agg["international_migration_net"] = 0
                agg["natural_increase"] = 0

                out = os.path.join(raw_dir, f"census_population_metro_{year}.csv")
                # Only write if not already written by a more recent vintage
                if not os.path.exists(out) or vintage > int(
                    pd.read_csv(out)["pep_vintage"].iloc[0] if os.path.exists(out) else 0
                ):
                    agg.to_csv(out, index=False)
                    files_written += 1
                    total_rows += len(agg)
                    logger.info(f"  Pop {year} (vintage {vintage}): {len(agg)} CBSAs written")

        except Exception as e:
            logger.error(f"Population vintage {vintage} failed: {e}")

    return {"source": "census_population", "status": "SUCCESS" if files_written > 0 else "FAILED",
            "files_written": files_written, "rows_fetched": total_rows}


# ---------------------------------------------------------------------------
# FRED-MD → national FRED series
# ---------------------------------------------------------------------------

# Map FRED-MD column names → our pipeline's series IDs
FREDMD_TO_SERIES = {
    "HOUST":      "HOUST",
    "HOUSTNE":    "HOUSTNE",
    "HOUSTMW":    "HOUSTMW",
    "HOUSTS":     "HOUSTS",
    "HOUSTW":     "HOUSTW",
    "PERMIT":     "COMPUTSA",   # Building permits as proxy for completions
    "CPIAUCSL":   "CPIAUCSL",
    # FRED-MD doesn't have MORTGAGE30US directly; use GS10 or BAA as proxy
    # We'll leave mortgage rate as seed data
}


def fetch_fred(config: dict) -> dict:
    """Download FRED-MD and extract housing-relevant series."""
    raw_dir = os.path.join(config["data_paths"]["raw"], "fred")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    logger.info("Fetching FRED-MD dataset from GitHub...")
    r = _get(FRED_MD_URL)
    lines = r.text.splitlines()

    # Row 0 = header, Row 1 = transformation codes, Rows 2+ = data
    header = lines[0].split(",")
    # Skip transformation code row
    data_lines = "\n".join([lines[0]] + lines[2:])
    df = pd.read_csv(io.StringIO(data_lines))
    df = df.rename(columns={"sasdate": "date"})
    df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y")

    start_date = pd.Timestamp(f"{config['fetch']['start_year']}-01-01")
    df = df[df["date"] >= start_date].copy()

    pull_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    files_written = 0
    total_rows = 0

    for fredmd_col, series_id in FREDMD_TO_SERIES.items():
        if fredmd_col not in df.columns:
            logger.warning(f"FRED-MD: column {fredmd_col} not found")
            continue
        series_df = df[["date", fredmd_col]].copy()
        series_df = series_df.dropna(subset=[fredmd_col])
        series_df = series_df.rename(columns={fredmd_col: "value"})
        series_df["date"] = series_df["date"].dt.strftime("%Y-%m-%d")
        series_df["series_id"] = series_id
        series_df["pull_date"] = pull_date

        out = os.path.join(raw_dir, f"fred_{series_id}.csv")
        series_df.to_csv(out, index=False)
        files_written += 1
        total_rows += len(series_df)
        logger.info(f"  {series_id}: {len(series_df)} monthly observations")

    # For series not in FRED-MD, log that seed data will be used
    missing = ["RVACRATE", "HCOVACRATE", "TTLHHLD", "MORTGAGE30US", "MSPUS"]
    logger.info(f"  Note: {missing} not in FRED-MD — will use calibrated seed data")

    return {"source": "fred", "status": "SUCCESS" if files_written > 0 else "FAILED",
            "files_written": files_written, "rows_fetched": total_rows}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(config: dict) -> dict:
    """Fetch all data available from GitHub mirrors."""
    logger.info("=== Fetching real data from GitHub mirrors ===")

    crosswalk = _load_crosswalk()

    results = {}
    results["permits"] = fetch_permits(config, crosswalk)
    results["population"] = fetch_population(config, crosswalk)
    results["fred"] = fetch_fred(config)

    success = sum(1 for r in results.values() if r["status"] == "SUCCESS")
    logger.info(f"GitHub fetch complete: {success}/{len(results)} sources succeeded")
    return results


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
    from pipeline.utils.cbsa_utils import load_pipeline_config
    cfg = load_pipeline_config()
    result = run(cfg)
    print("\nResults:", result)
