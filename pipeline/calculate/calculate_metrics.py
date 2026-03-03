"""
Calculates all metrics per METRICS.md and writes to metrics tables.
"""

import logging
import sqlite3

import numpy as np
import pandas as pd

from pipeline.utils.cbsa_utils import load_pipeline_config

logger = logging.getLogger("pipeline.calculate.metrics")


def run(config: dict = None) -> dict:
    """Calculate all metrics and write to metrics tables."""
    if config is None:
        config = load_pipeline_config()

    db_path = config["data_paths"]["db"]
    baseline_start = config["calculate"]["baseline_start_year"]
    demolition_baseline = config["calculate"]["demolition_rates"]["baseline"]
    down_pct = config["calculate"]["mortgage_down_payment_pct"]

    conn = sqlite3.connect(db_path)
    try:
        return _calculate(conn, baseline_start, demolition_baseline, down_pct)
    finally:
        conn.close()


def _calculate(conn, baseline_start, demolition_baseline, down_pct):
    # Load base tables
    permits = pd.read_sql("SELECT * FROM permits", conn)
    population = pd.read_sql("SELECT * FROM population", conn)
    housing = pd.read_sql("SELECT * FROM housing_stock", conn)
    vacancy = pd.read_sql("SELECT * FROM vacancy", conn)
    fmr = pd.read_sql("SELECT * FROM fair_market_rents", conn)
    fred = pd.read_sql("SELECT * FROM fred_series", conn)
    cbsa_ref = pd.read_sql("SELECT * FROM cbsa_reference", conn)

    rows_written = 0

    # ---- NATIONAL METRICS ----
    national_metrics = _calc_national(
        permits, population, housing, fred, baseline_start, demolition_baseline
    )
    if not national_metrics.empty:
        national_metrics.to_sql("metrics_national_annual", conn, if_exists="replace", index=False)
        rows_written += len(national_metrics)
        logger.info(f"National metrics: {len(national_metrics)} rows")

    # ---- METRO METRICS ----
    metro_metrics = _calc_metro(
        permits, population, housing, vacancy, fmr, fred, national_metrics,
        baseline_start, demolition_baseline, down_pct
    )
    if not metro_metrics.empty:
        metro_metrics.to_sql("metrics_metro_annual", conn, if_exists="replace", index=False)
        rows_written += len(metro_metrics)
        logger.info(f"Metro metrics: {len(metro_metrics)} rows")

    conn.commit()
    return {"status": "SUCCESS", "rows_written": rows_written}


def _calc_national(permits, population, housing, fred, baseline_start, demolition_rate):
    """Calculate national-level annual metrics."""
    # National permits by year
    nat_permits = permits.groupby("year")["permits_total"].sum().reset_index()
    nat_permits.columns = ["year", "total_permits"]

    # Households from FRED TTLHHLD
    ttlhhld = fred[fred["series_id"] == "TTLHHLD"].copy()
    ttlhhld["date"] = pd.to_datetime(ttlhhld["date"])
    ttlhhld["year"] = ttlhhld["date"].dt.year
    ttlhhld = ttlhhld.groupby("year")["value"].mean().reset_index()
    ttlhhld.columns = ["year", "total_households"]
    ttlhhld["total_households"] = (ttlhhld["total_households"] * 1000).astype(int)  # FRED in thousands

    # Completions from FRED COMPUTSA
    completions = fred[fred["series_id"] == "COMPUTSA"].copy()
    completions["date"] = pd.to_datetime(completions["date"])
    completions["year"] = completions["date"].dt.year
    comp_annual = completions.groupby("year")["value"].sum().reset_index()
    comp_annual.columns = ["year", "total_completions"]
    comp_annual["total_completions"] = (comp_annual["total_completions"] * 1000 / 12).astype(int)

    # Starts from FRED HOUST
    starts = fred[fred["series_id"] == "HOUST"].copy()
    starts["date"] = pd.to_datetime(starts["date"])
    starts["year"] = starts["date"].dt.year
    starts_annual = starts.groupby("year")["value"].sum().reset_index()
    starts_annual.columns = ["year", "total_starts"]
    starts_annual["total_starts"] = (starts_annual["total_starts"] * 1000 / 12).astype(int)

    # Mortgage rate
    mortgage = fred[fred["series_id"] == "MORTGAGE30US"].copy()
    mortgage["date"] = pd.to_datetime(mortgage["date"])
    mortgage["year"] = mortgage["date"].dt.year
    mort_annual = mortgage.groupby("year")["value"].mean().reset_index()
    mort_annual.columns = ["year", "mortgage_rate_annual_avg"]
    mort_annual["mortgage_rate_annual_avg"] /= 100  # Convert % to decimal

    # Vacancy rates from FRED
    rental_vac = fred[fred["series_id"] == "RVACRATE"].copy()
    rental_vac["date"] = pd.to_datetime(rental_vac["date"])
    rental_vac["year"] = rental_vac["date"].dt.year
    rv_annual = rental_vac.groupby("year")["value"].mean().reset_index()
    rv_annual.columns = ["year", "rental_vacancy_rate"]
    rv_annual["rental_vacancy_rate"] /= 100

    ho_vac = fred[fred["series_id"] == "HCOVACRATE"].copy()
    ho_vac["date"] = pd.to_datetime(ho_vac["date"])
    ho_vac["year"] = ho_vac["date"].dt.year
    hov_annual = ho_vac.groupby("year")["value"].mean().reset_index()
    hov_annual.columns = ["year", "homeowner_vacancy_rate"]
    hov_annual["homeowner_vacancy_rate"] /= 100

    # Merge all
    result = nat_permits
    for df in [ttlhhld, comp_annual, starts_annual, mort_annual, rv_annual, hov_annual]:
        result = result.merge(df, on="year", how="outer")

    result = result.sort_values("year")

    # HH formation rate
    result["hh_formation_rate"] = result["total_households"].diff()

    # Completions vs formation gap
    result["completions_vs_formation_gap"] = (
        result["total_completions"] - result["hh_formation_rate"]
    )

    # Cumulative deficit since baseline_start
    result["cumulative_deficit_since_2008"] = None
    mask = result["year"] >= baseline_start
    if mask.any():
        # Annual surplus/deficit = completions - (formation + demolition)
        nat_housing_total = housing.groupby("year")["total_units"].sum()
        result_with_stock = result.merge(
            nat_housing_total.reset_index().rename(columns={"total_units": "nat_stock"}),
            on="year",
            how="left",
        )
        result_with_stock["demolition"] = result_with_stock["nat_stock"] * demolition_rate
        result_with_stock["annual_net"] = (
            result_with_stock["total_completions"]
            - result_with_stock["hh_formation_rate"]
            - result_with_stock["demolition"]
        )
        deficit = result_with_stock.loc[mask, "annual_net"].cumsum()
        result.loc[mask, "cumulative_deficit_since_2008"] = deficit.values

    result = result[result["year"] >= 2000]
    return result


def _calc_metro(permits, population, housing, vacancy, fmr, fred,
                national_metrics, baseline_start, demolition_rate, down_pct):
    """Calculate metro-level annual metrics."""
    # Join permits and population
    metro = permits.merge(population, on=["cbsa_code", "year"], how="outer", suffixes=("", "_pop"))

    # 1. Permits per 1,000 residents
    metro["permits_per_1000_residents"] = (
        metro["permits_total"] / metro["population"].replace(0, float("nan")) * 1000
    )

    # 2. National avg permits per 1000
    nat_rate = permits.merge(population, on=["cbsa_code", "year"])
    nat_annual = nat_rate.groupby("year").agg(
        total_permits=("permits_total", "sum"),
        total_pop=("population", "sum"),
    ).reset_index()
    nat_annual["national_permits_per_1000"] = nat_annual["total_permits"] / nat_annual["total_pop"] * 1000

    # 3. Permits vs national avg ratio
    metro = metro.merge(
        nat_annual[["year", "national_permits_per_1000"]], on="year", how="left"
    )
    metro["permits_vs_national_avg_ratio"] = (
        metro["permits_per_1000_residents"]
        / metro["national_permits_per_1000"].replace(0, float("nan"))
    )

    # 4. Vacancy rate annual avg and YoY change
    vac_annual = vacancy.groupby(["cbsa_code", "year"])["vacancy_rate"].mean().reset_index()
    vac_annual.columns = ["cbsa_code", "year", "vacancy_rate_annual_avg"]
    vac_annual = vac_annual.sort_values(["cbsa_code", "year"])
    vac_annual["vacancy_rate_yoy_change"] = vac_annual.groupby("cbsa_code")[
        "vacancy_rate_annual_avg"
    ].diff()

    metro = metro.merge(vac_annual, on=["cbsa_code", "year"], how="left")

    # 5. Implied new households (using ACS occupied units as proxy)
    hh = housing[["cbsa_code", "year", "occupied_units", "total_units",
                   "median_hh_income", "median_home_value", "median_gross_rent"]].copy()
    hh = hh.sort_values(["cbsa_code", "year"])
    hh["implied_new_households"] = hh.groupby("cbsa_code")["occupied_units"].diff()

    metro = metro.merge(
        hh[["cbsa_code", "year", "implied_new_households", "total_units",
            "median_hh_income", "median_home_value", "median_gross_rent"]],
        on=["cbsa_code", "year"],
        how="left",
    )

    # 6. Permits vs households gap
    metro["permits_vs_households_gap"] = (
        metro["implied_new_households"] - metro["permits_total"]
    )

    # 7. Pop growth per new unit
    metro = metro.sort_values(["cbsa_code", "year"])
    metro["_pop_change"] = metro.groupby("cbsa_code")["population"].diff()
    metro["pop_growth_per_new_unit"] = (
        metro["_pop_change"] / metro["permits_total"].replace(0, float("nan"))
    )

    # 8. Domestic migration per permit
    metro["domestic_migration_per_permit"] = (
        metro["domestic_migration_net"] / metro["permits_total"].replace(0, float("nan"))
    )

    # 9. Mortgage payment as % of median income
    # Get annual average mortgage rate
    mortgage = fred[fred["series_id"] == "MORTGAGE30US"].copy()
    mortgage["date"] = pd.to_datetime(mortgage["date"])
    mortgage["year"] = mortgage["date"].dt.year
    mort_annual = mortgage.groupby("year")["value"].mean().reset_index()
    mort_annual.columns = ["year", "mort_rate_pct"]
    mort_annual["monthly_rate"] = mort_annual["mort_rate_pct"] / 100 / 12

    metro = metro.merge(mort_annual, on="year", how="left")

    # Monthly mortgage = loan * r / (1 - (1+r)^-360)
    loan = metro["median_home_value"] * (1 - down_pct)
    r = metro["monthly_rate"]
    monthly_payment = loan * r / (1 - (1 + r) ** -360)
    annual_payment = monthly_payment * 12
    metro["mortgage_pct_median_income"] = (
        annual_payment / metro["median_hh_income"].replace(0, float("nan"))
    )

    # 10. FMR as % of median income
    fmr_data = fmr.rename(columns={"fiscal_year": "year"})
    metro = metro.merge(fmr_data[["cbsa_code", "year", "fmr_2br"]], on=["cbsa_code", "year"], how="left")
    metro["fmr_pct_median_income"] = (
        metro["fmr_2br"] * 12 / metro["median_hh_income"].replace(0, float("nan"))
    )

    # 11-12. Annual implied need and surplus/deficit
    metro["annual_implied_need"] = (
        metro["implied_new_households"].fillna(0)
        + demolition_rate * metro["total_units"].fillna(0)
    )

    # Use prior year permits as completions proxy (18-month lag)
    metro = metro.sort_values(["cbsa_code", "year"])
    metro["completions_proxy"] = metro.groupby("cbsa_code")["permits_total"].shift(1)
    metro["annual_surplus_deficit"] = metro["completions_proxy"] - metro["annual_implied_need"]

    # Completions to starts ratio (national proxy)
    metro["completions_to_starts_ratio"] = None
    if national_metrics is not None and not national_metrics.empty:
        nat_ratio = national_metrics[["year", "total_completions", "total_starts"]].copy()
        nat_ratio["c2s"] = nat_ratio["total_completions"] / nat_ratio["total_starts"].replace(0, float("nan"))
        metro = metro.merge(nat_ratio[["year", "c2s"]], on="year", how="left")
        metro["completions_to_starts_ratio"] = metro["c2s"]
        metro = metro.drop(columns=["c2s"], errors="ignore")

    # 13. Cumulative deficit since 2008
    metro = metro.sort_values(["cbsa_code", "year"])
    mask = metro["year"] >= baseline_start
    metro["cumulative_deficit_since_2008"] = None
    for cbsa in metro["cbsa_code"].unique():
        cbsa_mask = (metro["cbsa_code"] == cbsa) & mask
        if cbsa_mask.any():
            cumsum = metro.loc[cbsa_mask, "annual_surplus_deficit"].cumsum()
            metro.loc[cbsa_mask, "cumulative_deficit_since_2008"] = cumsum

    # DQ flag
    metro["dq_flag"] = None

    # Select final columns
    out_cols = [
        "cbsa_code", "year",
        "permits_per_1000_residents", "permits_vs_national_avg_ratio",
        "completions_to_starts_ratio", "vacancy_rate_annual_avg",
        "vacancy_rate_yoy_change", "implied_new_households",
        "permits_vs_households_gap", "pop_growth_per_new_unit",
        "domestic_migration_per_permit", "mortgage_pct_median_income",
        "fmr_pct_median_income", "annual_implied_need",
        "annual_surplus_deficit", "cumulative_deficit_since_2008", "dq_flag",
    ]

    result = metro[[c for c in out_cols if c in metro.columns]].copy()
    result = result.dropna(subset=["cbsa_code", "year"])
    result = result.drop_duplicates(subset=["cbsa_code", "year"], keep="last")

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
