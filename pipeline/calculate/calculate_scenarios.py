"""
Generates the precalculated scenario grid.
"""

import logging
import sqlite3
from itertools import product

import pandas as pd

from pipeline.utils.cbsa_utils import load_pipeline_config, load_scenario_params

logger = logging.getLogger("pipeline.calculate.scenarios")


def run(config: dict = None) -> dict:
    """Generate scenario grid for all metros and national."""
    if config is None:
        config = load_pipeline_config()

    db_path = config["data_paths"]["db"]
    params = load_scenario_params()
    calc_config = config["calculate"]

    conn = sqlite3.connect(db_path)
    try:
        return _calculate(conn, params, calc_config)
    finally:
        conn.close()


def _calculate(conn, params, calc_config):
    # Load needed data
    metrics = pd.read_sql("SELECT * FROM metrics_metro_annual", conn)
    permits = pd.read_sql("SELECT * FROM permits", conn)
    population = pd.read_sql("SELECT * FROM population", conn)
    housing = pd.read_sql("SELECT * FROM housing_stock", conn)
    cbsa_ref = pd.read_sql("SELECT * FROM cbsa_reference", conn)
    national = pd.read_sql("SELECT * FROM metrics_national_annual", conn)

    hh_levels = list(params["hh_formation"].keys())
    demo_levels = list(params["demolition"].keys())
    mig_levels = list(params["migration"].keys())
    horizons = params["horizon"]["values"]

    # ---- METRO SCENARIOS ----
    metro_rows = []

    for _, cbsa in cbsa_ref.iterrows():
        cbsa_code = cbsa["cbsa_code"]
        cbsa_name = cbsa["cbsa_name"]

        # Get latest metrics
        cbsa_metrics = metrics[metrics["cbsa_code"] == cbsa_code].sort_values("year")
        if cbsa_metrics.empty:
            logger.warning(f"No metrics for {cbsa_code}, skipping scenarios")
            continue

        latest = cbsa_metrics.iloc[-1]
        latest_year = int(latest["year"])

        # Current baseline deficit
        current_deficit = latest.get("cumulative_deficit_since_2008", 0)
        if pd.isna(current_deficit):
            current_deficit = 0
        current_deficit = int(current_deficit)

        # Population trend (linear extrapolation of last 3 years)
        cbsa_pop = population[population["cbsa_code"] == cbsa_code].sort_values("year")
        if len(cbsa_pop) >= 3:
            recent_pop = cbsa_pop.tail(3)
            pop_trend = recent_pop["population"].diff().mean()
        else:
            pop_trend = 0
        if pd.isna(pop_trend):
            pop_trend = 0

        # Implied HH formation trend (last 3 years average)
        recent_metrics = cbsa_metrics.tail(3)
        hh_trend = recent_metrics["implied_new_households"].mean()
        if pd.isna(hh_trend):
            hh_trend = 0

        # Trailing permit rate
        cbsa_permits = permits[permits["cbsa_code"] == cbsa_code].sort_values("year")
        if not cbsa_permits.empty:
            trailing_permit_rate = cbsa_permits.tail(1)["permits_total"].values[0]
        else:
            trailing_permit_rate = 0
        if pd.isna(trailing_permit_rate):
            trailing_permit_rate = 0

        # Current housing stock
        cbsa_stock = housing[housing["cbsa_code"] == cbsa_code].sort_values("year")
        if not cbsa_stock.empty:
            current_stock = cbsa_stock.iloc[-1].get("total_units", 0)
        else:
            current_stock = 0
        if pd.isna(current_stock):
            current_stock = 0

        # Net domestic migration trend
        if not cbsa_pop.empty and "domestic_migration_net" in cbsa_pop.columns:
            recent_mig = cbsa_pop.tail(3)["domestic_migration_net"].mean()
        else:
            recent_mig = 0
        if pd.isna(recent_mig):
            recent_mig = 0

        for hh_level, demo_level, mig_level, horizon in product(
            hh_levels, demo_levels, mig_levels, horizons
        ):
            hh_adj = params["hh_formation"][hh_level]["adjustment"]
            demo_rate = params["demolition"][demo_level]["rate"]
            mig_adj = params["migration"][mig_level]["adjustment"]

            projected_hh = 0
            projected_comp = 0
            projected_deficit = current_deficit

            for yr in range(1, horizon + 1):
                # Projected households: base formation * hh adjustment + migration adjustment
                yr_hh = hh_trend * hh_adj + recent_mig * (mig_adj - 1.0)
                projected_hh += yr_hh

                # Demolition
                yr_demolition = current_stock * demo_rate

                # Completions: permit backlog first year, then trailing rate
                yr_completions = trailing_permit_rate

                # Annual net
                yr_net = yr_completions - yr_hh - yr_demolition
                projected_deficit += yr_net
                projected_comp += yr_completions

            projected_surplus_deficit = int(projected_comp - projected_hh - current_stock * demo_rate * horizon)

            scenario_label = (
                f"{params['hh_formation'][hh_level]['label']} | "
                f"{params['demolition'][demo_level]['label']} | "
                f"{params['migration'][mig_level]['label']} | "
                f"{horizon}yr"
            )

            metro_rows.append({
                "cbsa_code": cbsa_code,
                "hh_formation_assumption": hh_level,
                "demolition_assumption": demo_level,
                "migration_assumption": mig_level,
                "horizon_years": horizon,
                "projected_new_households": int(projected_hh),
                "projected_completions": int(projected_comp),
                "projected_surplus_deficit": projected_surplus_deficit,
                "current_deficit_baseline": current_deficit,
                "end_state_deficit": int(projected_deficit),
                "scenario_label": scenario_label,
            })

    metro_grid = pd.DataFrame(metro_rows)
    if not metro_grid.empty:
        metro_grid.to_sql("scenario_grid", conn, if_exists="replace", index=False)
        logger.info(f"Scenario grid: {len(metro_grid)} rows ({len(metro_grid) // 81} metros x 81)")

    # ---- NATIONAL SCENARIOS ----
    national_rows = []
    if not national.empty:
        nat_latest = national.sort_values("year").iloc[-1]
        nat_deficit = int(nat_latest.get("cumulative_deficit_since_2008", 0) or 0)
        nat_hh_trend = national.tail(3)["hh_formation_rate"].mean()
        if pd.isna(nat_hh_trend):
            nat_hh_trend = 0
        nat_comp_trend = national.tail(3)["total_completions"].mean()
        if pd.isna(nat_comp_trend):
            nat_comp_trend = 0

        nat_stock = housing.groupby("year")["total_units"].sum()
        current_nat_stock = nat_stock.iloc[-1] if not nat_stock.empty else 0

        for hh_level, demo_level, mig_level, horizon in product(
            hh_levels, demo_levels, ["flat"], horizons  # Migration is not applied nationally
        ):
            hh_adj = params["hh_formation"][hh_level]["adjustment"]
            demo_rate = params["demolition"][demo_level]["rate"]

            proj_hh = nat_hh_trend * hh_adj * horizon
            proj_comp = nat_comp_trend * horizon
            proj_demo = current_nat_stock * demo_rate * horizon
            proj_surplus = int(proj_comp - proj_hh - proj_demo)
            end_deficit = int(nat_deficit + proj_surplus)

            scenario_label = (
                f"{params['hh_formation'][hh_level]['label']} | "
                f"{params['demolition'][demo_level]['label']} | "
                f"{horizon}yr"
            )

            national_rows.append({
                "hh_formation_assumption": hh_level,
                "demolition_assumption": demo_level,
                "migration_assumption": "flat",
                "horizon_years": horizon,
                "projected_new_households": int(proj_hh),
                "projected_completions": int(proj_comp),
                "projected_surplus_deficit": proj_surplus,
                "current_deficit_baseline": nat_deficit,
                "end_state_deficit": end_deficit,
                "scenario_label": scenario_label,
            })

    nat_grid = pd.DataFrame(national_rows)
    if not nat_grid.empty:
        nat_grid.to_sql("scenario_grid_national", conn, if_exists="replace", index=False)
        logger.info(f"National scenario grid: {len(nat_grid)} rows")

    conn.commit()

    total = len(metro_grid) + len(nat_grid)
    return {"status": "SUCCESS", "rows_written": total}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
