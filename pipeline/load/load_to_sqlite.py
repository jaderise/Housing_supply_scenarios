"""
Reads all processed CSVs and loads to SQLite. Drops and recreates all tables on each run.
"""

import logging
import os
import sqlite3
from pathlib import Path

import pandas as pd

from pipeline.utils.cbsa_utils import load_pipeline_config, load_cbsa_top50
from pipeline.utils.dq_checks import ensure_dq_log_table

logger = logging.getLogger("pipeline.load")

SCHEMA_DDL = {
    "cbsa_reference": """
        CREATE TABLE cbsa_reference (
            cbsa_code       TEXT PRIMARY KEY,
            cbsa_name       TEXT NOT NULL,
            principal_city  TEXT,
            states          TEXT,
            population_rank INTEGER,
            region          TEXT,
            sun_belt        INTEGER
        )
    """,
    "permits": """
        CREATE TABLE permits (
            cbsa_code       TEXT REFERENCES cbsa_reference(cbsa_code),
            year            INTEGER,
            permits_total   INTEGER,
            permits_sf      INTEGER,
            permits_mf_small INTEGER,
            permits_mf_large INTEGER,
            dq_flag         TEXT,
            PRIMARY KEY (cbsa_code, year)
        )
    """,
    "population": """
        CREATE TABLE population (
            cbsa_code               TEXT REFERENCES cbsa_reference(cbsa_code),
            year                    INTEGER,
            population              INTEGER,
            domestic_migration_net  INTEGER,
            international_migration_net INTEGER,
            natural_increase        INTEGER,
            pep_vintage             INTEGER,
            dq_flag                 TEXT,
            PRIMARY KEY (cbsa_code, year)
        )
    """,
    "housing_stock": """
        CREATE TABLE housing_stock (
            cbsa_code           TEXT REFERENCES cbsa_reference(cbsa_code),
            year                INTEGER,
            total_units         INTEGER,
            occupied_units      INTEGER,
            vacant_units        INTEGER,
            owner_occupied      INTEGER,
            renter_occupied     INTEGER,
            avg_household_size  REAL,
            median_hh_income    INTEGER,
            median_home_value   INTEGER,
            median_gross_rent   INTEGER,
            acs_type            TEXT,
            dq_flag             TEXT,
            PRIMARY KEY (cbsa_code, year)
        )
    """,
    "vacancy": """
        CREATE TABLE vacancy (
            cbsa_code                   TEXT REFERENCES cbsa_reference(cbsa_code),
            year                        INTEGER,
            quarter                     INTEGER,
            total_residential_addresses INTEGER,
            vacant_addresses            INTEGER,
            no_stat_addresses           INTEGER,
            vacancy_rate                REAL,
            vacancy_rate_incl_nostat    REAL,
            dq_flag                     TEXT,
            PRIMARY KEY (cbsa_code, year, quarter)
        )
    """,
    "fair_market_rents": """
        CREATE TABLE fair_market_rents (
            cbsa_code   TEXT REFERENCES cbsa_reference(cbsa_code),
            fiscal_year INTEGER,
            fmr_2br     INTEGER,
            dq_flag     TEXT,
            PRIMARY KEY (cbsa_code, fiscal_year)
        )
    """,
    "fred_series": """
        CREATE TABLE fred_series (
            series_id   TEXT,
            date        TEXT,
            value       REAL,
            pull_date   TEXT,
            PRIMARY KEY (series_id, date)
        )
    """,
    "metrics_metro_annual": """
        CREATE TABLE metrics_metro_annual (
            cbsa_code                       TEXT REFERENCES cbsa_reference(cbsa_code),
            year                            INTEGER,
            permits_per_1000_residents      REAL,
            permits_vs_national_avg_ratio   REAL,
            completions_to_starts_ratio     REAL,
            vacancy_rate_annual_avg         REAL,
            vacancy_rate_yoy_change         REAL,
            implied_new_households          INTEGER,
            permits_vs_households_gap       INTEGER,
            pop_growth_per_new_unit         REAL,
            domestic_migration_per_permit   REAL,
            mortgage_pct_median_income      REAL,
            fmr_pct_median_income           REAL,
            annual_implied_need             INTEGER,
            annual_surplus_deficit          INTEGER,
            cumulative_deficit_since_2008   INTEGER,
            dq_flag                         TEXT,
            PRIMARY KEY (cbsa_code, year)
        )
    """,
    "metrics_national_annual": """
        CREATE TABLE metrics_national_annual (
            year                            INTEGER PRIMARY KEY,
            total_households                INTEGER,
            total_completions               INTEGER,
            total_starts                    INTEGER,
            total_permits                   INTEGER,
            hh_formation_rate               REAL,
            completions_vs_formation_gap    INTEGER,
            cumulative_deficit_since_2008   INTEGER,
            mortgage_rate_annual_avg        REAL,
            rental_vacancy_rate             REAL,
            homeowner_vacancy_rate          REAL
        )
    """,
    "scenario_grid": """
        CREATE TABLE scenario_grid (
            cbsa_code               TEXT REFERENCES cbsa_reference(cbsa_code),
            hh_formation_assumption TEXT,
            demolition_assumption   TEXT,
            migration_assumption    TEXT,
            horizon_years           INTEGER,
            projected_new_households    INTEGER,
            projected_completions       INTEGER,
            projected_surplus_deficit   INTEGER,
            current_deficit_baseline    INTEGER,
            end_state_deficit           INTEGER,
            scenario_label              TEXT,
            PRIMARY KEY (cbsa_code, hh_formation_assumption, demolition_assumption, migration_assumption, horizon_years)
        )
    """,
    "scenario_grid_national": """
        CREATE TABLE scenario_grid_national (
            hh_formation_assumption TEXT,
            demolition_assumption   TEXT,
            migration_assumption    TEXT,
            horizon_years           INTEGER,
            projected_new_households    INTEGER,
            projected_completions       INTEGER,
            projected_surplus_deficit   INTEGER,
            current_deficit_baseline    INTEGER,
            end_state_deficit           INTEGER,
            scenario_label              TEXT,
            PRIMARY KEY (hh_formation_assumption, demolition_assumption, migration_assumption, horizon_years)
        )
    """,
}

# Mapping: table_name -> (csv_filename, column_mapping)
CSV_TO_TABLE = {
    "permits": {
        "file": "permits_metro_annual.csv",
        "columns": {
            "cbsa_code": "cbsa_code",
            "year": "year",
            "permits_total": "permits_total",
            "permits_sf": "permits_sf",
            "permits_mf_small": "permits_mf_small",
            "permits_mf_large": "permits_mf_large",
            "dq_flag": "dq_flag",
        },
    },
    "population": {
        "file": "population_metro_annual.csv",
        "columns": {
            "cbsa_code": "cbsa_code",
            "year": "year",
            "population": "population",
            "domestic_migration_net": "domestic_migration_net",
            "international_migration_net": "international_migration_net",
            "natural_increase": "natural_increase",
            "pep_vintage": "pep_vintage",
            "dq_flag": "dq_flag",
        },
    },
    "housing_stock": {
        "file": "acs_metro_annual.csv",
        "columns": {
            "cbsa_code": "cbsa_code",
            "year": "year",
            "total_housing_units": "total_units",
            "occupied_units": "occupied_units",
            "vacant_units": "vacant_units",
            "owner_occupied": "owner_occupied",
            "renter_occupied": "renter_occupied",
            "avg_household_size": "avg_household_size",
            "median_hh_income": "median_hh_income",
            "median_home_value": "median_home_value",
            "median_gross_rent": "median_gross_rent",
            "acs_type": "acs_type",
            "dq_flag": "dq_flag",
        },
    },
    "vacancy": {
        "file": "vacancy_metro_quarterly.csv",
        "columns": {
            "cbsa_code": "cbsa_code",
            "year": "year",
            "quarter": "quarter",
            "total_residential_addresses": "total_residential_addresses",
            "vacant_addresses": "vacant_addresses",
            "no_stat_addresses": "no_stat_addresses",
            "vacancy_rate": "vacancy_rate",
            "vacancy_rate_incl_nostat": "vacancy_rate_incl_nostat",
            "dq_flag": "dq_flag",
        },
    },
    "fair_market_rents": {
        "file": "fmr_metro_annual.csv",
        "columns": {
            "cbsa_code": "cbsa_code",
            "fiscal_year": "fiscal_year",
            "fmr_2br": "fmr_2br",
            "dq_flag": "dq_flag",
        },
    },
    "fred_series": {
        "file": "fred_national_series.csv",
        "columns": {
            "series_id": "series_id",
            "date": "date",
            "value": "value",
            "pull_date": "pull_date",
        },
    },
}


def run(config: dict = None) -> dict:
    """Load all processed CSVs into SQLite."""
    if config is None:
        config = load_pipeline_config()

    db_path = config["data_paths"]["db"]
    processed_dir = config["data_paths"]["processed"]
    Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    total_rows = 0
    tables_loaded = 0

    try:
        # Drop all tables except dq_log
        for table_name in SCHEMA_DDL:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Create all tables
        for table_name, ddl in SCHEMA_DDL.items():
            conn.execute(ddl)

        # Ensure dq_log exists
        ensure_dq_log_table(db_path)

        # Load cbsa_reference
        cbsa_df = load_cbsa_top50()
        cbsa_df.to_sql("cbsa_reference", conn, if_exists="replace", index=False)
        tables_loaded += 1
        total_rows += len(cbsa_df)
        logger.info(f"Loaded cbsa_reference: {len(cbsa_df)} rows")

        # Load each processed CSV
        for table_name, spec in CSV_TO_TABLE.items():
            csv_path = os.path.join(processed_dir, spec["file"])
            if not os.path.exists(csv_path):
                logger.warning(f"Processed file not found: {csv_path}")
                continue

            df = pd.read_csv(csv_path, dtype={"cbsa_code": str})

            # Rename columns per mapping
            rename_map = {}
            for src_col, dest_col in spec["columns"].items():
                if src_col in df.columns and src_col != dest_col:
                    rename_map[src_col] = dest_col
            if rename_map:
                df = df.rename(columns=rename_map)

            # Select only columns that exist in the table
            table_cols = list(spec["columns"].values())
            available = [c for c in table_cols if c in df.columns]
            df = df[available]

            # Drop duplicates on primary key
            if table_name == "fred_series":
                df = df.drop_duplicates(subset=["series_id", "date"], keep="last")
            elif table_name == "vacancy":
                df = df.drop_duplicates(subset=["cbsa_code", "year", "quarter"], keep="last")
            elif table_name == "fair_market_rents":
                df = df.drop_duplicates(subset=["cbsa_code", "fiscal_year"], keep="last")
            elif "year" in df.columns and "cbsa_code" in df.columns:
                df = df.drop_duplicates(subset=["cbsa_code", "year"], keep="last")

            df.to_sql(table_name, conn, if_exists="replace", index=False)
            tables_loaded += 1
            total_rows += len(df)
            logger.info(f"Loaded {table_name}: {len(df)} rows")

        conn.commit()
    finally:
        conn.close()

    logger.info(f"Load complete: {tables_loaded} tables, {total_rows} total rows")
    return {
        "status": "SUCCESS",
        "tables_loaded": tables_loaded,
        "total_rows": total_rows,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run()
    print(result)
