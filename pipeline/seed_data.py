"""
Seed data generator for development and pipeline validation.

Generates realistic raw CSV files for all 6 data sources based on
published housing market statistics. Use this when live API access
is unavailable (e.g., sandboxed environments).

Usage: python -m pipeline.seed_data
"""

import logging
import os
import random
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.utils.cbsa_utils import load_cbsa_top50, load_pipeline_config

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger("seed_data")

random.seed(42)
np.random.seed(42)

# ---------------------------------------------------------------------------
# Realistic metro-level parameters (approximate, based on published data)
# Keys: cbsa_code -> dict of baseline parameters
# ---------------------------------------------------------------------------

# Population estimates (2023 approx, in thousands)
METRO_POP_2023 = {
    "35620": 19500, "31080": 12900, "16980": 9400, "19100": 8100,
    "26420": 7300, "47900": 6400, "33100": 6200, "37980": 6250,
    "12060": 6200, "14460": 4950, "38060": 5050, "40140": 4700,
    "41860": 4550, "42660": 4050, "33460": 3700, "41740": 3350,
    "45300": 3300, "19740": 2970, "41700": 2600, "38900": 2510,
    "36740": 2730, "40900": 2420, "38300": 2370, "12420": 2470,
    "17460": 2260, "16740": 2750, "29820": 2330, "34980": 2050,
    "26900": 2120, "41180": 2010, "27260": 1700, "18140": 2180,
    "47260": 1830, "14860": 950, "39580": 1480, "13820": 1120,
    "36420": 1470, "33340": 1560, "40060": 1340, "28140": 2210,
    "35380": 1270, "31140": 1300, "41620": 1260, "39300": 1630,
    "32820": 1340, "25540": 1210, "15380": 1130, "40380": 1090,
    "24340": 1100, "46060": 1050,
}

# Permits per 1000 residents baseline (2019 level, approximate)
# Sun Belt metros have higher rates; coastal constrained metros lower
PERMITS_PER_1K = {
    "35620": 2.5, "31080": 2.8, "16980": 3.0, "19100": 8.5,
    "26420": 8.0, "47900": 4.0, "33100": 5.5, "37980": 2.5,
    "12060": 7.5, "14460": 3.5, "38060": 10.0, "40140": 5.0,
    "41860": 3.0, "42660": 5.5, "33460": 4.0, "41740": 3.5,
    "45300": 8.0, "19740": 6.5, "41700": 7.0, "38900": 5.0,
    "36740": 8.5, "40900": 5.0, "38300": 2.0, "12420": 12.0,
    "17460": 4.0, "16740": 9.5, "29820": 9.0, "34980": 8.0,
    "26900": 5.5, "41180": 3.0, "27260": 8.0, "18140": 5.0,
    "47260": 3.5, "14860": 1.5, "39580": 10.0, "13820": 3.5,
    "36420": 5.0, "33340": 2.5, "40060": 4.5, "28140": 5.0,
    "35380": 3.0, "31140": 4.5, "41620": 7.0, "39300": 2.0,
    "32820": 3.5, "25540": 1.5, "15380": 1.5, "40380": 1.5,
    "24340": 5.5, "46060": 5.5,
}

# Median household income (2022 approx)
METRO_INCOME = {
    "35620": 82000, "31080": 80000, "16980": 72000, "19100": 73000,
    "26420": 68000, "47900": 108000, "33100": 62000, "37980": 72000,
    "12060": 72000, "14460": 96000, "38060": 68000, "40140": 65000,
    "41860": 120000, "42660": 100000, "33460": 82000, "41740": 82000,
    "45300": 58000, "19740": 82000, "41700": 55000, "38900": 76000,
    "36740": 56000, "40900": 75000, "38300": 60000, "12420": 82000,
    "17460": 65000, "16740": 62000, "29820": 60000, "34980": 65000,
    "26900": 60000, "41180": 130000, "27260": 62000, "18140": 62000,
    "47260": 62000, "14860": 95000, "39580": 68000, "13820": 52000,
    "36420": 55000, "33340": 58000, "40060": 70000, "28140": 65000,
    "35380": 52000, "31140": 58000, "41620": 72000, "39300": 65000,
    "32820": 52000, "25540": 72000, "15380": 56000, "40380": 58000,
    "24340": 62000, "46060": 52000,
}

# Median home value (2022 approx)
METRO_HOME_VALUE = {
    "35620": 550000, "31080": 700000, "16980": 280000, "19100": 330000,
    "26420": 280000, "47900": 530000, "33100": 420000, "37980": 290000,
    "12060": 340000, "14460": 550000, "38060": 370000, "40140": 450000,
    "41860": 1050000, "42660": 580000, "33460": 310000, "41740": 700000,
    "45300": 310000, "19740": 510000, "41700": 250000, "38900": 470000,
    "36740": 340000, "40900": 470000, "38300": 175000, "12420": 430000,
    "17460": 225000, "16740": 320000, "29820": 370000, "34980": 350000,
    "26900": 225000, "41180": 1300000, "27260": 300000, "18140": 230000,
    "47260": 280000, "14860": 410000, "39580": 350000, "13820": 180000,
    "36420": 200000, "33340": 230000, "40060": 290000, "28140": 235000,
    "35380": 200000, "31140": 220000, "41620": 390000, "39300": 310000,
    "32820": 180000, "25540": 260000, "15380": 180000, "40380": 175000,
    "24340": 240000, "46060": 260000,
}


def _housing_cycle_factor(year: int) -> float:
    """Model the U.S. housing cycle: boom→bust→recovery→pandemic boom."""
    if year <= 2005:
        return 0.85 + 0.05 * (year - 2000)  # Build-up
    elif year <= 2006:
        return 1.10  # Peak
    elif year <= 2009:
        return 1.10 - 0.22 * (year - 2006)  # Crash
    elif year <= 2011:
        return 0.44  # Trough
    elif year <= 2019:
        return 0.44 + 0.07 * (year - 2011)  # Recovery
    elif year == 2020:
        return 0.95  # COVID dip
    elif year <= 2022:
        return 1.05 + 0.05 * (year - 2020)  # Pandemic boom
    else:
        return 1.05  # Normalization


def _pop_growth_rate(cbsa_code: str, year: int) -> float:
    """Annual population growth rate varies by metro and era."""
    cbsa = load_cbsa_top50()
    row = cbsa[cbsa["cbsa_code"] == cbsa_code].iloc[0]
    is_sun_belt = row["sun_belt"] == 1

    base_rate = 0.012 if is_sun_belt else 0.004

    # Pandemic migration boost for Sun Belt
    if is_sun_belt and year >= 2020:
        base_rate += 0.008
    elif not is_sun_belt and year >= 2020:
        base_rate -= 0.002

    # Northeast/Midwest slower growth
    if row["region"] in ("Northeast", "Midwest"):
        base_rate -= 0.003

    return max(base_rate + np.random.normal(0, 0.002), -0.005)


def seed_census_permits(config: dict):
    """Generate permit data: one CSV per year."""
    raw_dir = os.path.join(config["data_paths"]["raw"], "census_permits")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    cbsa_df = load_cbsa_top50()

    for year in range(2000, 2025):
        cycle = _housing_cycle_factor(year)
        records = []

        for _, row in cbsa_df.iterrows():
            code = row["cbsa_code"]
            pop = METRO_POP_2023.get(code, 1000)
            base_rate = PERMITS_PER_1K.get(code, 4.0)

            # Scale pop backward from 2023
            years_back = 2023 - year
            pop_scaled = pop * (1 - 0.005 * years_back)

            total = int(pop_scaled * base_rate * cycle * (1 + np.random.normal(0, 0.08)))
            total = max(total, 100)

            sf_share = 0.55 + np.random.normal(0, 0.05)
            sf_share = max(0.3, min(0.85, sf_share))

            permits_sf = int(total * sf_share)
            permits_mf = total - permits_sf
            permits_2_4 = int(permits_mf * 0.1)
            permits_5plus = permits_mf - permits_2_4

            records.append({
                "cbsa_code": code,
                "cbsa_name": row["cbsa_name"],
                "year": year,
                "permits_total": total,
                "permits_sf": permits_sf,
                "permits_mf_small": permits_2_4,
                "permits_mf_large": permits_5plus,
            })

        df = pd.DataFrame(records)
        out = os.path.join(raw_dir, f"census_permits_metro_annual_{year}.csv")
        df.to_csv(out, index=False)

    logger.info(f"Census permits: 25 annual files written to {raw_dir}")


def seed_census_population(config: dict):
    """Generate population estimates by vintage year."""
    raw_dir = os.path.join(config["data_paths"]["raw"], "census_population")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    cbsa_df = load_cbsa_top50()

    for vintage in range(2010, 2025):
        records = []
        for _, row in cbsa_df.iterrows():
            code = row["cbsa_code"]
            pop_2023 = METRO_POP_2023.get(code, 1000) * 1000
            years_back = 2023 - vintage
            growth = _pop_growth_rate(code, vintage)

            pop = int(pop_2023 * (1 - growth) ** years_back)

            # Migration components
            dom_mig = int(pop * np.random.normal(0.003 if row["sun_belt"] == 1 else -0.001, 0.002))
            intl_mig = int(pop * abs(np.random.normal(0.003, 0.001)))
            nat_inc = int(pop * np.random.normal(0.004, 0.001))

            records.append({
                "cbsa_name": row["cbsa_name"],
                "population": pop,
                "domestic_migration_net": dom_mig,
                "international_migration_net": intl_mig,
                "natural_increase": nat_inc,
                "cbsa_code": code,
                "year": vintage,        # estimate year
                "pep_vintage": vintage, # release vintage
                "metropolitan statistical area/micropolitan statistical area": code,
            })

        df = pd.DataFrame(records)
        out = os.path.join(raw_dir, f"census_population_metro_{vintage}.csv")
        df.to_csv(out, index=False)

    logger.info(f"Census population: 15 vintage files written to {raw_dir}")


def seed_census_acs(config: dict):
    """Generate ACS housing variables."""
    raw_dir = os.path.join(config["data_paths"]["raw"], "census_acs")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    cbsa_df = load_cbsa_top50()

    for year in range(2005, 2025):
        if year == 2020:
            continue  # ACS 2020 was not released

        records = []
        for _, row in cbsa_df.iterrows():
            code = row["cbsa_code"]
            pop = METRO_POP_2023.get(code, 1000) * 1000
            years_back = 2023 - year

            # Housing units ~ population / avg hh size + vacancy
            avg_hh_size = 2.5 + np.random.normal(0, 0.15)
            total_units = int(pop / avg_hh_size * 1.07)  # ~7% vacancy headroom
            vacancy_rate = 0.06 + np.random.normal(0, 0.02)
            if row["sun_belt"] == 1 and year >= 2021:
                vacancy_rate -= 0.02  # Tighter Sun Belt
            vacancy_rate = max(0.02, min(0.15, vacancy_rate))

            occupied = int(total_units * (1 - vacancy_rate))
            vacant = total_units - occupied
            owner_share = 0.63 + np.random.normal(0, 0.05)
            owner_share = max(0.40, min(0.80, owner_share))
            owner_occ = int(occupied * owner_share)
            renter_occ = occupied - owner_occ

            income = METRO_INCOME.get(code, 60000)
            # Income grows ~3% per year
            income_yr = int(income * (1 - 0.03) ** years_back)

            home_val = METRO_HOME_VALUE.get(code, 300000)
            # Home values follow cycle
            if year <= 2006:
                val_factor = 0.6 + 0.07 * (year - 2000)
            elif year <= 2011:
                val_factor = 1.0 - 0.08 * (year - 2006)
            elif year <= 2019:
                val_factor = 0.60 + 0.05 * (year - 2011)
            else:
                val_factor = 1.0 + 0.08 * (year - 2019)
            home_val_yr = int(home_val * val_factor * (1 + np.random.normal(0, 0.03)))

            median_rent = int(income_yr * 0.015 + np.random.normal(0, 50))
            median_rent = max(600, median_rent)

            records.append({
                "cbsa_name": row["cbsa_name"],
                "total_housing_units": total_units,
                "occupied_units": occupied,
                "vacant_units": vacant,
                "owner_occupied": owner_occ,
                "renter_occupied": renter_occ,
                "avg_household_size": round(avg_hh_size, 2),
                "median_hh_income": income_yr,
                "median_home_value": home_val_yr,
                "median_gross_rent": median_rent,
                "cbsa_code": code,
                "year": year,
                "acs_type": "1yr",
                "metropolitan statistical area/micropolitan statistical area": code,
            })

        df = pd.DataFrame(records)
        out = os.path.join(raw_dir, f"census_acs_metro_{year}.csv")
        df.to_csv(out, index=False)

    logger.info(f"Census ACS: 19 annual files written to {raw_dir}")


def seed_hud_vacancy(config: dict):
    """Generate HUD USPS vacancy data at zip level (aggregated to sample zips)."""
    raw_dir = os.path.join(config["data_paths"]["raw"], "hud_vacancy")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    cbsa_df = load_cbsa_top50()

    for year in range(2015, 2025):
        for quarter in range(1, 5):
            vac_records = []
            xw_records = []

            for _, row in cbsa_df.iterrows():
                code = row["cbsa_code"]
                pop = METRO_POP_2023.get(code, 1000) * 1000

                # Generate 5-10 representative zips per metro
                n_zips = random.randint(5, 10)
                for z in range(n_zips):
                    zip_code = f"{int(code[:3]):03d}{z:02d}"

                    res_addresses = int(pop / n_zips / 2.5 * (1 + np.random.normal(0, 0.1)))
                    vac_rate = 0.04 + np.random.normal(0, 0.02)
                    if row["sun_belt"] == 1 and year >= 2021:
                        vac_rate -= 0.01
                    vac_rate = max(0.01, min(0.15, vac_rate))

                    vacant = int(res_addresses * vac_rate)
                    no_stat = int(res_addresses * np.random.uniform(0.005, 0.02))

                    vac_records.append({
                        "zip": zip_code,
                        "state": row["states"].split("-")[0],
                        "res_vadr": res_addresses,
                        "bus_vadr": int(res_addresses * 0.15),
                        "tot_vadr": int(res_addresses * 1.15),
                        "res_vacant": vacant,
                        "bus_vacant": int(vacant * 0.1),
                        "tot_vacant": int(vacant * 1.1),
                        "no_stat": no_stat,
                    })

                    xw_records.append({
                        "zip": zip_code,
                        "cbsa": code,
                        "res_ratio": round(1.0 / n_zips * n_zips, 4),  # Simplified: 1.0
                        "bus_ratio": round(1.0, 4),
                        "oth_ratio": round(1.0, 4),
                        "tot_ratio": round(1.0, 4),
                    })

            vac_df = pd.DataFrame(vac_records)
            xw_df = pd.DataFrame(xw_records)

            vac_df.to_csv(os.path.join(raw_dir, f"hud_vacancy_zip_{year}Q{quarter}.csv"), index=False)
            xw_df.to_csv(os.path.join(raw_dir, f"hud_cbsa_crosswalk_{year}Q{quarter}.csv"), index=False)

    logger.info(f"HUD vacancy: 40 quarterly files + 40 crosswalks written to {raw_dir}")


def seed_hud_fmr(config: dict):
    """Generate HUD Fair Market Rent files."""
    raw_dir = os.path.join(config["data_paths"]["raw"], "hud_fmr")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    cbsa_df = load_cbsa_top50()

    for fy in range(2000, 2026):
        records = []
        for _, row in cbsa_df.iterrows():
            code = row["cbsa_code"]
            income = METRO_INCOME.get(code, 60000)

            # FMR 2BR ~ 30% of area median income / 12, at 40th percentile
            base_fmr = income * 0.25 / 12
            years_back = 2023 - fy
            fmr_2br = int(base_fmr * (1 - 0.025) ** years_back * (1 + np.random.normal(0, 0.03)))
            fmr_2br = max(500, fmr_2br)

            # Other bedroom sizes
            fmr_0 = int(fmr_2br * 0.65)
            fmr_1 = int(fmr_2br * 0.80)
            fmr_3 = int(fmr_2br * 1.20)
            fmr_4 = int(fmr_2br * 1.35)

            records.append({
                "fmrdd": code,
                "areaname": row["cbsa_name"],
                "state": row["states"].split("-")[0],
                "metro": 1,
                "fmr0": fmr_0,
                "fmr1": fmr_1,
                "fmr2": fmr_2br,
                "fmr3": fmr_3,
                "fmr4": fmr_4,
                "fiscal_year": fy,
            })

        df = pd.DataFrame(records)
        out = os.path.join(raw_dir, f"hud_fmr_fy{fy}.csv")
        df.to_csv(out, index=False)

    logger.info(f"HUD FMR: 26 annual files written to {raw_dir}")


def seed_fred(config: dict):
    """Generate FRED national series."""
    raw_dir = os.path.join(config["data_paths"]["raw"], "fred")
    Path(raw_dir).mkdir(parents=True, exist_ok=True)

    pull_date = pd.Timestamp.now().strftime("%Y-%m-%d")

    # --- Monthly series ---

    # Housing Starts (HOUST) - thousands of units, SAAR
    dates_monthly = pd.date_range("2000-01-01", "2024-12-01", freq="MS")
    for series_id, base_val, region_mult in [
        ("HOUST", 1500, 1.0),
        ("HOUSTNE", 150, 0.10),
        ("HOUSTMW", 250, 0.17),
        ("HOUSTS", 700, 0.47),
        ("HOUSTW", 400, 0.27),
    ]:
        values = []
        for d in dates_monthly:
            cycle = _housing_cycle_factor(d.year)
            val = base_val * cycle * (1 + np.random.normal(0, 0.05))
            values.append(round(max(val * 0.3, val), 1))

        df = pd.DataFrame({
            "date": dates_monthly.strftime("%Y-%m-%d"),
            "value": values,
            "series_id": series_id,
            "pull_date": pull_date,
        })
        df.to_csv(os.path.join(raw_dir, f"fred_{series_id}.csv"), index=False)

    # Completions (COMPUTSA) - tracks starts with ~18-month lag
    values = []
    for d in dates_monthly:
        lag_year = d.year - 1 if d.month <= 6 else d.year
        cycle = _housing_cycle_factor(lag_year)
        val = 1400 * cycle * (1 + np.random.normal(0, 0.05))
        values.append(round(max(300, val), 1))
    df = pd.DataFrame({
        "date": dates_monthly.strftime("%Y-%m-%d"),
        "value": values,
        "series_id": "COMPUTSA",
        "pull_date": pull_date,
    })
    df.to_csv(os.path.join(raw_dir, "fred_COMPUTSA.csv"), index=False)

    # CPI (CPIAUCSL)
    cpi_base = 170  # Jan 2000 level
    cpi_vals = []
    for i, d in enumerate(dates_monthly):
        cpi = cpi_base * (1.025 ** (i / 12))  # ~2.5% annual inflation
        cpi_vals.append(round(cpi, 3))
    df = pd.DataFrame({
        "date": dates_monthly.strftime("%Y-%m-%d"),
        "value": cpi_vals,
        "series_id": "CPIAUCSL",
        "pull_date": pull_date,
    })
    df.to_csv(os.path.join(raw_dir, "fred_CPIAUCSL.csv"), index=False)

    # --- Weekly series: Mortgage rates ---
    dates_weekly = pd.date_range("2000-01-06", "2024-12-26", freq="W-THU")
    mort_vals = []
    for d in dates_weekly:
        if d.year <= 2003:
            base = 7.0 - 0.5 * (d.year - 2000)
        elif d.year <= 2008:
            base = 5.8 + 0.2 * (d.year - 2003)
        elif d.year <= 2012:
            base = 5.0 - 0.5 * (d.year - 2008)
        elif d.year <= 2019:
            base = 3.5 + 0.15 * (d.year - 2012)
        elif d.year <= 2021:
            base = 3.0 - 0.3 * (d.year - 2019)
        elif d.year <= 2023:
            base = 3.0 + 2.0 * (d.year - 2021)
        else:
            base = 6.8
        mort_vals.append(round(base + np.random.normal(0, 0.15), 2))

    df = pd.DataFrame({
        "date": dates_weekly.strftime("%Y-%m-%d"),
        "value": mort_vals,
        "series_id": "MORTGAGE30US",
        "pull_date": pull_date,
    })
    df.to_csv(os.path.join(raw_dir, "fred_MORTGAGE30US.csv"), index=False)

    # --- Quarterly series ---
    dates_quarterly = pd.date_range("2000-01-01", "2024-10-01", freq="QS")

    # Rental vacancy rate
    rvac_vals = []
    for d in dates_quarterly:
        if d.year <= 2009:
            base = 8.0 + 0.3 * (d.year - 2000)
        elif d.year <= 2019:
            base = 10.0 - 0.4 * (d.year - 2009)
        else:
            base = 6.0 - 0.2 * (d.year - 2019)
        rvac_vals.append(round(max(4.0, base + np.random.normal(0, 0.3)), 1))

    df = pd.DataFrame({
        "date": dates_quarterly.strftime("%Y-%m-%d"),
        "value": rvac_vals,
        "series_id": "RVACRATE",
        "pull_date": pull_date,
    })
    df.to_csv(os.path.join(raw_dir, "fred_RVACRATE.csv"), index=False)

    # Homeowner vacancy rate
    hvac_vals = []
    for d in dates_quarterly:
        if d.year <= 2008:
            base = 1.5 + 0.15 * (d.year - 2000)
        elif d.year <= 2010:
            base = 2.8
        elif d.year <= 2019:
            base = 2.8 - 0.15 * (d.year - 2010)
        else:
            base = 1.0
        hvac_vals.append(round(max(0.5, base + np.random.normal(0, 0.1)), 1))

    df = pd.DataFrame({
        "date": dates_quarterly.strftime("%Y-%m-%d"),
        "value": hvac_vals,
        "series_id": "HCOVACRATE",
        "pull_date": pull_date,
    })
    df.to_csv(os.path.join(raw_dir, "fred_HCOVACRATE.csv"), index=False)

    # Median sales price
    msp_vals = []
    for d in dates_quarterly:
        if d.year <= 2006:
            base = 170000 + 15000 * (d.year - 2000)
        elif d.year <= 2011:
            base = 260000 - 15000 * (d.year - 2006)
        elif d.year <= 2019:
            base = 185000 + 12000 * (d.year - 2011)
        else:
            base = 280000 + 30000 * (d.year - 2019)
        msp_vals.append(int(base * (1 + np.random.normal(0, 0.02))))

    df = pd.DataFrame({
        "date": dates_quarterly.strftime("%Y-%m-%d"),
        "value": msp_vals,
        "series_id": "MSPUS",
        "pull_date": pull_date,
    })
    df.to_csv(os.path.join(raw_dir, "fred_MSPUS.csv"), index=False)

    # --- Annual series: Total Households ---
    dates_annual = pd.date_range("2000-01-01", "2024-01-01", freq="YS")
    hh_vals = []
    base_hh = 105000  # thousands
    for d in dates_annual:
        hh = base_hh + 1100 * (d.year - 2000)
        if d.year >= 2020:
            hh += 400 * (d.year - 2019)
        hh_vals.append(int(hh + np.random.normal(0, 200)))

    df = pd.DataFrame({
        "date": dates_annual.strftime("%Y-%m-%d"),
        "value": hh_vals,
        "series_id": "TTLHHLD",
        "pull_date": pull_date,
    })
    df.to_csv(os.path.join(raw_dir, "fred_TTLHHLD.csv"), index=False)

    logger.info(f"FRED: 12 series files written to {raw_dir}")


def main():
    config = load_pipeline_config()

    logger.info("=== Seeding raw data for pipeline validation ===")
    logger.info("")

    seed_census_permits(config)
    seed_census_population(config)
    seed_census_acs(config)
    seed_hud_vacancy(config)
    seed_hud_fmr(config)
    seed_fred(config)

    # Count output
    raw_dir = config["data_paths"]["raw"]
    total_files = sum(len(files) for _, _, files in os.walk(raw_dir))
    logger.info("")
    logger.info(f"=== Done: {total_files} raw CSV files written to {raw_dir}/ ===")


if __name__ == "__main__":
    main()
