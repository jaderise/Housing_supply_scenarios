# PIPELINE.md — ETL Specification

---

## Overview

The pipeline runs in four sequential stages. Each stage has a clear input, a clear output, and a clear failure mode. Stages must run in order; later stages fail if earlier stages have not completed successfully.

```
Stage 1: FETCH      Raw APIs/downloads → /data/raw/ CSVs
Stage 2: TRANSFORM  Raw CSVs → /data/processed/ CSVs (clean, normalize, CBSA-join)
Stage 3: LOAD       Processed CSVs → SQLite base tables
Stage 4: CALCULATE  SQLite base tables → metrics tables + scenario grid
```

Each stage is an independent script. A master orchestrator script (`run_pipeline.py`) runs them in sequence with logging and failure handling.

---

## Directory Structure

```
/project/
  pipeline/
    fetch/
      fetch_census_permits.py
      fetch_census_population.py
      fetch_census_acs.py
      fetch_hud_vacancy.py
      fetch_hud_fmr.py
      fetch_fred.py
    transform/
      transform_permits.py
      transform_population.py
      transform_acs.py
      transform_vacancy.py
      transform_fmr.py
      transform_fred.py
    load/
      load_to_sqlite.py
    calculate/
      calculate_metrics.py
      calculate_scenarios.py
    config/
      pipeline_config.yaml
      cbsa_top50.csv          # reference data
      scenario_params.yaml    # scenario grid parameter definitions
    utils/
      logger.py
      dq_checks.py
      cbsa_utils.py
    run_pipeline.py           # master orchestrator
  data/
    raw/
    processed/
    db/
      housing.db
    logs/
      pipeline_{YYYYMMDD_HHMMSS}.log
      dq_report_{YYYYMMDD_HHMMSS}.csv
```

---

## Pipeline Configuration (`pipeline_config.yaml`)

```yaml
data_paths:
  raw: ./data/raw
  processed: ./data/processed
  db: ./data/db/housing.db
  logs: ./data/logs

fetch:
  census_api_key: ${CENSUS_API_KEY}   # environment variable
  fred_api_key: ${FRED_API_KEY}       # environment variable
  start_year: 2000
  end_year: 2024                      # update annually
  request_delay_seconds: 0.5          # sleep between API calls
  max_retries: 3

transform:
  include_nostat_in_vacancy: false    # HUD vacancy: exclude no-stat by default
  population_vintage_preference: latest  # use most recent PEP vintage

calculate:
  baseline_start_year: 2008          # cumulative deficit start year
  demolition_rates:
    low: 0.0015
    baseline: 0.0025
    high: 0.0035
  hh_formation_adjustments:
    low: 0.90
    baseline: 1.00
    high: 1.10
  migration_adjustments:
    reverting: 0.50
    flat: 1.00
    continuing: 1.25
  horizon_years: [1, 2, 3]
  mortgage_down_payment_pct: 0.20

sun_belt_cbsas:   # CBSA codes for Sun Belt classification
  - "38060"  # Phoenix
  - "12420"  # Austin
  - "19100"  # Dallas-Fort Worth
  - "26420"  # Houston
  - "41700"  # San Antonio
  - "45300"  # Tampa
  - "36740"  # Orlando
  - "27260"  # Jacksonville
  - "16740"  # Charlotte
  - "39580"  # Raleigh
  - "34980"  # Nashville
  - "12060"  # Atlanta
  - "29820"  # Las Vegas
  - "40140"  # Riverside-San Bernardino
  - "40900"  # Sacramento
  - "19740"  # Denver
```

---

## Stage 1: FETCH

**Entry condition:** Internet access available, API keys set in environment.
**Exit condition:** Raw CSVs written to `/data/raw/`, fetch manifest written to log.
**Failure behavior:** On any fetch failure, log the error, skip that source, and continue. Report all skipped sources at end of stage. Do NOT proceed to Stage 2 if any critical source failed (Census permits, Census population are critical; HUD FMR is non-critical).

### fetch_census_permits.py

```python
"""
Fetches building permit data from Census BPS text files (historical) 
and Census API (recent). Outputs one CSV per year to /data/raw/census_permits/.
"""

Logic:
1. Download bps_geocodes.csv (BPS-to-CBSA crosswalk) if not present or older than 90 days
2. For years 2000–(current_year - 2): download annual BPS text files
   URL pattern: https://www.census.gov/construction/bps/txt/tb3u{YYYY}.txt
   - Parse fixed-width or pipe-delimited format per bpstruct.txt specification
   - Apply geocodes crosswalk to get CBSA codes
   - Write: census_permits_metro_annual_{YYYY}.csv
3. For years (current_year - 1)–current_year: use Census API for more recent data
   - Pull monthly, aggregate to annual
   - Write: census_permits_metro_annual_{YYYY}.csv
4. Pull national monthly completions and starts from API
   - Write: census_permits_national_monthly_{YYYY}.csv

DQ checks at fetch:
  - Row count per year file must be > 300 (there are ~380 MSAs)
  - No negative permit values
  - CBSA code must be 5 digits

Output columns: cbsa_code, cbsa_name, year, permits_total, permits_sf, permits_mf_small, permits_mf_large
```

### fetch_census_population.py

```python
"""
Fetches Population Estimates Program data via Census API.
One call per vintage year.
"""

Logic:
1. Identify available PEP vintage years (2010–current)
2. For each vintage year, call:
   GET https://api.census.gov/data/{vintage}/pep/population
   ?get=NAME,POP,DOMESTICMIG,INTERNATIONALMIG,NATURALINC
   &for=metropolitan+statistical+area/micropolitan+statistical+area:*
3. Parse JSON response, write: census_population_metro_{vintage_year}.csv
4. Log which vintage year was used for each calendar year

DQ checks at fetch:
  - Population values must be > 0
  - All 50 target CBSAs must be present in the response
```

### fetch_census_acs.py

```python
"""
Fetches ACS 1-year estimates for housing variables.
"""

Logic:
1. For each year 2005–current (skipping 2020):
   Call ACS 1-year API with variables: B25001_001E, B25002_002E, B25002_003E,
   B25003_002E, B25003_003E, B25010_001E, B19013_001E, B25077_001E, B25064_001E
   Geography: metropolitan+statistical+area:*
2. Write: census_acs_metro_{YYYY}.csv

DQ checks at fetch:
  - Non-null values for all 50 target CBSAs
  - Occupied + vacant must approximately equal total (within 2%)
  - Median income must be > 20000 (sanity bound)
```

### fetch_hud_vacancy.py

```python
"""
Downloads HUD USPS vacancy files and zip-to-CBSA crosswalk files.
Manual download paths required; see DATA_SOURCES.md for URL pattern.
"""

Logic:
1. For each quarter from 2015Q1 to current quarter:
   a. Download USPS_ZCTA_CITY_AP_{YYYYQ}.xlsx → parse → write CSV
   b. Download ZIP_CBSA_{YYYYQ}.xlsx (crosswalk) → parse → write CSV
2. Write: hud_vacancy_zip_{YYYY}Q{Q}.csv, hud_cbsa_crosswalk_{YYYY}Q{Q}.csv

DQ checks at fetch:
  - File must contain > 20,000 zip records (national coverage)
  - Vacancy rate at national level must be between 2% and 20% (sanity bound)
```

### fetch_hud_fmr.py

```python
"""
Downloads HUD Fair Market Rent files.
"""

Logic:
1. For fiscal years 2000–current:
   Download FMR CSV from HUD download page
   Parse, extract 2-bedroom FMR by area
   Apply HUD-to-CBSA crosswalk
   Write: hud_fmr_fy{YYYY}.csv

DQ checks at fetch:
  - FMR values must be > 200 and < 10000 (sanity bound)
```

### fetch_fred.py

```python
"""
Fetches all required FRED series via API.
"""

SERIES = [
    'HOUST', 'HOUSTNE', 'HOUSTMW', 'HOUSTS', 'HOUSTW',
    'COMPUTSA', 'RVACRATE', 'HCOVACRATE', 'TTLHHLD', 
    'MORTGAGE30US', 'MSPUS'
]

Logic:
1. For each series:
   GET https://api.fred.stlouisfed.org/series/observations
   ?series_id={id}&api_key={key}&file_type=json&observation_start=2000-01-01
2. Replace "." values with null
3. Write: fred_{series_id}.csv
4. Sleep 0.5s between calls

DQ checks at fetch:
  - Each series must return > 100 observations
  - No series should have more than 10% null values
```

---

## Stage 2: TRANSFORM

**Entry condition:** Stage 1 completed, raw CSVs present.
**Exit condition:** Processed CSVs written to `/data/processed/`, DQ check results logged.
**Failure behavior:** Log DQ failures as errors. Flag affected rows. If error rate > 5% for any source, halt pipeline and require manual review.

### transform_permits.py

```python
"""
Reads all annual permit CSVs, normalizes, filters to top 50 CBSAs.
"""

Steps:
1. Read cbsa_top50.csv as the canonical CBSA list
2. For each annual permit file:
   a. Validate CBSA code format (5-digit string)
   b. Filter to top 50 CBSAs
   c. Fill missing permit values with 0 ONLY if the metro reported in prior year 
      AND the gap is one year (one-year gaps are common in BPS for small-county metros)
      Otherwise: set to null and flag as dq_flag='MISSING_PERMITS'
   d. Cap any single-year YoY change > 200% as suspicious → flag as dq_flag='IMPLAUSIBLE_CHANGE'
3. Concatenate all years
4. Write: permits_metro_annual.csv

DQ rules applied:
  - NULL_VALUE: any null in permits_total after fill logic
  - OUT_OF_RANGE: negative values
  - IMPLAUSIBLE_CHANGE: YoY change > 200%
  - MISSING_CBSA: any top-50 CBSA absent from a year's data
```

### transform_population.py

```python
"""
Selects preferred PEP vintage for each year, normalizes, handles 2020 discontinuity.
"""

Steps:
1. For each calendar year, select the most recent vintage that covers that year
2. Flag the 2020 Census rebase: any series crossing 2020 carries a discontinuity flag
3. Filter to top 50 CBSAs
4. Validate: population must be monotonically increasing OR the YoY change must 
   be explainable by migration data (i.e., large declines flagged for review)
5. Write: population_metro_annual.csv
```

### transform_acs.py

```python
"""
Cleans ACS data, validates internal consistency, inflation-adjusts dollar values.
"""

Steps:
1. Validate: occupied + vacant ≈ total (within 2%); flag if not
2. Validate: median income, home value, rent must be positive and within plausible range
3. Inflation-adjust dollar values to constant 2023 dollars using CPI
   (Pull CPI from FRED series CPIAUCSL — add to fetch_fred.py)
   Store both nominal and real values; schema column names refer to nominal; 
   add _real suffix columns for inflation-adjusted versions
4. Filter to top 50 CBSAs
5. Write: acs_metro_annual.csv
```

### transform_vacancy.py

```python
"""
Aggregates zip-level HUD vacancy to CBSA level using weighted crosswalk.
"""

Steps:
1. For each quarter:
   a. Load zip vacancy file
   b. Load matching zip-to-CBSA crosswalk
   c. For each zip, distribute addresses and vacancies to CBSAs by res_ratio weight
   d. Aggregate to CBSA level: sum weighted addresses, sum weighted vacancies
   e. Calculate vacancy_rate = vacant / total (excluding no-stat per config)
   f. Validate: national vacancy rate implied must be within 2–15%
2. Concatenate all quarters
3. Filter to top 50 CBSAs
4. Write: vacancy_metro_quarterly.csv
```

### transform_fmr.py

```python
"""
Maps HUD FMR data to CBSA codes via HUD-to-CBSA crosswalk.
"""

Steps:
1. Load HUD FMR file
2. Apply HMFA-to-CBSA crosswalk (some HMFAs span multiple CBSAs; use majority-county assignment)
3. Filter to top 50 CBSAs
4. Validate: FMR values must be > $200 and < $10,000
5. Write: fmr_metro_annual.csv
```

### transform_fred.py

```python
"""
Converts FRED series to analytical format, handles frequency normalization.
"""

Steps:
1. Read all fred_{series_id}.csv files
2. For weekly series (MORTGAGE30US): aggregate to monthly average
3. For quarterly series: forward-fill to monthly for join compatibility
4. Validate: no sudden jumps > 50% MoM in any series
5. Write: fred_national_series.csv (all series combined, long format)
```

---

## Stage 3: LOAD

**Entry condition:** All processed CSVs present.
**Exit condition:** SQLite tables populated, row counts validated.

### load_to_sqlite.py

```python
"""
Reads all processed CSVs and loads to SQLite. Drops and recreates all tables on each run.
"""

Steps:
1. Drop existing tables (except dq_log, which is append-only)
2. Create tables per SCHEMA.md DDL
3. Load cbsa_top50.csv → cbsa_reference (with sun_belt flag from config)
4. Load permits_metro_annual.csv → permits
5. Load population_metro_annual.csv → population
6. Load acs_metro_annual.csv → housing_stock
7. Load vacancy_metro_quarterly.csv → vacancy
8. Load fmr_metro_annual.csv → fair_market_rents
9. Load fred_national_series.csv → fred_series
10. Validate row counts: each table must have > 0 rows for every CBSA in cbsa_reference
11. Write load summary to log
```

---

## Stage 4: CALCULATE

**Entry condition:** SQLite base tables loaded.
**Exit condition:** `metrics_metro_annual`, `metrics_national_annual`, `scenario_grid`, `scenario_grid_national` populated.

### calculate_metrics.py

```python
"""
Calculates all metrics per METRICS.md and writes to metrics tables.
"""

Steps (in order — some metrics depend on others):

1. Calculate permits_per_1000_residents for each CBSA × year
2. Calculate national average permits_per_1000 (population-weighted)
3. Calculate permits_vs_national_avg_ratio
4. Calculate vacancy_rate_annual_avg and yoy_change
5. Calculate implied_new_households (from FRED TTLHHLD for national; ACS occupied for metro)
6. Calculate permits_vs_households_gap
7. Calculate pop_growth_per_new_unit
8. Calculate domestic_migration_per_permit
9. Calculate mortgage payment and mortgage_pct_median_income
10. Calculate fmr_pct_median_income
11. Calculate annual_implied_need (baseline scenario: baseline demolition, baseline formation)
12. Calculate annual_surplus_deficit
13. Calculate cumulative_deficit_since_2008 (running sum from 2008)
14. Write to metrics_metro_annual and metrics_national_annual
15. Validate against published benchmarks (log warning if outside expected range)
```

### calculate_scenarios.py

```python
"""
Generates the precalculated scenario grid.
"""

Steps:
1. Read scenario parameter definitions from scenario_params.yaml
2. For each CBSA in cbsa_reference:
   For each combination of (hh_formation × demolition × migration × horizon):
     a. Start from current_deficit_baseline (latest year, baseline scenario from metrics table)
     b. Project population trend (linear extrapolation of last 3 years)
     c. Apply hh_formation_adjustment and migration_adjustment
     d. Apply demolition rate to current housing stock
     e. Project completions: current permit backlog + trailing 12-month permit rate × horizon
     f. Calculate projected_surplus_deficit for each horizon year
     g. Sum to end_state_deficit
     h. Write row to scenario_grid
3. Repeat for national totals → scenario_grid_national
4. Validate: scenario grid must have exactly 81 rows per CBSA (3×3×3×3)
```

---

## Master Orchestrator (`run_pipeline.py`)

```python
"""
Runs all four stages in sequence. Logs to pipeline_{timestamp}.log.
Usage: python run_pipeline.py [--stage {fetch,transform,load,calculate,all}]
"""

Stages:
  --stage fetch        Run Stage 1 only
  --stage transform    Run Stage 2 only (requires Stage 1 complete)
  --stage load         Run Stage 3 only (requires Stage 2 complete)
  --stage calculate    Run Stage 4 only (requires Stage 3 complete)
  --stage all          Run all stages (default)

Exit codes:
  0 = success
  1 = warning (pipeline completed but DQ issues flagged)
  2 = error (pipeline halted; manual review required)
```

---

## Pipeline Run Checklist

Before each pipeline run:
- [ ] CENSUS_API_KEY environment variable set
- [ ] FRED_API_KEY environment variable set
- [ ] Sufficient disk space (full historical run ~500MB)
- [ ] HUD vacancy files for current quarter downloaded manually (if new quarter available)

After each pipeline run:
- [ ] Review pipeline log for ERROR or CRITICAL entries
- [ ] Review DQ report: any new flags not seen in prior run?
- [ ] Validate national cumulative deficit against benchmark range (METRICS.md Part 7)
- [ ] Confirm scenario grid row count: 4,050 metro rows + 27 national rows
