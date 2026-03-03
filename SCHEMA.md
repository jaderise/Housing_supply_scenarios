# SCHEMA.md — Data Model

---

## Layer Architecture

```
Layer 0: Raw CSVs          — Immutable source files, never modified
Layer 1: Processed CSVs    — Cleaned, normalized, CBSA-joined intermediate files
Layer 2: SQLite            — Analytical store: base tables + metrics + scenario grid
```

All layers are append-friendly. Re-running the pipeline overwrites processed CSVs and rebuilds SQLite tables from scratch (drop and recreate). Raw CSVs are never touched after landing.

---

## Layer 0: Raw CSV Inventory

These files are written by fetch scripts and never modified. Full specification in `DATA_SOURCES.md`.

```
/data/raw/
  census_permits/
    census_permits_metro_annual_{YYYY}.csv        # One file per year, 2000–present
    census_permits_national_monthly_{YYYY}.csv    # One file per year
    bps_geocodes.csv                              # BPS-to-CBSA crosswalk
  census_population/
    census_population_metro_{YYYY}.csv            # One file per vintage year
  census_acs/
    census_acs_metro_{YYYY}.csv                   # One file per year
  hud_vacancy/
    hud_vacancy_zip_{YYYY}Q{Q}.csv               # One file per quarter
    hud_cbsa_crosswalk_{YYYY}Q{Q}.csv            # Matching crosswalk per quarter
  hud_fmr/
    hud_fmr_fy{YYYY}.csv                         # One file per fiscal year
  fred/
    fred_{series_id}.csv                          # One file per series
  reference/
    cbsa_top50.csv                                # Static reference, update annually
```

---

## Layer 1: Processed CSV Schema

Intermediate files after cleaning and CBSA normalization. Written to `/data/processed/`.

### permits_metro_annual.csv
One row per CBSA per year.

| Column | Type | Description |
|--------|------|-------------|
| `cbsa_code` | string(5) | FIPS CBSA code, zero-padded |
| `cbsa_name` | string | Metro name |
| `year` | int | Calendar year |
| `permits_total` | int | Total units authorized |
| `permits_sf` | int | Single-family units authorized |
| `permits_mf_small` | int | 2–4 unit structures authorized |
| `permits_mf_large` | int | 5+ unit structures authorized |
| `data_source` | string | Source file reference |
| `dq_flag` | string | Null = clean; code = quality issue (see DATA_GOVERNANCE.md) |

### population_metro_annual.csv
One row per CBSA per year.

| Column | Type | Description |
|--------|------|-------------|
| `cbsa_code` | string(5) | |
| `cbsa_name` | string | |
| `year` | int | |
| `population` | int | Total population estimate |
| `domestic_migration_net` | int | Net domestic in-migration |
| `international_migration_net` | int | Net international migration |
| `natural_increase` | int | Births minus deaths |
| `pep_vintage` | int | Vintage year of PEP estimate used |
| `dq_flag` | string | |

### acs_metro_annual.csv
One row per CBSA per year.

| Column | Type | Description |
|--------|------|-------------|
| `cbsa_code` | string(5) | |
| `cbsa_name` | string | |
| `year` | int | ACS survey year |
| `total_housing_units` | int | |
| `occupied_units` | int | |
| `vacant_units` | int | |
| `owner_occupied` | int | |
| `renter_occupied` | int | |
| `avg_household_size` | float | |
| `median_hh_income` | int | Nominal dollars |
| `median_home_value` | int | Nominal dollars |
| `median_gross_rent` | int | Nominal dollars |
| `acs_type` | string | '1yr' or '5yr' |
| `dq_flag` | string | |

### vacancy_metro_quarterly.csv
One row per CBSA per quarter.

| Column | Type | Description |
|--------|------|-------------|
| `cbsa_code` | string(5) | |
| `cbsa_name` | string | |
| `year` | int | |
| `quarter` | int | 1–4 |
| `total_residential_addresses` | int | Total deliverable addresses |
| `vacant_addresses` | int | Confirmed vacant |
| `no_stat_addresses` | int | No-stat (mail not delivered) |
| `vacancy_rate` | float | vacant / total, excluding no-stat |
| `vacancy_rate_incl_nostat` | float | (vacant + no_stat) / total |
| `zip_count` | int | Number of zips contributing to CBSA |
| `dq_flag` | string | |

### fmr_metro_annual.csv
One row per CBSA per fiscal year.

| Column | Type | Description |
|--------|------|-------------|
| `cbsa_code` | string(5) | |
| `cbsa_name` | string | |
| `fiscal_year` | int | HUD fiscal year |
| `fmr_2br` | int | 2-bedroom Fair Market Rent, nominal $ |
| `dq_flag` | string | |

### fred_national_series.csv
One row per series per date.

| Column | Type | Description |
|--------|------|-------------|
| `series_id` | string | FRED series identifier |
| `date` | date | Observation date |
| `value` | float | Observed value (null if FRED returns ".") |
| `frequency` | string | 'monthly', 'quarterly', 'annual', 'weekly' |
| `pull_date` | date | Date this observation was fetched |

---

## Layer 2: SQLite Schema (`housing.db`)

### Table: `cbsa_reference`
Master metro list. Join key for all other tables.

```sql
CREATE TABLE cbsa_reference (
    cbsa_code       TEXT PRIMARY KEY,
    cbsa_name       TEXT NOT NULL,
    principal_city  TEXT,
    states          TEXT,
    population_rank INTEGER,
    region          TEXT,   -- 'Northeast', 'Midwest', 'South', 'West'
    sun_belt        INTEGER -- 1 if Sun Belt market, 0 otherwise (see METRICS.md for definition)
);
```

### Table: `permits`
```sql
CREATE TABLE permits (
    cbsa_code       TEXT REFERENCES cbsa_reference(cbsa_code),
    year            INTEGER,
    permits_total   INTEGER,
    permits_sf      INTEGER,
    permits_mf_small INTEGER,
    permits_mf_large INTEGER,
    dq_flag         TEXT,
    PRIMARY KEY (cbsa_code, year)
);
```

### Table: `population`
```sql
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
);
```

### Table: `housing_stock`
From ACS.
```sql
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
);
```

### Table: `vacancy`
```sql
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
);
```

### Table: `fair_market_rents`
```sql
CREATE TABLE fair_market_rents (
    cbsa_code   TEXT REFERENCES cbsa_reference(cbsa_code),
    fiscal_year INTEGER,
    fmr_2br     INTEGER,
    dq_flag     TEXT,
    PRIMARY KEY (cbsa_code, fiscal_year)
);
```

### Table: `fred_series`
```sql
CREATE TABLE fred_series (
    series_id   TEXT,
    date        TEXT,   -- ISO 8601
    value       REAL,
    pull_date   TEXT,
    PRIMARY KEY (series_id, date)
);
```

---

## Metrics Tables (Precalculated)

### Table: `metrics_metro_annual`
One row per CBSA per year. All calculated metrics from METRICS.md.

```sql
CREATE TABLE metrics_metro_annual (
    cbsa_code                       TEXT REFERENCES cbsa_reference(cbsa_code),
    year                            INTEGER,
    -- Supply metrics
    permits_per_1000_residents      REAL,
    permits_vs_national_avg_ratio   REAL,
    completions_to_starts_ratio     REAL,   -- national proxy applied at metro
    vacancy_rate_annual_avg         REAL,   -- averaged from quarterly
    vacancy_rate_yoy_change         REAL,
    -- Demand metrics
    implied_new_households          INTEGER,
    permits_vs_households_gap       INTEGER,
    pop_growth_per_new_unit         REAL,
    domestic_migration_per_permit   REAL,
    -- Affordability
    mortgage_pct_median_income      REAL,
    fmr_pct_median_income           REAL,
    -- Shortage metric
    annual_implied_need             INTEGER,
    annual_surplus_deficit          INTEGER,    -- positive = surplus, negative = deficit
    cumulative_deficit_since_2008   INTEGER,    -- running total from 2008, baseline scenario
    dq_flag                         TEXT,
    PRIMARY KEY (cbsa_code, year)
);
```

### Table: `metrics_national_annual`
One row per year. National aggregates and FRED-derived series.

```sql
CREATE TABLE metrics_national_annual (
    year                            INTEGER PRIMARY KEY,
    total_households                INTEGER,
    total_completions               INTEGER,
    total_starts                    INTEGER,
    total_permits                   INTEGER,
    hh_formation_rate               REAL,       -- YoY change in households
    completions_vs_formation_gap    INTEGER,
    cumulative_deficit_since_2008   INTEGER,    -- baseline scenario
    mortgage_rate_annual_avg        REAL,
    rental_vacancy_rate             REAL,
    homeowner_vacancy_rate          REAL
);
```

### Table: `scenario_grid`
Precalculated scenario outputs. One row per CBSA per scenario combination per time horizon.

```sql
CREATE TABLE scenario_grid (
    cbsa_code               TEXT REFERENCES cbsa_reference(cbsa_code),
    hh_formation_assumption TEXT,   -- 'low', 'baseline', 'high'
    demolition_assumption   TEXT,   -- 'low', 'baseline', 'high'
    migration_assumption    TEXT,   -- 'reverting', 'flat', 'continuing'
    horizon_years           INTEGER,    -- 1, 2, or 3
    -- Outputs
    projected_new_households    INTEGER,
    projected_completions       INTEGER,    -- based on current pipeline
    projected_surplus_deficit   INTEGER,    -- positive = surplus, negative = deficit
    current_deficit_baseline    INTEGER,    -- starting point (baseline scenario, latest year)
    end_state_deficit           INTEGER,    -- cumulative at end of horizon
    scenario_label              TEXT,       -- human-readable description
    PRIMARY KEY (cbsa_code, hh_formation_assumption, demolition_assumption, migration_assumption, horizon_years)
);
```

### Table: `scenario_grid_national`
Same structure as `scenario_grid` but for national totals.

```sql
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
);
```

---

## Data Quality Log Table

```sql
CREATE TABLE dq_log (
    log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp   TEXT,
    stage           TEXT,   -- 'fetch', 'transform', 'metrics', 'scenario'
    source          TEXT,   -- e.g., 'census_permits', 'fred'
    cbsa_code       TEXT,
    year            INTEGER,
    quarter         INTEGER,
    rule_code       TEXT,   -- e.g., 'NULL_VALUE', 'OUT_OF_RANGE', 'MISSING_PERIOD'
    severity        TEXT,   -- 'WARNING', 'ERROR', 'CRITICAL'
    message         TEXT,
    action_taken    TEXT    -- 'flagged', 'imputed', 'excluded', 'pipeline_halted'
);
```

---

## Key Design Decisions

**CBSA code as universal join key.** Every table uses the 5-digit CBSA code. Sources that use different identifiers (BPS metro codes, HUD HMFAs, zip codes) must be crosswalked to CBSA before entering Layer 1.

**Nominal dollars throughout.** All dollar values stored in nominal terms. Inflation adjustment is applied at the metrics layer, documented in METRICS.md, not in the schema.

**Precalculation over runtime joins.** `metrics_metro_annual` and `scenario_grid` are fully denormalized for frontend query speed. Do not join multiple tables at query time in the API layer.

**Scenario grid uses string keys, not integers.** The three scenario parameters are stored as descriptive strings ('low', 'baseline', 'high', etc.) so the frontend can display them directly without a lookup table.
