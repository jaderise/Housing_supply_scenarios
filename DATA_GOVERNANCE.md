# DATA_GOVERNANCE.md — Data Governance and Quality Framework

---

## Principles

**1. Fail loudly.** The pipeline must never silently produce incorrect metrics. A missing or bad data point that causes a wrong vacancy rate is worse than a pipeline that fails and requires attention.

**2. Immutable raw layer.** Raw CSVs are write-once. All cleaning happens downstream. If you need to re-examine what came from the source, you can always look at the raw file.

**3. Every flag is queryable.** Data quality issues are stored in the `dq_log` table and as `dq_flag` codes on individual rows — not buried in log files. The frontend should surface DQ warnings to the user.

**4. Document assumptions, not just data.** Where a methodological choice (demolition rate, formation rate baseline) shapes the output, it must be documented in METRICS.md and exposed as a parameter, not hidden in code.

**5. Provenance for every metric.** Every metric can be traced back to: which source file, which transformation, which version of the pipeline produced it.

---

## DQ Flag Code Registry

All DQ flags are stored as short codes. The same codes appear in:
- `dq_flag` columns in processed CSVs and SQLite tables
- `rule_code` column in the `dq_log` table

| Code | Stage | Severity | Description | Action |
|------|-------|----------|-------------|--------|
| `NULL_VALUE` | Transform | WARNING | Expected field is null | Flag row; impute if rule exists; else exclude from metrics |
| `OUT_OF_RANGE` | Transform | WARNING | Value outside plausible bounds | Flag row; exclude from metrics; log bounds violated |
| `IMPLAUSIBLE_CHANGE` | Transform | WARNING | YoY change exceeds threshold | Flag row; use prior year value for metric calculation |
| `MISSING_PERIOD` | Transform | ERROR | CBSA × year combination missing entirely | Flag; attempt interpolation if gap = 1 year; else exclude |
| `MISSING_CBSA` | Transform | ERROR | Top-50 CBSA absent from source data | Log; exclude CBSA from that year's metric |
| `CROSSWALK_PARTIAL` | Transform | WARNING | CBSA matched via crosswalk with < 80% address coverage | Flag; use partial match; note reduced confidence |
| `SERIES_DISCONTINUED` | Fetch | WARNING | FRED series returned no data for last 6 months | Flag; use last available value; notify analyst |
| `VINTAGE_MISMATCH` | Transform | WARNING | PEP estimate uses different vintage than adjacent year | Flag; note discontinuity in series |
| `BENCHMARK_DEVIATION` | Calculate | WARNING | National deficit outside expected range vs. published estimates | Log; do not halt; requires analyst review |
| `IMPUTED_VALUE` | Transform | INFO | Missing value filled via interpolation | Note imputation method used |
| `FETCH_FAILURE` | Fetch | ERROR | Source returned error code or 0 rows | Log; halt this source; continue others |
| `FETCH_FAILURE_CRITICAL` | Fetch | CRITICAL | Critical source (permits/population) failed | Halt entire pipeline |

---

## Validation Rules by Source

### Census Building Permits

| Rule | Check | Threshold | Severity |
|------|-------|-----------|----------|
| Completeness | All 50 CBSAs present per year | < 50 CBSAs | ERROR |
| Non-negative | All permit values ≥ 0 | Any negative | ERROR |
| Plausible range | permits_total per CBSA per year | > 200,000 or = 0 for CBSAs with prior data | WARNING |
| YoY stability | YoY change in permits_total | > 200% increase or > 75% decrease | WARNING |
| National total | Sum of metro permits vs. national total | > 20% deviation | WARNING |

### Census Population

| Rule | Check | Threshold | Severity |
|------|-------|-----------|----------|
| Completeness | All 50 CBSAs present | < 50 | ERROR |
| Positive | population > 0 | Any ≤ 0 | ERROR |
| Plausible change | YoY population change | > 10% or < -5% without explanation from migration data | WARNING |
| Components sum | births - deaths + domestic_mig + intl_mig ≈ pop_change | > 5% deviation | WARNING |

### ACS

| Rule | Check | Threshold | Severity |
|------|-------|-----------|----------|
| Internal consistency | occupied + vacant ≈ total_units | > 2% deviation | WARNING |
| Income plausibility | median_hh_income > 0 | Any ≤ 0 | ERROR |
| Household size | avg_household_size between 1.5 and 5.0 | Outside range | WARNING |
| Value plausibility | median_home_value > 0 and < 5,000,000 | Outside range | WARNING |
| Year coverage | Data available for target years | Gap > 2 years | ERROR |

### HUD USPS Vacancy

| Rule | Check | Threshold | Severity |
|------|-------|-----------|----------|
| Address coverage | National address count | < 100M or > 175M | WARNING |
| Vacancy rate bounds | National vacancy rate | < 2% or > 20% | ERROR |
| Metro coverage | Each top-50 CBSA has zip coverage | Coverage < 70% of addresses | WARNING |
| Crosswalk completeness | Zips matched in crosswalk | > 20% unmatched | ERROR |
| QoQ stability | Vacancy rate change per metro | > 5 percentage points in one quarter | WARNING |

### FRED Series

| Rule | Check | Threshold | Severity |
|------|-------|-----------|----------|
| Series length | Observations from 2000 | < 50 observations | ERROR |
| Null rate | Share of null observations | > 10% | WARNING |
| Recency | Last observation | > 6 months ago | WARNING |
| Plausible range | MORTGAGE30US between 2% and 20% | Outside range | WARNING |
| Plausible range | HOUST (national starts) between 300K and 2.5M annualized | Outside range | WARNING |

---

## Imputation Rules

When a value is missing and imputation is applied, the `dq_flag` is set to `IMPUTED_VALUE` and the imputation method is logged.

| Scenario | Imputation Method | Conditions |
|----------|-----------------|------------|
| Single-year gap in permits | Linear interpolation between adjacent years | Gap = 1 year; both neighbors present |
| Single-year gap in population | Linear interpolation | Gap = 1 year; both neighbors present |
| ACS 2020 missing year | Average of 2019 and 2021 | ACS not released for 2020 due to COVID |
| FRED weekly to monthly | Mean of weekly observations in month | Standard aggregation |
| FRED quarterly to annual | Mean of four quarters | Standard aggregation |
| Missing ACS for small CBSA | Use 5-year ACS estimate | Only for CBSAs outside top 50; all top-50 should have 1-year |

**No imputation is applied to:**
- Permit counts missing for > 1 consecutive year
- Population data missing for > 1 year
- Vacancy data missing for entire quarters at CBSA level

---

## Pipeline Run Audit Log

Every pipeline run writes a structured log to `pipeline_{YYYYMMDD_HHMMSS}.log` with the following sections:

```
[RUN START]
  timestamp: 
  pipeline_version:
  config_hash: (MD5 of pipeline_config.yaml to detect config changes)

[STAGE 1: FETCH]
  source: census_permits
    status: SUCCESS | PARTIAL | FAILED
    files_written: N
    rows_fetched: N
    api_calls_made: N
    elapsed_seconds: N
  ... (one block per source)

[STAGE 2: TRANSFORM]
  source: permits
    rows_in: N
    rows_out: N
    rows_flagged: N (with breakdown by flag code)
    rows_excluded: N
  ... (one block per source)

[STAGE 3: LOAD]
  tables_loaded: N
  total_rows: N
  (per-table row counts)

[STAGE 4: CALCULATE]
  metrics_rows_written: N
  scenario_grid_rows_written: N
  benchmark_check: PASSED | WARNING (with details)

[RUN END]
  status: SUCCESS | WARNING | ERROR | CRITICAL
  elapsed_total_seconds: N
  dq_issues_total: N
  dq_issues_by_severity: {WARNING: N, ERROR: N, CRITICAL: N}
```

---

## DQ Dashboard (Frontend Integration)

The `dq_log` table is exposed via the backend API so the frontend can surface data quality context to the user.

**API endpoint:** `GET /api/dq/summary` returns:
- Total warnings and errors in current pipeline run
- List of CBSAs with ERROR-level flags (excluded from any metric)
- List of metrics with BENCHMARK_DEVIATION flag
- Date of last successful pipeline run

**Frontend behavior:**
- An info icon appears on any metro with DQ flags
- Clicking it shows: which metrics are flagged, which flag code, what action was taken
- A global banner shows if any CRITICAL issue exists in the current data

---

## Data Lineage

The following lineage map documents how each SQLite table is produced:

```
Raw CSV                    → Transform Script         → Processed CSV         → SQLite Table

census_permits_metro_*     → transform_permits.py     → permits_metro_annual  → permits
census_population_metro_*  → transform_population.py  → population_metro_*    → population
census_acs_metro_*         → transform_acs.py         → acs_metro_annual      → housing_stock
hud_vacancy_zip_*          → transform_vacancy.py     → vacancy_metro_*       → vacancy
hud_fmr_fy*               → transform_fmr.py         → fmr_metro_annual      → fair_market_rents
fred_*.csv                 → transform_fred.py        → fred_national_series  → fred_series

permits + population                                                           → metrics_metro_annual
housing_stock + fair_market_rents + fred_series                               → metrics_metro_annual
metrics_metro_annual                                                           → scenario_grid
metrics_national_annual                                                        → scenario_grid_national
```

---

## Known Limitations and Accepted Risks

These are documented limitations that are understood and accepted, not unresolved bugs.

| Limitation | Impact | Mitigation |
|-----------|--------|-----------|
| BPS permits ≠ completions | Metro-level metric uses permits as a completions proxy | Document lag assumption; use national completions data as calibration |
| ACS 2020 gap | One-year gap in all ACS-derived metro metrics | Interpolate and flag; document clearly in UI |
| HUD HMFA ≠ CBSA | Some FMR values crosswalked imperfectly | Flag affected CBSAs; note crosswalk method used |
| PEP vintage revision | Historical population estimates revised annually | Use latest vintage; log vintage used |
| No-stat ambiguity | HUD vacancy definition excludes no-stat by default | Config parameter allows inclusion; document choice |
| 20% down payment assumption | Overstates affordability for first-time buyers | Note limitation; add toggle in future version |
| National completions applied to metros | Completions-to-starts ratio is national, applied as context | Label clearly as national metric; do not present as metro-specific |
