# DATA_SOURCES.md — Data Source Specifications

---

## Overview

All sources are free and require no authentication unless noted. Raw data lands as flat CSVs in `/data/raw/{source}/`. Files are never modified after landing — all transformations happen downstream.

Directory structure:
```
/data/
  raw/
    census_permits/
    census_population/
    census_acs/
    hud_vacancy/
    hud_fmr/
    hud_chas/
    fred/
  processed/
  db/
    housing.db
  logs/
```

---

## Source 1: Census Bureau — Building Permits Survey

**What it provides:** Monthly building permits, housing starts, and completions by geography.

**Base URL:** `https://www.census.gov/construction/bps/`

**API Endpoint (preferred):**
```
https://api.census.gov/data/timeseries/eits/bps
```

**Key Parameters:**
- `get=category_code,cell_value,error_data,time_slot_id,seasonally_adj,geo_level_code,is_adj`
- `for=metropolitan+statistical+area/micropolitan+statistical+area:*` for metro level
- `time=YYYY-MM` format
- No API key required for low-volume pulls; register at api.census.gov for a key to avoid rate limits (free)

**Alternative (direct file download):**
Annual permit data files are available as flat text files:
```
https://www.census.gov/construction/bps/txt/tb3u{YYYY}.txt   # Annual, MSA level, units authorized
```
These are pipe-delimited or fixed-width. The file format specification is at:
```
https://www.census.gov/construction/bps/txt/bpstruct.txt
```

**Recommended approach:** Use the direct annual files for the historical backfill (2000–2022), then use the API for recent monthly data. This avoids rate limit issues for the bulk historical pull.

**Series to collect:**
| Series | Description | Grain |
|--------|-------------|-------|
| Total units authorized | Permits issued | Metro × Month |
| Single-family units authorized | SF permits | Metro × Month |
| 2-4 units authorized | Small MF permits | Metro × Month |
| 5+ units authorized | Large MF permits | Metro × Month |
| Total units started | Starts (national/regional only at monthly) | National × Month |
| Total units completed | Completions (national/regional only at monthly) | National × Month |

**Key quirk:** Permits are available at metro level monthly. Starts and completions are only available at national and regional (4 Census regions) level monthly — not metro. For metro-level starts/completions, use annual data.

**CBSA mapping:** The BPS uses its own metro code list, which maps to CBSA codes but is not identical. The crosswalk file is at:
```
https://www.census.gov/construction/bps/txt/geocodes.txt
```
Download this file and join it to all permit data before storing.

**Raw CSV output:** `census_permits_metro_annual_{YYYY}.csv`, `census_permits_national_monthly_{YYYY}.csv`

**Expected columns (metro annual):**
`cbsa_code, cbsa_name, state_code, year, permits_total, permits_sf, permits_2_4, permits_5plus`

---

## Source 2: Census Bureau — Population Estimates Program (PEP)

**What it provides:** Annual county and metro population estimates with components of change (births, deaths, domestic migration, international migration).

**API Endpoint:**
```
https://api.census.gov/data/2023/pep/population
```
(Adjust year in URL for each vintage year)

**Key Parameters:**
- `get=NAME,POP,DOMESTICMIG,INTERNATIONALMIG,NATURALINC` 
- `for=metropolitan+statistical+area/micropolitan+statistical+area:*`
- Vintage years available: 2010–2023 (separate API endpoints per vintage)

**Recommended approach:** Pull each vintage year separately. The 2020 vintage rebaselined to the new Census; treat pre/post 2020 as a joined series with a noted discontinuity.

**Raw CSV output:** `census_population_metro_{YYYY}.csv`

**Expected columns:**
`cbsa_code, cbsa_name, year, population, domestic_migration, international_migration, natural_increase`

**Key quirk:** PEP revises historical estimates each year. Use the most recent vintage for each year but log which vintage was used. Do not mix vintages within a single time series without flagging.

---

## Source 3: Census Bureau — American Community Survey (ACS)

**What it provides:** Household size, vacancy rates, total housing units, tenure (own vs. rent), by metro. Annual 1-year and 5-year estimates.

**API Endpoint:**
```
https://api.census.gov/data/{YYYY}/acs/acs1
```

**Key Tables and Variables:**
| Variable | Description |
|----------|-------------|
| `B25001_001E` | Total housing units |
| `B25002_002E` | Occupied housing units |
| `B25002_003E` | Vacant housing units |
| `B25003_002E` | Owner-occupied units |
| `B25003_003E` | Renter-occupied units |
| `B25010_001E` | Average household size |
| `B19013_001E` | Median household income |
| `B25077_001E` | Median home value |
| `B25064_001E` | Median gross rent |

**Geography parameter:** `for=metropolitan+statistical+area/micropolitan+statistical+area:*`

**Note:** ACS 1-year is only available for geographies with 65,000+ population. For smaller metros, use 5-year estimates. For the top 50 metros, 1-year is fine.

**Years available:** 2005–present (with gap for 2020, which was not released due to COVID data quality issues).

**Raw CSV output:** `census_acs_metro_{YYYY}.csv`

**Expected columns:**
`cbsa_code, cbsa_name, year, total_units, occupied_units, vacant_units, owner_occupied, renter_occupied, avg_household_size, median_hh_income, median_home_value, median_gross_rent`

---

## Source 4: HUD — USPS Vacancy Data

**What it provides:** Quarterly counts of residential addresses by occupancy status (occupied, vacant, no-stat) at the zip code level, derived from USPS postal carrier data. The most granular vacancy signal available publicly.

**Download URL:**
```
https://www.huduser.gov/portal/datasets/usps_crosswalk.html
```
Navigate to "HUD-USPS ZIP Code Crosswalk Files" — the vacancy data itself is separate:
```
https://www.huduser.gov/portal/datasets/usps.html
```

**Format:** Excel files (.xlsx) by quarter, downloadable from the HUD user portal. No API; must download manually or automate via the direct file URLs (these are stable and follow a naming convention).

**File naming convention:**
```
USPS_ZCTA_CITY_AP_{YYYYQ}.xlsx   # where Q is 1,2,3,4
```

**Key columns in raw file:**
`zip, state, res_vadr, bus_vadr, tot_vadr, res_vacant, bus_vacant, tot_vacant, no_stat`

- `res_vadr` = total residential addresses
- `res_vacant` = vacant residential addresses
- Vacancy rate = `res_vacant / res_vadr`

**Zip-to-CBSA crosswalk:** This is the critical join. HUD publishes a crosswalk:
```
https://www.huduser.gov/portal/datasets/usps_crosswalk.html
```
Download `ZIP_CBSA_{YYYYQ}.xlsx`. Each zip can map to multiple CBSAs (weighted). Use the `res_ratio` column to weight the allocation.

**Recommended approach:**
1. Download quarterly files back to 2015 (earlier data has quality issues)
2. Download the corresponding quarterly crosswalk file
3. Aggregate zip-level vacancies to CBSA level using weighted average
4. Store one row per CBSA per quarter

**Raw CSV output:** `hud_vacancy_zip_{YYYYQQ}.csv`, `hud_cbsa_crosswalk_{YYYYQQ}.csv`

**Processed CSV output:** `hud_vacancy_metro_quarterly.csv`

**Expected columns (processed):**
`cbsa_code, year, quarter, total_residential_addresses, vacant_addresses, vacancy_rate`

**Key quirk:** "No-stat" addresses (properties where mail is not delivered) are not the same as vacant. Treat them separately. Some analysts include no-stat as a vacancy signal; others exclude them. Make this a parameter in the pipeline config.

---

## Source 5: HUD — Fair Market Rents (FMR)

**What it provides:** Annual HUD-published estimates of the 40th percentile gross rent for standard quality units by metro. A standardized proxy for rental market conditions.

**Download URL:**
```
https://www.huduser.gov/portal/datasets/fmr.html
```

**Format:** Excel or CSV files, one per year. Available from FY1983 forward.

**Key columns:**
`fmrdd, areaname, state, metro, fmr0, fmr1, fmr2, fmr3, fmr4`
(fmr0 = efficiency, fmr1 = 1BR, fmr2 = 2BR, fmr3 = 3BR, fmr4 = 4BR)

**Note:** HUD uses its own metro area definitions (HUD Metro FMR Areas, or HMFAs) which do not perfectly align with CBSAs. A crosswalk is available on the same page. Use 2-bedroom FMR as the standard comparison unit.

**Raw CSV output:** `hud_fmr_{FY}.csv`

**Expected columns (after crosswalk):**
`cbsa_code, fiscal_year, fmr_2br`

---

## Source 6: FRED — Federal Reserve Economic Data

**What it provides:** National and regional housing series, mortgage rates, economic indicators.

**API Base URL:**
```
https://api.fred.stlouisfed.org/series/observations
```

**Authentication:** Free API key required. Register at `https://fred.stlouisfed.org/docs/api/api_key.html`. Store key in environment variable `FRED_API_KEY`.

**Series to pull:**

| FRED Series ID | Description | Frequency |
|---------------|-------------|-----------|
| `HOUST` | Housing Starts: Total (national) | Monthly |
| `HOUSTNE` | Housing Starts: Northeast | Monthly |
| `HOUSTMW` | Housing Starts: Midwest | Monthly |
| `HOUSTS` | Housing Starts: South | Monthly |
| `HOUSTW` | Housing Starts: West | Monthly |
| `COMPUTSA` | Housing Completions: Total (national) | Monthly |
| `RVACRATE` | Rental Vacancy Rate | Quarterly |
| `HCOVACRATE` | Homeowner Vacancy Rate | Quarterly |
| `TTLHHLD` | Total Households (from CPS) | Annual |
| `MORTGAGE30US` | 30-Year Fixed Mortgage Rate | Weekly |
| `MSPUS` | Median Sales Price of Houses Sold | Quarterly |

**API call format:**
```
https://api.fred.stlouisfed.org/series/observations?series_id={SERIES_ID}&api_key={KEY}&file_type=json&observation_start=2000-01-01
```

**Raw CSV output:** `fred_{series_id}.csv` (one file per series)

**Expected columns:**
`date, series_id, value`

**Key quirk:** FRED returns "." for missing observations. Treat as null, not zero. Some series revise historical data — log the pull date so you know which vintage you have.

---

## Reference Data: CBSA Master List

**The canonical list of top 50 metros drives all geographic filtering.**

Source: Census Bureau CBSA delineation files:
```
https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html
```

Download the current CBSA delineation file. Filter to Metropolitan Statistical Areas (not Micropolitan). Sort by population and take the top 50.

**Store as:** `reference/cbsa_top50.csv`

**Columns:** `cbsa_code, cbsa_name, principal_city, states, population_rank`

This file is the join key across all sources. Every source must be joined to this list before entering the processed layer. If a source uses a different metro definition, document the crosswalk used.

---

## Rate Limits and Operational Notes

| Source | Rate Limit | Recommended Approach |
|--------|-----------|---------------------|
| Census API | 500 requests/day without key, higher with free key | Register for key; pull in batches |
| FRED API | 120 requests/minute | Add 0.5s sleep between calls |
| HUD downloads | No API; file downloads | Download once, cache locally |
| BPS text files | No limit | Download full historical set once |

**General rule:** Log every fetch with timestamp, URL, response code, and row count received. If a fetch returns 0 rows or an error code, the pipeline must halt that source and log the failure — do not proceed to transformation with missing data.
