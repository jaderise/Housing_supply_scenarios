# METRICS.md — Metric and Scenario Definitions

---

## Design Philosophy

Every metric has an explicit formula, an explicit data dependency, and — where a methodological choice exists — an explicit default and the range of alternatives. The goal is that the analyst can defend every number and explain what assumption drives it.

Where the bull/bear debate lives in the methodology (most importantly: what counts as "normal" household formation), we expose that as a scenario parameter rather than burying it in a hardcoded assumption.

---

## Part 1: Supply-Side Metrics

### 1.1 Permits Per 1,000 Residents
**What it measures:** Construction activity normalized by population, enabling cross-metro comparison.

**Formula:**
```
permits_per_1000 = (permits_total / population) * 1000
```

**Inputs:** `permits.permits_total`, `population.population` — same CBSA, same year.

**Interpretation:** A higher number means more active construction relative to the population base. The national average in a given year is the benchmark.

**Stored in:** `metrics_metro_annual.permits_per_1000_residents`

---

### 1.2 Permits vs. National Average Ratio
**What it measures:** Whether a metro is building above or below the national pace.

**Formula:**
```
ratio = metro_permits_per_1000 / national_avg_permits_per_1000
```

Where `national_avg_permits_per_1000` = sum of all permits across all CBSAs / sum of all population across all CBSAs (population-weighted national average, not simple average of metro ratios).

**Stored in:** `metrics_metro_annual.permits_vs_national_avg_ratio`

**Threshold for UI color coding:**
- ratio > 1.25 → potential oversupply signal (red)
- ratio 0.75–1.25 → neutral (yellow)
- ratio < 0.75 → undersupply signal (green)

*Note: These thresholds are directional, not determinative. Combine with vacancy rate trend before concluding.*

---

### 1.3 Vacancy Rate (Annual Average)
**What it measures:** Share of residential addresses confirmed vacant, averaged across four quarters.

**Formula:**
```
vacancy_rate_annual_avg = mean(vacancy_rate) for year's four quarters
```

**Source:** HUD USPS vacancy data, aggregated to CBSA from zip level.

**YoY change:**
```
vacancy_rate_yoy_change = vacancy_rate_annual_avg[year] - vacancy_rate_annual_avg[year-1]
```

**Interpretation thresholds (for UI color coding):**
- vacancy_rate > 0.08 AND yoy_change > 0.01 → oversupply signal (red)
- vacancy_rate 0.05–0.08 → neutral
- vacancy_rate < 0.05 → undersupply signal (green)

**Stored in:** `metrics_metro_annual.vacancy_rate_annual_avg`, `vacancy_rate_yoy_change`

---

### 1.4 Completions-to-Starts Ratio
**What it measures:** Pipeline health — are projects getting finished at a normal rate, or is there a growing backlog?

**Formula:**
```
completions_to_starts = national_completions / national_starts  (trailing 12 months)
```

**Note:** This is only calculable at the national level from FRED monthly data. Apply as a national context metric; do not extrapolate to metro level.

**Normal range:** Historically 0.90–1.05. Ratio < 0.85 sustained signals a backlog (construction delays, labor/materials constraints). Ratio > 1.10 signals drawdown of prior pipeline.

**Stored in:** `metrics_national_annual.completions_to_starts_ratio`

---

## Part 2: Demand-Side Metrics

### 2.1 Implied New Households
**What it measures:** The number of new households that formed in a given year, estimated from population and household size data.

**Primary formula (preferred when household count data available from FRED):**
```
implied_new_households = total_households[year] - total_households[year-1]
```

**Fallback formula (when using population + household size):**
```
implied_new_households = population / avg_household_size - population[year-1] / avg_household_size[year-1]
```

**Note:** The FRED series `TTLHHLD` (from CPS) gives national household counts directly. For metro-level estimates, use the ACS occupied units series as a proxy for households.

**Stored in:** `metrics_metro_annual.implied_new_households`, `metrics_national_annual.total_households`

---

### 2.2 Permits vs. Household Formation Gap
**What it measures:** Whether construction is keeping pace with new household demand.

**Formula:**
```
gap = implied_new_households - permits_total
```

A negative gap means more households are forming than permits are being issued → undersupply pressure.
A positive gap means more permits than new households → potential oversupply.

**Important caveat:** Permits ≠ completions. There is typically a 6–18 month lag between permit and completion. For trend analysis, compare household formation to completions where available.

**Stored in:** `metrics_metro_annual.permits_vs_households_gap`

---

### 2.3 Population Growth Per New Housing Unit
**What it measures:** How many new residents each new housing unit must absorb.

**Formula:**
```
pop_growth_per_new_unit = (population[year] - population[year-1]) / permits_total[year]
```

**Interpretation:** A rising ratio means each new unit must absorb more population growth → strengthening demand signal. A falling ratio → weakening demand or oversupply.

**Stored in:** `metrics_metro_annual.pop_growth_per_new_unit`

---

### 2.4 Net Domestic Migration Per Permit
**What it measures:** For Sun Belt markets specifically, how much of the construction boom was demand-driven by actual migration vs. speculative.

**Formula:**
```
domestic_migration_per_permit = domestic_migration_net / permits_total
```

**Interpretation:** For pandemic-era Sun Belt markets (2020–2023), a ratio well above 1.0 would suggest demand was real and permits were chasing migration. A ratio that drops sharply post-2022 suggests the migration wave normalized while the permit pipeline did not.

**Stored in:** `metrics_metro_annual.domestic_migration_per_permit`

---

## Part 3: The Core Shortage Metric

### 3.1 Annual Implied Housing Need
**What it measures:** How many units are needed in a given year to meet new household demand plus replace demolished/obsolete units, net of completions.

**Formula:**
```
annual_implied_need = implied_new_households + demolition_rate_pct * housing_stock - completions
```

Where:
- `implied_new_households`: from section 2.1
- `demolition_rate_pct`: percentage of existing stock demolished or rendered uninhabitable annually (see scenario parameters below)
- `housing_stock`: total housing units from ACS
- `completions`: actual completions from Census/FRED

At the metro level, use permits as a completions proxy with an 18-month lag (apply prior year's permits as this year's completions proxy).

**Stored in:** `metrics_metro_annual.annual_implied_need`

---

### 3.2 Annual Surplus/Deficit
**Formula:**
```
annual_surplus_deficit = completions - annual_implied_need
```

Positive = surplus (more built than needed). Negative = deficit (more needed than built).

**Stored in:** `metrics_metro_annual.annual_surplus_deficit`

---

### 3.3 Cumulative Deficit Since 2008
**What it measures:** The structural shortfall (or excess) that has accumulated since the post-GFC construction collapse.

**Formula:**
```
cumulative_deficit[year] = sum(annual_surplus_deficit) for years 2008 through year
```

**Baseline scenario uses:** household formation rate = baseline, demolition rate = baseline (see scenario parameters).

**Stored in:** `metrics_metro_annual.cumulative_deficit_since_2008`, `metrics_national_annual.cumulative_deficit_since_2008`

---

## Part 4: Affordability Metrics

### 4.1 Mortgage Payment as % of Median Income
**What it measures:** Affordability pressure — what share of median household income is consumed by a median-priced home purchase.

**Formula:**
```
monthly_mortgage = median_home_value * 0.80 * monthly_rate / (1 - (1 + monthly_rate)^-360)
```
Where:
- `median_home_value`: ACS B25077
- Down payment assumed: 20% (so loan = 80% of value)
- `monthly_rate`: MORTGAGE30US / 12 (FRED), annual average for the year
- Loan term: 30 years (360 months)

```
mortgage_pct_median_income = (monthly_mortgage * 12) / median_hh_income
```

**Stored in:** `metrics_metro_annual.mortgage_pct_median_income`

**Note:** The 20% down payment assumption is standard but aggressive. It overstates affordability for first-time buyers. This is a known limitation.

---

### 4.2 Fair Market Rent as % of Median Income
**Formula:**
```
fmr_pct_median_income = (fmr_2br * 12) / median_hh_income
```

**Stored in:** `metrics_metro_annual.fmr_pct_median_income`

**Standard affordability threshold:** 30% of gross income. Values above 30% indicate rent burden.

---

## Part 5: Scenario Parameters and Grid

### The Three Scenario Dimensions

**Dimension 1: Household Formation Rate Assumption**

The single most important methodological choice. Determines how many new households the market needs to absorb.

| Level | Description | Formula Adjustment |
|-------|-------------|-------------------|
| `low` | Formation rate 10% below the 2010–2019 average annual rate | `implied_new_households * 0.90` |
| `baseline` | 2010–2019 average annual rate (pre-pandemic trend) | No adjustment |
| `high` | Formation rate 10% above the 2010–2019 average (pent-up demand release) | `implied_new_households * 1.10` |

**Rationale for 2010–2019 baseline:** This period excludes the pre-GFC housing bubble and the pandemic distortion, making it the cleanest measure of structural demand. The NAHB, Freddie Mac, and Up for Growth all use variants of this approach.

---

**Dimension 2: Demolition/Obsolescence Rate**

Annual attrition of existing housing stock.

| Level | Rate | Source Basis |
|-------|------|-------------|
| `low` | 0.15% of stock per year | Lower end of academic estimates |
| `baseline` | 0.25% of stock per year | Commonly used industry estimate; consistent with Freddie Mac methodology |
| `high` | 0.35% of stock per year | Upper bound; reflects aging Sun Belt stock and climate risk |

**Applied to:** Total housing units from ACS × rate = annual units to replace.

---

**Dimension 3: Migration Trend Continuation**

For Sun Belt markets specifically: does the pandemic migration wave continue, normalize, or reverse?

| Level | Description | Applied To |
|-------|-------------|-----------|
| `reverting` | Net domestic migration returns to 2015–2019 trend rate | `domestic_migration_net * 0.50` |
| `flat` | Migration stays at 2022–2023 actual levels | No adjustment to recent actuals |
| `continuing` | Migration remains elevated at pandemic peak rate | `domestic_migration_net * 1.25` |

**Note:** This dimension primarily affects metro-level scenarios. For national totals, migration is a redistribution, not a net demand driver. Adjust household formation indirectly at metro level; do not apply to national scenario grid.

---

**Dimension 4: Time Horizon**

| Value | Description |
|-------|-------------|
| `1` | 1-year projection from most recent data year |
| `2` | 2-year projection |
| `3` | 3-year projection |

The pipeline uses the current permit authorization backlog (permits issued minus estimated completions) as the completion projection base, then assumes permits continue at the most recent 12-month trailing rate.

---

### Scenario Grid Calculation

For each CBSA × each combination of (hh_formation × demolition × migration × horizon):

```
Step 1: Start from current_deficit_baseline (baseline scenario, latest year)

Step 2: For each horizon year:
    projected_new_households = population_trend * hh_formation_adjustment * migration_adjustment
    projected_completions = permit_backlog + (trailing_permit_rate * years_remaining)
    annual_net = projected_completions - (projected_new_households + demolition_units)
    
Step 3: end_state_deficit = current_deficit_baseline + sum(annual_net over horizon)
```

**Grid size:** 3 × 3 × 3 × 3 = 81 combinations per metro × 50 metros = 4,050 rows in `scenario_grid`. Plus 27 rows for national. Easily manageable in SQLite.

---

## Part 6: Sun Belt Classification

For the Metro Explorer color-coding and investment framing, metros are pre-classified as Sun Belt or not.

**Sun Belt metros (in top 50):** Phoenix, Austin, Dallas-Fort Worth, Houston, San Antonio, Tampa, Orlando, Jacksonville, Charlotte, Raleigh, Nashville, Atlanta, Las Vegas, Riverside-San Bernardino, Sacramento, Denver.

**Stored in:** `cbsa_reference.sun_belt` (1 = Sun Belt, 0 = other)

This classification is used in the Scenario Builder Claude prompt to frame the investment question appropriately (Sun Belt → focus on migration normalization; non-Sun Belt → focus on supply constraints).

---

## Part 7: Validation Benchmarks

Before the product is considered reliable, the following published estimates should be used as sanity checks:

| Published Estimate | Source | Methodology | Expected Range |
|-------------------|--------|-------------|----------------|
| 3.8M unit shortage | Freddie Mac (2021) | National cumulative since 2008 | ±500K acceptable |
| 3.79M unit shortage | Up for Growth (2022) | National, includes rental | ±500K acceptable |
| 1.5M unit shortage | NAR (2021) | Different methodology, narrower scope | Lower bound reference |
| NAHB HMI by metro | NAHB | Builder sentiment, not a count | Directional cross-check only |

If the baseline scenario national cumulative deficit is outside the range of these published estimates, treat as a data quality or methodology issue and investigate before proceeding.
