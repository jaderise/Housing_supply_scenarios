# APP_SPEC.md — Application Specification

---

## Architecture

```
React Frontend  ←→  FastAPI Backend  ←→  SQLite (housing.db)
                         ↑
                   Claude API (for Panel 3 interpretation)
```

The backend is the only component that reads from SQLite. The frontend makes HTTP requests to the backend and renders responses. No direct SQLite access from the frontend.

---

## Backend: FastAPI Server

**File:** `backend/main.py`
**Port:** 8000 (default)
**Run command:** `uvicorn main:app --host 0.0.0.0 --port 8000`

### Endpoint Inventory

#### Reference Data
```
GET /api/metros
  Returns: list of top-50 CBSAs with name, code, sun_belt flag, region
  Source: cbsa_reference table
  Response: [{cbsa_code, cbsa_name, region, sun_belt, population_rank}]

GET /api/metadata
  Returns: data vintage info (last pipeline run date, latest data year per source)
  Source: dq_log table + SQLite table timestamps
```

#### Panel 1: National Picture
```
GET /api/national/timeseries
  Query params: metric (optional, default: all)
  Returns: annual national data from 2000–present
  Source: metrics_national_annual + fred_series
  Response: {
    years: [2000...2024],
    total_households: [...],
    total_completions: [...],
    total_permits: [...],
    hh_formation_rate: [...],
    cumulative_deficit_baseline: [...],
    mortgage_rate_annual_avg: [...]
  }

GET /api/national/scenario
  Query params: hh_formation (low|baseline|high), demolition (low|baseline|high)
  Returns: cumulative deficit under specified national scenario
  Source: scenario_grid_national (horizon=3)
  Response: {
    hh_formation_assumption,
    demolition_assumption,
    current_deficit,
    end_state_deficit_1yr,
    end_state_deficit_2yr,
    end_state_deficit_3yr
  }
```

#### Panel 2: Metro Explorer
```
GET /api/metro/{cbsa_code}/summary
  Returns: all annual metrics for the selected metro
  Source: metrics_metro_annual
  Response: {
    cbsa_code, cbsa_name,
    years: [...],
    permits_per_1000: [...],
    permits_vs_national_avg_ratio: [...],
    vacancy_rate: [...],
    vacancy_rate_yoy_change: [...],
    mortgage_pct_median_income: [...],
    fmr_pct_median_income: [...],
    cumulative_deficit_since_2008: [...],
    domestic_migration_per_permit: [...],
    dq_flags: {year: flag_code}   // any flagged years
  }

GET /api/metro/{cbsa_code}/latest
  Returns: single-year snapshot (most recent year) for dashboard display
  Source: metrics_metro_annual (latest year)
  Response: {
    cbsa_code, cbsa_name,
    latest_year,
    permits_per_1000_residents,
    permits_vs_national_avg_ratio,
    vacancy_rate,
    vacancy_rate_yoy_change,
    mortgage_pct_median_income,
    fmr_pct_median_income,
    cumulative_deficit_since_2008,
    oversupply_signal: 'red'|'yellow'|'green',   // computed from thresholds in METRICS.md
    dq_flag
  }

GET /api/metro/compare
  Query params: cbsa_code_1, cbsa_code_2
  Returns: latest-year snapshot for both metros side by side
  Source: metrics_metro_annual (latest year for each)
```

#### Panel 3: Scenario Builder
```
GET /api/scenario/{cbsa_code}
  Query params:
    hh_formation: low|baseline|high
    demolition: low|baseline|high
    migration: reverting|flat|continuing
    horizon: 1|2|3
  Returns: single scenario grid row
  Source: scenario_grid
  Response: {
    cbsa_code, cbsa_name,
    hh_formation_assumption,
    demolition_assumption,
    migration_assumption,
    horizon_years,
    current_deficit_baseline,
    projected_new_households,
    projected_completions,
    projected_surplus_deficit,
    end_state_deficit,
    scenario_label
  }

GET /api/scenario/{cbsa_code}/all
  Returns: all 81 scenario combinations for the metro
  Used for: populating scenario comparison table in UI
  Source: scenario_grid (all rows for cbsa_code)

POST /api/interpret
  Body: {
    cbsa_code: string,
    cbsa_name: string,
    scenario_params: {hh_formation, demolition, migration, horizon},
    scenario_output: {current_deficit, end_state_deficit, projected_surplus_deficit},
    metro_context: {vacancy_rate, permits_vs_national_avg, mortgage_pct_income, sun_belt},
    national_context: {national_deficit, mortgage_rate},
    user_question: string
  }
  Returns: Claude API response (streamed)
  Action: Calls Claude API with structured prompt (see Claude Integration below)
```

#### Data Quality
```
GET /api/dq/summary
  Returns: DQ status summary for current data
  Source: dq_log
  Response: {
    last_run: timestamp,
    total_warnings: N,
    total_errors: N,
    cbsas_with_errors: [{cbsa_code, cbsa_name, flag_code, metric_affected}],
    benchmark_status: 'ok'|'warning'
  }
```

---

## Backend Implementation Notes

**Database connection:** Use a single shared SQLite connection per request via FastAPI's dependency injection. Close after each request.

**CORS:** Allow all origins for local development (`*`). Tighten if deployed.

**Error handling:** All endpoints return `{error: message, code: string}` on failure with appropriate HTTP status codes.

**Query patterns:** All queries hit precalculated tables (`metrics_*`, `scenario_grid`). No runtime joins across base tables.

**File structure:**
```
backend/
  main.py           # FastAPI app, route definitions
  db.py             # SQLite connection management
  routers/
    national.py
    metro.py
    scenario.py
    dq.py
  models.py         # Pydantic response models
  config.py         # DB path, API keys from environment
```

---

## Claude API Integration (Panel 3)

### Purpose
When the user sets a scenario and asks "what does this mean for DHI vs. regional banks?", Claude receives the structured scenario output plus metro context and provides an investment-framing interpretation.

### Prompt Template
The `/api/interpret` endpoint constructs this prompt before calling the Claude API:

```
SYSTEM:
You are a housing market analyst helping an investor understand the investment implications 
of U.S. housing supply data. You have access to specific quantitative data for the metro 
and scenario the user has selected. Your job is to interpret what the numbers mean — not 
to recite them back — focusing on the implications for:
1. Homebuilder equity exposure (DHI, LEN, NVR), particularly geographic mix
2. Regional bank credit risk (construction lending, mortgage portfolios)

Be specific. When the data supports a clear conclusion, say so. When it is ambiguous, 
explain the ambiguity. Do not hedge everything. The user understands investing involves 
uncertainty.

DATA CONTEXT:
Metro: {cbsa_name} ({cbsa_code})
Sun Belt market: {sun_belt}
Region: {region}

Current conditions (latest available year):
- Cumulative housing deficit since 2008 (baseline): {current_deficit:,} units
  (negative = deficit/undersupply; positive = surplus/oversupply)
- Vacancy rate: {vacancy_rate:.1%} (national avg ~6%)
- Permits per 1,000 residents vs. national average: {permits_ratio:.2f}x
- Mortgage payment as % of median income: {mortgage_pct:.1%} (30% = standard affordability threshold)

Scenario selected:
- Household formation assumption: {hh_formation} 
- Demolition/obsolescence rate: {demolition}
- Migration trend: {migration} {migration_note}
- Time horizon: {horizon} year(s)

Scenario output:
- Projected surplus/deficit over horizon: {projected_surplus_deficit:,} units
  (negative = additional deficit accumulating; positive = supply catching up)
- Projected end-state deficit: {end_state_deficit:,} units

National context:
- National cumulative deficit (baseline): {national_deficit:,} units
- Current 30-year mortgage rate: {mortgage_rate:.2%}

USER QUESTION:
{user_question}
```

**Where `{migration_note}` is:**
- reverting: "(pandemic migration wave normalizing to pre-2020 trend)"
- flat: "(migration staying at recent 2022–2023 levels)"
- continuing: "(migration remaining elevated at pandemic peak rate)"

### API Call Configuration
```python
{
  "model": "claude-sonnet-4-20250514",
  "max_tokens": 600,
  "stream": True,
  "messages": [
    {"role": "user", "content": constructed_prompt}
  ]
}
```

**Stream to frontend:** The `/api/interpret` endpoint returns a streaming response. The frontend renders Claude's output as it arrives.

**Default user question (pre-filled in UI):**
```
Given this scenario, what are the investment implications for homebuilder exposure 
(DHI, LEN, NVR) and regional bank credit risk in this market?
```

The user can edit this before submitting.

---

## Frontend: React Application

**Entry point:** `frontend/index.html` (served statically or as React app)
**Framework:** React 18 with hooks
**Charting:** Recharts
**Styling:** Tailwind CSS (utility classes only)
**API base URL:** Configurable, defaults to `http://localhost:8000`

### Component Hierarchy

```
App
├── Header (title, data vintage info, DQ status indicator)
├── Panel1_NationalPicture
│   ├── FormationVsCompletionsChart (line chart: dual series)
│   ├── CumulativeDeficitCounter (animated number display)
│   └── ScenarioToggle (low/baseline/high buttons)
├── Panel2_MetroExplorer
│   ├── MetroSelector (dropdown, 50 metros)
│   ├── MetroSnapshot (key stats grid)
│   ├── PermitsBarChart (metro vs national avg)
│   ├── VacancyTrendChart (line chart)
│   ├── AffordabilityGauge (mortgage % and FMR %)
│   └── DQIndicator (flag icon if metro has DQ issues)
├── Panel3_ScenarioBuilder
│   ├── MetroSelector (same component, different instance)
│   ├── ScenarioSliders
│   │   ├── HHFormationSlider (3 stops: low/baseline/high)
│   │   ├── DemolitionSlider (3 stops)
│   │   ├── MigrationSlider (3 stops)
│   │   └── HorizonSlider (3 stops: 1/2/3 years)
│   ├── ScenarioOutput (deficit number, scenario label)
│   ├── ComparisonToggle (enable side-by-side)
│   └── ClaudeInterpretation
│       ├── QuestionInput (editable text, pre-filled)
│       ├── SubmitButton
│       └── ResponseStream (renders streaming Claude output)
└── Footer (data sources, methodology note link)
```

### State Management

Use React `useState` and `useEffect` only. No Redux or external state library needed for this scope.

Key state objects:
```javascript
// Panel 1
const [nationalData, setNationalData] = useState(null);
const [nationalScenario, setNationalScenario] = useState({hh_formation: 'baseline', demolition: 'baseline'});

// Panel 2
const [selectedMetro, setSelectedMetro] = useState('19100'); // default: Dallas-Fort Worth
const [metroData, setMetroData] = useState(null);

// Panel 3
const [scenarioMetro, setScenarioMetro] = useState('38060'); // default: Phoenix
const [scenarioParams, setScenarioParams] = useState({
  hh_formation: 'baseline',
  demolition: 'baseline',
  migration: 'flat',
  horizon: 3
});
const [scenarioOutput, setScenarioOutput] = useState(null);
const [userQuestion, setUserQuestion] = useState('Given this scenario, what are the investment implications for homebuilder exposure (DHI, LEN, NVR) and regional bank credit risk in this market?');
const [claudeResponse, setClaudeResponse] = useState('');
const [isStreaming, setIsStreaming] = useState(false);
```

### Slider UX Specification

Sliders snap to three discrete values. Implementation: use a standard HTML range input with `min=0, max=2, step=1`, mapping 0→low, 1→baseline, 2→high (or equivalent). Display the label, not the number. When the slider value changes, update `scenarioParams` state and re-fetch the corresponding scenario grid row from the backend.

Slider labels:

**Household Formation:**
- 0: `Low (−10% vs. trend)` 
- 1: `Baseline (2010–2019 trend)`
- 2: `High (+10% vs. trend)`

**Demolition/Obsolescence:**
- 0: `Low (0.15%/yr)`
- 1: `Baseline (0.25%/yr)`
- 2: `High (0.35%/yr)`

**Migration Trend:**
- 0: `Reverting (normalize to pre-2020)`
- 1: `Flat (stay at recent levels)`
- 2: `Continuing (maintain pandemic pace)`

**Time Horizon:**
- 0: `1 Year`
- 1: `2 Years`
- 2: `3 Years`

### Chart Specifications

**Panel 1 — Household Formation vs. Completions:**
- Type: LineChart (Recharts)
- X axis: year (2000–present)
- Y axis: thousands of units
- Series 1: total_completions (solid blue)
- Series 2: implied_new_households (solid orange)
- Series 3: cumulative_deficit_since_2008 on secondary Y axis (dashed gray, right axis)
- Tooltip: show all three values on hover

**Panel 1 — Deficit Counter:**
- Large number display: `{national_deficit:,} units` 
- Color: red if negative (deficit), green if positive (surplus)
- Updates when national scenario toggle changes

**Panel 2 — Permits Bar Chart:**
- Type: BarChart (Recharts)
- X axis: year (last 10 years)
- Y axis: permits per 1,000 residents
- Bar: metro value (blue)
- Reference line: national average (red dashed)
- Tooltip: show ratio vs. national average

**Panel 2 — Vacancy Trend:**
- Type: LineChart
- X axis: year (2015–present)
- Y axis: vacancy rate (%)
- Series: metro vacancy rate (solid)
- Reference band: 5–8% "normal" range (gray shaded area)

**Panel 2 — Affordability:**
- Type: Two horizontal bar gauges
- Gauge 1: mortgage_pct_median_income (0–60% scale, 30% threshold line)
- Gauge 2: fmr_pct_median_income (0–60% scale, 30% threshold line)
- Color: green < 30%, yellow 30–40%, red > 40%

**Panel 3 — Scenario Output:**
- Primary display: large number showing `end_state_deficit` with sign and label
- Secondary: `projected_surplus_deficit` for the horizon period (the flow, not the stock)
- Scenario label string from the database displayed below

### Color Coding Logic (Metro Explorer)

Computed in frontend from `oversupply_signal` field returned by `/api/metro/{cbsa_code}/latest`:

```javascript
// Backend computes this based on METRICS.md thresholds:
// permits_vs_national_avg_ratio > 1.25 AND vacancy_rate > 0.08 AND vacancy_rate_yoy_change > 0.01
//   → 'red' (oversupply signal)
// permits_vs_national_avg_ratio < 0.75 OR vacancy_rate < 0.05
//   → 'green' (undersupply signal)  
// Otherwise → 'yellow' (mixed)
```

Display as: colored badge next to metro name in dropdown and as a status indicator in the snapshot panel.

### DQ Warning Display

If `dq_flag` is non-null on any metric:
- Show a ⚠️ icon next to the affected metric label
- Tooltip on hover: human-readable explanation of the flag (lookup from flag code registry in DATA_GOVERNANCE.md)
- Do not hide flagged metrics — display them with the warning

### Claude Interpretation Panel

1. User reads the scenario output numbers
2. User optionally edits the pre-filled question
3. User clicks "Interpret with Claude"
4. Frontend calls `POST /api/interpret` with full context payload
5. Backend constructs prompt (see Claude Integration above) and calls Claude API with streaming
6. Frontend renders the streamed response character-by-character in a styled text area
7. Below the response: small text noting "Analysis generated by Claude based on precalculated scenario data"

**Loading state:** Show a pulsing cursor animation while streaming. Do not show a spinner that blocks the rest of the UI.

---

## Running the Application

### Start backend:
```bash
cd backend
pip install fastapi uvicorn sqlite3
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### Start frontend (development):
```bash
cd frontend
npm install
npm start   # starts on port 3000
```

### Environment variables required:
```
FRED_API_KEY=your_key_here
CENSUS_API_KEY=your_key_here
ANTHROPIC_API_KEY=your_key_here
DB_PATH=../data/db/housing.db
```

---

## MVP Simplifications (Acceptable for V1)

These are deliberate scope reductions for the MVP:

1. **No authentication.** Single-user tool; no login.
2. **No mobile layout.** Desktop only; the three-panel layout requires wide screen.
3. **No caching layer.** All API calls hit SQLite directly. Acceptable given precalculated tables and 50-metro scope.
4. **Static CBSA list.** The top-50 list is hardcoded in `cbsa_top50.csv`. No dynamic metro selection beyond the 50.
5. **No chart export.** Copy-paste from browser is sufficient for V1.
6. **No comparison mode in Panel 2.** Single metro only in V1; comparison is Panel 3 feature only.
