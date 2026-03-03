# PRD.md — Product Requirements Document
## Housing Market Investment Analysis Tool

---

## 1. Purpose and Investment Question

This product exists to answer one investment question:

> **Is the U.S. housing shortage a national structural phenomenon, or a patchwork of supply-constrained markets coexisting with Sun Belt markets that overbuilt during the pandemic migration wave — and what does that distinction mean for homebuilder equities (DHI, LEN, NVR) and regional bank credit risk?**

The tool provides evidence-based analysis grounded in public data, with explicit scenario assumptions so the user can stress-test both the bull and bear cases.

---

## 2. User

A single primary user: the analyst/author building this product. The tool is an analytical workbench, not a consumer product. Design for depth over onboarding simplicity.

---

## 3. Success Criteria

The product is successful if it can answer the following questions from data:

1. What is the cumulative housing deficit or surplus nationally since 2008, under a range of household formation assumptions?
2. Which of the top 50 metros are structurally undersupplied vs. potentially oversupplied, and by how much?
3. How does Sun Belt construction activity compare to population-adjusted demand, before and after the pandemic migration wave?
4. What does affordability look like by metro, and how does it interact with the supply picture?
5. Given a specific metro and scenario, what are the investment implications for homebuilder exposure vs. regional bank credit risk?

---

## 4. Product Components

### 4.1 Data Pipeline
- Fetches raw data from Census, HUD, and FRED public APIs/downloads
- Lands raw data as immutable flat CSVs
- Transforms and normalizes to a SQLite analytical store
- Precalculates all metrics and scenario grid outputs
- Runs on-demand to refresh data; designed to be re-runnable without side effects

### 4.2 Backend API
- Lightweight FastAPI server running in Claude's environment
- Serves precalculated data from SQLite to the frontend
- Stateless; no user session management required

### 4.3 Frontend Application
- React-based web application
- Three panels: National Picture, Metro Explorer, Scenario Builder
- Scenario explorer maps to precalculated scenario grid (discrete parameter snapping)
- Claude API integration in Scenario Builder for analytical interpretation

---

## 5. The Three Panels

### Panel 1: National Picture
- Line chart: household formation vs. housing completions, 2000–present
- Running cumulative surplus/deficit counter (updates with scenario toggle)
- Toggle between three household formation rate assumptions (low / baseline / high)
- Source callout for transparency

### Panel 2: Metro Explorer
- Dropdown: top 50 metros by population
- Bar chart: permits per 1,000 residents vs. national average (trailing 3 years)
- Line chart: vacancy rate trend (2015–present where available)
- Affordability gauge: mortgage payment as % of median income
- Color coding: red = potential oversupply signal, green = undersupply signal, yellow = mixed
- Key stats panel: population growth, net domestic migration, completions vs. household formation

### Panel 3: Scenario Builder
- Metro selector (same 50 metros)
- Sliders snapping to discrete grid values:
  - Household formation rate assumption (low / baseline / high)
  - Demolition/obsolescence rate (low / baseline / high)
  - Migration trend (reverting / flat / continuing)
  - Time horizon (1 / 2 / 3 years)
- Output: projected surplus/deficit under selected scenario
- Claude interpretation panel: user can ask Claude to interpret the scenario in context of the investment thesis
- Comparison mode: side-by-side two metros

---

## 6. Scope

### In Scope
- Top 50 U.S. metros by population (CBSA level)
- Data from 2000 forward where available (some series start later)
- Residential housing only (single-family and multifamily combined, with ability to split)
- Public data sources only (Census, HUD, FRED)
- Precalculated scenarios; limited on-the-fly calculation
- Claude API integration for scenario interpretation

### Out of Scope
- Sub-metro (zip/tract) level analysis
- Commercial real estate
- Real-time data feeds or live price data
- User accounts, persistence, or multi-user support
- Mobile optimization
- Specific stock price targets or buy/sell recommendations

---

## 7. Constraints

- All components run within Claude's environment
- No external databases; SQLite only
- No paid data sources
- Frontend served as a React application connecting to local FastAPI backend
- Pipeline must be re-runnable without corrupting existing outputs

---

## 8. Data Refresh Cadence

| Source | Update Frequency | Pipeline Refresh Recommendation |
|--------|-----------------|--------------------------------|
| Census Building Permits | Monthly | Monthly |
| Census Population Estimates | Annual | Annual |
| ACS | Annual | Annual |
| HUD USPS Vacancy | Quarterly | Quarterly |
| FRED Series | Varies (monthly/quarterly) | Monthly |
| Fair Market Rents | Annual | Annual |

---

## 9. Non-Functional Requirements

- Pipeline must complete a full refresh in under 30 minutes
- Frontend must load initial view in under 3 seconds
- All scenario grid queries must return in under 500ms
- Data quality failures must log loudly — the pipeline must not silently produce bad metrics
