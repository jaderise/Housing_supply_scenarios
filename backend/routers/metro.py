"""Metro-level API endpoints (Panel 2)."""

from fastapi import APIRouter, HTTPException, Query

from backend.db import get_db

router = APIRouter(prefix="/api/metro", tags=["metro"])


def compute_oversupply_signal(row: dict) -> str:
    """Compute the oversupply signal color from metrics."""
    ratio = row.get("permits_vs_national_avg_ratio")
    vac = row.get("vacancy_rate_annual_avg")
    vac_change = row.get("vacancy_rate_yoy_change")

    if ratio and vac and vac_change:
        if ratio > 1.25 and vac > 0.08 and vac_change > 0.01:
            return "red"
        if ratio < 0.75 or (vac and vac < 0.05):
            return "green"
    return "yellow"


@router.get("/{cbsa_code}/summary")
def get_metro_summary(cbsa_code: str):
    """Return all annual metrics for a metro."""
    with get_db() as conn:
        # Get metro name
        ref = conn.execute(
            "SELECT cbsa_name FROM cbsa_reference WHERE cbsa_code = ?", (cbsa_code,)
        ).fetchone()
        if not ref:
            raise HTTPException(status_code=404, detail=f"Metro {cbsa_code} not found")

        rows = conn.execute(
            "SELECT * FROM metrics_metro_annual WHERE cbsa_code = ? ORDER BY year",
            (cbsa_code,),
        ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No metrics for metro {cbsa_code}")

    result = {
        "cbsa_code": cbsa_code,
        "cbsa_name": dict(ref)["cbsa_name"],
        "years": [],
        "permits_per_1000": [],
        "permits_vs_national_avg_ratio": [],
        "vacancy_rate": [],
        "vacancy_rate_yoy_change": [],
        "mortgage_pct_median_income": [],
        "fmr_pct_median_income": [],
        "cumulative_deficit_since_2008": [],
        "domestic_migration_per_permit": [],
        "dq_flags": {},
    }

    for row in rows:
        d = dict(row)
        result["years"].append(d.get("year"))
        result["permits_per_1000"].append(d.get("permits_per_1000_residents"))
        result["permits_vs_national_avg_ratio"].append(d.get("permits_vs_national_avg_ratio"))
        result["vacancy_rate"].append(d.get("vacancy_rate_annual_avg"))
        result["vacancy_rate_yoy_change"].append(d.get("vacancy_rate_yoy_change"))
        result["mortgage_pct_median_income"].append(d.get("mortgage_pct_median_income"))
        result["fmr_pct_median_income"].append(d.get("fmr_pct_median_income"))
        result["cumulative_deficit_since_2008"].append(d.get("cumulative_deficit_since_2008"))
        result["domestic_migration_per_permit"].append(d.get("domestic_migration_per_permit"))

        if d.get("dq_flag"):
            result["dq_flags"][d["year"]] = d["dq_flag"]

    return result


@router.get("/{cbsa_code}/latest")
def get_metro_latest(cbsa_code: str):
    """Return single-year snapshot for dashboard display."""
    with get_db() as conn:
        ref = conn.execute(
            "SELECT cbsa_name FROM cbsa_reference WHERE cbsa_code = ?", (cbsa_code,)
        ).fetchone()
        if not ref:
            raise HTTPException(status_code=404, detail=f"Metro {cbsa_code} not found")

        row = conn.execute(
            """SELECT * FROM metrics_metro_annual
               WHERE cbsa_code = ?
               ORDER BY year DESC LIMIT 1""",
            (cbsa_code,),
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"No metrics for metro {cbsa_code}")

    d = dict(row)
    signal = compute_oversupply_signal(d)

    return {
        "cbsa_code": cbsa_code,
        "cbsa_name": dict(ref)["cbsa_name"],
        "latest_year": d.get("year"),
        "permits_per_1000_residents": d.get("permits_per_1000_residents"),
        "permits_vs_national_avg_ratio": d.get("permits_vs_national_avg_ratio"),
        "vacancy_rate": d.get("vacancy_rate_annual_avg"),
        "vacancy_rate_yoy_change": d.get("vacancy_rate_yoy_change"),
        "mortgage_pct_median_income": d.get("mortgage_pct_median_income"),
        "fmr_pct_median_income": d.get("fmr_pct_median_income"),
        "cumulative_deficit_since_2008": d.get("cumulative_deficit_since_2008"),
        "oversupply_signal": signal,
        "dq_flag": d.get("dq_flag"),
    }


@router.get("/{cbsa_code}/trends")
def get_metro_trends(cbsa_code: str):
    """Return historical trend data for sparklines on the scenario builder sliders."""
    with get_db() as conn:
        ref = conn.execute(
            "SELECT cbsa_name FROM cbsa_reference WHERE cbsa_code = ?", (cbsa_code,)
        ).fetchone()
        if not ref:
            raise HTTPException(status_code=404, detail=f"Metro {cbsa_code} not found")

        # Metrics: implied_new_households, vacancy_rate
        metrics = conn.execute(
            """SELECT year, implied_new_households, vacancy_rate_annual_avg
               FROM metrics_metro_annual
               WHERE cbsa_code = ? ORDER BY year""",
            (cbsa_code,),
        ).fetchall()

        # Population (for migration proxy: YoY change)
        pop_rows = conn.execute(
            "SELECT year, population FROM population WHERE cbsa_code = ? ORDER BY year",
            (cbsa_code,),
        ).fetchall()

        # Income from housing_stock (ACS)
        income_rows = conn.execute(
            "SELECT year, median_hh_income FROM housing_stock WHERE cbsa_code = ? ORDER BY year",
            (cbsa_code,),
        ).fetchall()

        # Mortgage rate (national)
        mortgage_rows = conn.execute(
            "SELECT year, mortgage_rate_annual_avg FROM metrics_national_annual ORDER BY year"
        ).fetchall()

    # Build population YoY change series
    pop_list = [(r[0], r[1]) for r in pop_rows if r[1]]
    pop_change = []
    for i in range(1, len(pop_list)):
        pop_change.append({
            "year": pop_list[i][0],
            "value": pop_list[i][1] - pop_list[i - 1][1],
        })

    # Filter to last ~15 years for cleaner sparklines
    min_year = 2008

    def series(rows, year_col, val_col):
        out = []
        for r in rows:
            d = dict(r) if not isinstance(r, dict) else r
            yr = d.get(year_col, d.get("year"))
            val = d.get(val_col, d.get("value"))
            if yr and yr >= min_year and val is not None:
                out.append({"year": yr, "value": round(val, 6) if isinstance(val, float) else val})
        return out

    # Build HH formation YoY growth rate using 3-year trailing avg (smooths noisy data)
    hh_series = series(metrics, "year", "implied_new_households")
    hh_growth = []
    if len(hh_series) >= 4:
        # Compute 3-year trailing averages, then growth rate between them
        for i in range(3, len(hh_series)):
            avg_prev = sum(p["value"] for p in hh_series[i-3:i]) / 3
            avg_curr = sum(p["value"] for p in hh_series[i-2:i+1]) / 3
            if avg_prev and abs(avg_prev) > 500:
                rate = (avg_curr - avg_prev) / abs(avg_prev)
                rate = max(-0.30, min(0.30, rate))
                hh_growth.append({"year": hh_series[i]["year"], "value": round(rate, 6)})

    # Build population YoY growth rate series
    pop_series = series(pop_rows, "year", "population")
    pop_growth = []
    for i in range(1, len(pop_series)):
        prev_val = pop_series[i - 1]["value"]
        curr_val = pop_series[i]["value"]
        if prev_val and prev_val != 0:
            rate = (curr_val - prev_val) / abs(prev_val)
            pop_growth.append({"year": pop_series[i]["year"], "value": round(rate, 6)})

    return {
        "cbsa_code": cbsa_code,
        "cbsa_name": dict(ref)["cbsa_name"],
        "hh_formation": hh_series,
        "hh_formation_growth": hh_growth,
        "vacancy_rate": series(metrics, "year", "vacancy_rate_annual_avg"),
        "migration": [p for p in pop_change if p["year"] >= min_year],
        "income": series(income_rows, "year", "median_hh_income"),
        "mortgage_rate": series(mortgage_rows, "year", "mortgage_rate_annual_avg"),
        "population": pop_series,
        "population_growth": pop_growth,
    }


@router.get("/compare")
def compare_metros(
    cbsa_code_1: str = Query(..., description="First metro CBSA code"),
    cbsa_code_2: str = Query(..., description="Second metro CBSA code"),
):
    """Return latest-year snapshot for two metros side by side."""
    metro1 = get_metro_latest(cbsa_code_1)
    metro2 = get_metro_latest(cbsa_code_2)
    return {"metro_1": metro1, "metro_2": metro2}
