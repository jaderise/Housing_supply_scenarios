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


@router.get("/compare")
def compare_metros(
    cbsa_code_1: str = Query(..., description="First metro CBSA code"),
    cbsa_code_2: str = Query(..., description="Second metro CBSA code"),
):
    """Return latest-year snapshot for two metros side by side."""
    metro1 = get_metro_latest(cbsa_code_1)
    metro2 = get_metro_latest(cbsa_code_2)
    return {"metro_1": metro1, "metro_2": metro2}
