"""National-level API endpoints (Panel 1)."""

from fastapi import APIRouter, HTTPException

from backend.db import get_db

router = APIRouter(prefix="/api/national", tags=["national"])


@router.get("/timeseries")
def get_national_timeseries():
    """Return annual national data from 2000-present."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM metrics_national_annual ORDER BY year"
        ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="No national data available")

    result = {
        "years": [],
        "total_households": [],
        "total_completions": [],
        "total_permits": [],
        "hh_formation_rate": [],
        "cumulative_deficit_baseline": [],
        "mortgage_rate_annual_avg": [],
    }

    for row in rows:
        row_dict = dict(row)
        result["years"].append(row_dict.get("year"))
        result["total_households"].append(row_dict.get("total_households"))
        result["total_completions"].append(row_dict.get("total_completions"))
        result["total_permits"].append(row_dict.get("total_permits"))
        result["hh_formation_rate"].append(row_dict.get("hh_formation_rate"))
        result["cumulative_deficit_baseline"].append(row_dict.get("cumulative_deficit_since_2008"))
        result["mortgage_rate_annual_avg"].append(row_dict.get("mortgage_rate_annual_avg"))

    return result


@router.get("/scenario")
def get_national_scenario(
    hh_formation: str = "baseline",
    demolition: str = "baseline",
):
    """Return cumulative deficit under a national scenario."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT * FROM scenario_grid_national
               WHERE hh_formation_assumption = ?
               AND demolition_assumption = ?
               ORDER BY horizon_years""",
            (hh_formation, demolition),
        ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="Scenario not found")

    result = {
        "hh_formation_assumption": hh_formation,
        "demolition_assumption": demolition,
        "current_deficit": dict(rows[0]).get("current_deficit_baseline"),
        "end_state_deficit_1yr": None,
        "end_state_deficit_2yr": None,
        "end_state_deficit_3yr": None,
    }

    for row in rows:
        row_dict = dict(row)
        horizon = row_dict.get("horizon_years")
        if horizon == 1:
            result["end_state_deficit_1yr"] = row_dict.get("end_state_deficit")
        elif horizon == 2:
            result["end_state_deficit_2yr"] = row_dict.get("end_state_deficit")
        elif horizon == 3:
            result["end_state_deficit_3yr"] = row_dict.get("end_state_deficit")

    return result
