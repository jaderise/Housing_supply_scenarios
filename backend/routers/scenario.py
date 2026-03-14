"""Scenario Builder API endpoints (Panel 3) — real-time calculation."""

import os

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.db import get_db
from backend.models import InterpretRequest
from backend.config import ANTHROPIC_API_KEY

router = APIRouter(prefix="/api", tags=["scenario"])


def _compute_scenario(conn, cbsa_code: str, hh_formation: str, demolition: str,
                      migration: str, income_growth: str, borrowing: str,
                      demographic: str, horizon: int):
    """Compute a single scenario in real time from metrics data."""
    from pipeline.utils.cbsa_utils import load_scenario_params

    params = load_scenario_params()

    ref = conn.execute(
        "SELECT cbsa_name FROM cbsa_reference WHERE cbsa_code = ?", (cbsa_code,)
    ).fetchone()
    if not ref:
        return None

    # Load metro metrics
    metrics_rows = conn.execute(
        "SELECT * FROM metrics_metro_annual WHERE cbsa_code = ? ORDER BY year",
        (cbsa_code,),
    ).fetchall()
    if not metrics_rows:
        return None
    metrics = [dict(r) for r in metrics_rows]
    latest = metrics[-1]

    # Current baseline deficit
    current_deficit = float(latest.get("cumulative_deficit_since_2008") or 0)

    # Implied HH formation trend (last 3 years average)
    recent = metrics[-3:] if len(metrics) >= 3 else metrics
    hh_trend = sum(float(m.get("implied_new_households") or 0) for m in recent) / len(recent)

    # Trailing permit rate (latest year)
    permits_row = conn.execute(
        "SELECT permits_total FROM permits WHERE cbsa_code = ? ORDER BY year DESC LIMIT 1",
        (cbsa_code,),
    ).fetchone()
    trailing_permit_rate = float(dict(permits_row)["permits_total"]) if permits_row else 0

    # Current housing stock
    stock_row = conn.execute(
        "SELECT total_units FROM housing_stock WHERE cbsa_code = ? ORDER BY year DESC LIMIT 1",
        (cbsa_code,),
    ).fetchone()
    current_stock = float(dict(stock_row)["total_units"]) if stock_row else 0

    # Net domestic migration trend
    pop_rows = conn.execute(
        "SELECT domestic_migration_net FROM population WHERE cbsa_code = ? ORDER BY year DESC LIMIT 3",
        (cbsa_code,),
    ).fetchall()
    recent_mig = sum(float(dict(r)["domestic_migration_net"] or 0) for r in pop_rows) / max(len(pop_rows), 1)

    # --- Parameter adjustments (with fallback for legacy values) ---
    hh_cfg = params["hh_formation"].get(hh_formation, params["hh_formation"]["baseline"])
    hh_adj = hh_cfg["adjustment"]
    demo_cfg = params["demolition"].get(demolition, params["demolition"]["baseline"])
    demo_rate = demo_cfg["rate"]
    mig_cfg = params["migration"].get(migration, params["migration"]["flat"])
    mig_adj = mig_cfg["adjustment"]
    inc_cfg = params["income_growth"].get(income_growth, params["income_growth"]["baseline"])
    income_rate = inc_cfg["rate"]
    bor_cfg = params["borrowing_environment"].get(borrowing, params["borrowing_environment"]["baseline"])
    borrow_ltv_adj = bor_cfg["ltv_adjustment"]
    dem_cfg = params["demographic_shift"].get(demographic, params["demographic_shift"]["baseline"])
    demo_hh_adj = dem_cfg["hh_adjustment"]

    # Combined HH formation multiplier: base * hh_formation * demographic_shift
    combined_hh_adj = hh_adj * demo_hh_adj

    # Borrowing affects completions (tighter credit = fewer starts = fewer completions)
    completions_adj = borrow_ltv_adj

    projected_hh = 0
    projected_comp = 0
    running_deficit = current_deficit

    for yr in range(1, horizon + 1):
        # HH formation: trend * adjustments + migration shift
        yr_hh = hh_trend * combined_hh_adj + recent_mig * (mig_adj - 1.0)

        # Income growth affects demand (higher income = more household formation capacity)
        income_factor = (1 + income_rate) ** yr
        yr_hh *= (1 + (income_factor - 1) * 0.3)  # 30% passthrough of income to HH formation

        # Demolition
        yr_demolition = current_stock * demo_rate

        # Completions: trailing rate * borrowing adjustment
        yr_completions = trailing_permit_rate * completions_adj

        # Annual net
        yr_net = yr_completions - yr_hh - yr_demolition
        running_deficit += yr_net
        projected_hh += yr_hh
        projected_comp += yr_completions

    projected_surplus_deficit = int(
        projected_comp - projected_hh - current_stock * demo_rate * horizon
    )

    # Build scenario label
    label_parts = [
        params["hh_formation"][hh_formation]["label"],
        params["demolition"][demolition]["label"],
        params["migration"][migration]["label"],
        params["income_growth"][income_growth]["label"],
        params["borrowing_environment"][borrowing]["label"],
        params["demographic_shift"][demographic]["label"],
        f"{horizon}yr",
    ]

    return {
        "cbsa_code": cbsa_code,
        "cbsa_name": dict(ref)["cbsa_name"],
        "hh_formation_assumption": hh_formation,
        "demolition_assumption": demolition,
        "migration_assumption": migration,
        "income_growth_assumption": income_growth,
        "borrowing_assumption": borrowing,
        "demographic_assumption": demographic,
        "horizon_years": horizon,
        "current_deficit_baseline": int(current_deficit),
        "projected_new_households": int(projected_hh),
        "projected_completions": int(projected_comp),
        "projected_surplus_deficit": projected_surplus_deficit,
        "end_state_deficit": int(running_deficit),
        "scenario_label": " | ".join(label_parts),
    }


@router.get("/scenario/{cbsa_code}")
def get_scenario(
    cbsa_code: str,
    hh_formation: str = "baseline",
    demolition: str = "baseline",
    migration: str = "flat",
    income_growth: str = "baseline",
    borrowing: str = "baseline",
    demographic: str = "baseline",
    horizon: int = 3,
):
    """Compute and return a single scenario in real time."""
    with get_db() as conn:
        result = _compute_scenario(
            conn, cbsa_code, hh_formation, demolition, migration,
            income_growth, borrowing, demographic, horizon
        )

    if not result:
        raise HTTPException(status_code=404, detail=f"Metro {cbsa_code} not found or no data")

    return result


@router.get("/scenario/{cbsa_code}/all")
def get_all_scenarios(cbsa_code: str):
    """Return all pre-computed scenario combinations for a metro (legacy, from grid)."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM scenario_grid WHERE cbsa_code = ?", (cbsa_code,)
        ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No scenarios for metro {cbsa_code}")

    return [dict(row) for row in rows]


@router.post("/interpret")
async def interpret_scenario(request: InterpretRequest):
    """Call Claude API to interpret a scenario. Returns a streaming response."""
    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY == "your_anthropic_key_here":
        raise HTTPException(
            status_code=503,
            detail="ANTHROPIC_API_KEY not configured. Add your real API key to the .env file."
        )

    import anthropic

    migration = request.scenario_params.get("migration", "flat")
    migration_notes = {
        "strong_decline": "(net migration falling 50% from current pace)",
        "moderate_decline": "(net migration falling 25% from current pace)",
        "flat": "(net migration staying at current pace)",
        "moderate_growth": "(net migration accelerating 25% above current pace)",
        "strong_growth": "(net migration accelerating 50% above current pace)",
    }
    migration_note = migration_notes.get(migration, "")

    prompt = f"""Metro: {request.cbsa_name} ({request.cbsa_code})
Sun Belt market: {request.metro_context.get('sun_belt', False)}

Current conditions (latest available year):
- Cumulative housing deficit since 2008 (baseline): {request.scenario_output.get('current_deficit', 0):,} units
  (negative = deficit/undersupply; positive = surplus/oversupply)
- Vacancy rate: {request.metro_context.get('vacancy_rate', 0):.1%}
- Permits per 1,000 residents vs. national average: {request.metro_context.get('permits_vs_national_avg', 1.0):.2f}x
- Mortgage payment as % of median income: {request.metro_context.get('mortgage_pct_income', 0):.1%}

Scenario selected:
- Household formation assumption: {request.scenario_params.get('hh_formation', 'baseline')}
- Demolition/obsolescence rate: {request.scenario_params.get('demolition', 'baseline')}
- Migration trend: {migration} {migration_note}
- Income growth: {request.scenario_params.get('income_growth', 'baseline')}
- Borrowing environment: {request.scenario_params.get('borrowing', 'baseline')}
- Demographic shift: {request.scenario_params.get('demographic', 'baseline')}
- Time horizon: {request.scenario_params.get('horizon', 3)} year(s)

Scenario output:
- Projected surplus/deficit over horizon: {request.scenario_output.get('projected_surplus_deficit', 0):,} units
- Projected end-state deficit: {request.scenario_output.get('end_state_deficit', 0):,} units

National context:
- National cumulative deficit (baseline): {request.national_context.get('national_deficit', 0):,} units
- Current 30-year mortgage rate: {request.national_context.get('mortgage_rate', 0):.2%}

{request.user_question}"""

    system_prompt = """You are a housing market analyst helping an investor understand the investment implications \
of U.S. housing supply data. You have access to specific quantitative data for the metro \
and scenario the user has selected. Your job is to interpret what the numbers mean -- not \
to recite them back -- focusing on the implications for:
1. Homebuilder equity exposure (DHI, LEN, NVR), particularly geographic mix
2. Regional bank credit risk (construction lending, mortgage portfolios)

Be specific. When the data supports a clear conclusion, say so. When it is ambiguous, \
explain the ambiguity. Do not hedge everything. The user understands investing involves \
uncertainty."""

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    def generate():
        with client.messages.stream(
            model="claude-sonnet-4-20250514",
            max_tokens=600,
            system=system_prompt,
            messages=[{"role": "user", "content": prompt}],
        ) as stream:
            for text in stream.text_stream:
                yield text

    return StreamingResponse(generate(), media_type="text/plain")
