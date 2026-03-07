"""Scenario Builder API endpoints (Panel 3)."""

import os

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from backend.db import get_db
from backend.models import InterpretRequest
from backend.config import ANTHROPIC_API_KEY

router = APIRouter(prefix="/api", tags=["scenario"])


@router.get("/scenario/{cbsa_code}")
def get_scenario(
    cbsa_code: str,
    hh_formation: str = "baseline",
    demolition: str = "baseline",
    migration: str = "flat",
    horizon: int = 3,
):
    """Return a single scenario grid row."""
    with get_db() as conn:
        ref = conn.execute(
            "SELECT cbsa_name FROM cbsa_reference WHERE cbsa_code = ?", (cbsa_code,)
        ).fetchone()
        if not ref:
            raise HTTPException(status_code=404, detail=f"Metro {cbsa_code} not found")

        row = conn.execute(
            """SELECT * FROM scenario_grid
               WHERE cbsa_code = ?
               AND hh_formation_assumption = ?
               AND demolition_assumption = ?
               AND migration_assumption = ?
               AND horizon_years = ?""",
            (cbsa_code, hh_formation, demolition, migration, horizon),
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Scenario not found")

    d = dict(row)
    return {
        "cbsa_code": cbsa_code,
        "cbsa_name": dict(ref)["cbsa_name"],
        "hh_formation_assumption": d.get("hh_formation_assumption"),
        "demolition_assumption": d.get("demolition_assumption"),
        "migration_assumption": d.get("migration_assumption"),
        "horizon_years": d.get("horizon_years"),
        "current_deficit_baseline": d.get("current_deficit_baseline"),
        "projected_new_households": d.get("projected_new_households"),
        "projected_completions": d.get("projected_completions"),
        "projected_surplus_deficit": d.get("projected_surplus_deficit"),
        "end_state_deficit": d.get("end_state_deficit"),
        "scenario_label": d.get("scenario_label"),
    }


@router.get("/scenario/{cbsa_code}/all")
def get_all_scenarios(cbsa_code: str):
    """Return all 81 scenario combinations for a metro."""
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
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not configured")

    import anthropic

    # Build migration note
    migration = request.scenario_params.get("migration", "flat")
    migration_notes = {
        "reverting": "(pandemic migration wave normalizing to pre-2020 trend)",
        "flat": "(migration staying at recent 2022-2023 levels)",
        "continuing": "(migration remaining elevated at pandemic peak rate)",
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
