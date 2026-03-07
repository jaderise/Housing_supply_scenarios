"""FastAPI application for Housing Market Analysis Tool."""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.db import get_db
from backend.routers import national, metro, scenario, dq

app = FastAPI(
    title="Housing Market Analysis API",
    description="Backend API for the Housing Market Investment Analysis Tool",
    version="1.0.0",
)

# CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(national.router)
app.include_router(metro.router)
app.include_router(scenario.router)
app.include_router(dq.router)


@app.get("/api/metros")
def list_metros():
    """Return list of top-50 CBSAs with metadata."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM cbsa_reference ORDER BY population_rank"
        ).fetchall()

    return [
        {
            "cbsa_code": dict(r)["cbsa_code"],
            "cbsa_name": dict(r)["cbsa_name"],
            "region": dict(r)["region"],
            "sun_belt": bool(dict(r)["sun_belt"]),
            "population_rank": dict(r)["population_rank"],
        }
        for r in rows
    ]


@app.get("/api/metadata")
def get_metadata():
    """Return data vintage info."""
    with get_db() as conn:
        # Latest data year per source
        try:
            latest_year = conn.execute(
                "SELECT MAX(year) as max_year FROM metrics_metro_annual"
            ).fetchone()
        except Exception:
            latest_year = None

        # Last pipeline run
        try:
            last_run = conn.execute(
                "SELECT MAX(run_timestamp) as last_run FROM dq_log"
            ).fetchone()
        except Exception:
            last_run = None

        # Total metros
        try:
            total = conn.execute(
                "SELECT COUNT(*) as cnt FROM cbsa_reference"
            ).fetchone()
        except Exception:
            total = None

    return {
        "last_run": dict(last_run)["last_run"] if last_run else None,
        "latest_data_year": dict(latest_year)["max_year"] if latest_year else None,
        "total_metros": dict(total)["cnt"] if total else 0,
    }


@app.get("/download/project.zip")
async def download_project():
    """Download the full project as a zip file."""
    zip_path = os.path.join(os.path.dirname(__file__), "..", "Housing_supply_scenarios.zip")
    if os.path.isfile(zip_path):
        return FileResponse(zip_path, media_type="application/zip", filename="Housing_supply_scenarios.zip")
    return {"error": "Zip file not found. Please regenerate it."}


# Serve React frontend build if available
_frontend_build = os.path.join(os.path.dirname(__file__), "..", "frontend", "build")
if os.path.isdir(_frontend_build):
    from fastapi.responses import FileResponse

    # Serve static assets (JS, CSS, etc.)
    app.mount("/static", StaticFiles(directory=os.path.join(_frontend_build, "static")), name="static")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve React SPA — all non-API routes return index.html."""
        file_path = os.path.join(_frontend_build, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(_frontend_build, "index.html"))
