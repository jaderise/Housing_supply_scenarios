"""Data quality API endpoints."""

from fastapi import APIRouter

from backend.db import get_db

router = APIRouter(prefix="/api/dq", tags=["data_quality"])


@router.get("/summary")
def get_dq_summary():
    """Return DQ status summary for current data."""
    with get_db() as conn:
        # Last run timestamp
        last_run_row = conn.execute(
            "SELECT MAX(run_timestamp) as last_run FROM dq_log"
        ).fetchone()
        last_run = dict(last_run_row)["last_run"] if last_run_row else None

        # Count warnings and errors
        warning_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM dq_log WHERE severity = 'WARNING'"
        ).fetchone()
        error_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM dq_log WHERE severity IN ('ERROR', 'CRITICAL')"
        ).fetchone()

        # CBSAs with errors
        cbsa_errors = conn.execute(
            """SELECT DISTINCT dq.cbsa_code, ref.cbsa_name, dq.rule_code, dq.message
               FROM dq_log dq
               LEFT JOIN cbsa_reference ref ON dq.cbsa_code = ref.cbsa_code
               WHERE dq.severity IN ('ERROR', 'CRITICAL')
               AND dq.cbsa_code IS NOT NULL
               ORDER BY dq.cbsa_code"""
        ).fetchall()

        # Benchmark status
        benchmark = conn.execute(
            "SELECT COUNT(*) as cnt FROM dq_log WHERE rule_code = 'BENCHMARK_DEVIATION'"
        ).fetchone()

    return {
        "last_run": last_run,
        "total_warnings": dict(warning_count)["cnt"] if warning_count else 0,
        "total_errors": dict(error_count)["cnt"] if error_count else 0,
        "cbsas_with_errors": [
            {
                "cbsa_code": dict(r)["cbsa_code"],
                "cbsa_name": dict(r)["cbsa_name"],
                "flag_code": dict(r)["rule_code"],
                "metric_affected": dict(r)["message"],
            }
            for r in cbsa_errors
        ],
        "benchmark_status": "warning" if (benchmark and dict(benchmark)["cnt"] > 0) else "ok",
    }
