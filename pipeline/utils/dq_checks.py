"""Data quality checks and DQ log writer."""

import sqlite3
from datetime import datetime
from typing import Optional

import pandas as pd


DQ_FLAGS = {
    "NULL_VALUE": ("WARNING", "Expected field is null"),
    "OUT_OF_RANGE": ("WARNING", "Value outside plausible bounds"),
    "IMPLAUSIBLE_CHANGE": ("WARNING", "YoY change exceeds threshold"),
    "MISSING_PERIOD": ("ERROR", "CBSA x year combination missing entirely"),
    "MISSING_CBSA": ("ERROR", "Top-50 CBSA absent from source data"),
    "CROSSWALK_PARTIAL": ("WARNING", "CBSA matched via crosswalk with < 80% address coverage"),
    "SERIES_DISCONTINUED": ("WARNING", "FRED series returned no data for last 6 months"),
    "VINTAGE_MISMATCH": ("WARNING", "PEP estimate uses different vintage than adjacent year"),
    "BENCHMARK_DEVIATION": ("WARNING", "National deficit outside expected range vs. published estimates"),
    "IMPUTED_VALUE": ("INFO", "Missing value filled via interpolation"),
    "FETCH_FAILURE": ("ERROR", "Source returned error code or 0 rows"),
    "FETCH_FAILURE_CRITICAL": ("CRITICAL", "Critical source (permits/population) failed"),
}


def log_dq_issue(
    db_path: str,
    stage: str,
    source: str,
    rule_code: str,
    action_taken: str,
    cbsa_code: Optional[str] = None,
    year: Optional[int] = None,
    quarter: Optional[int] = None,
    message: Optional[str] = None,
) -> None:
    """Write a DQ issue to the dq_log table."""
    severity = DQ_FLAGS.get(rule_code, ("WARNING", "Unknown"))[0]
    if message is None:
        message = DQ_FLAGS.get(rule_code, ("WARNING", "Unknown"))[1]

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """INSERT INTO dq_log
               (run_timestamp, stage, source, cbsa_code, year, quarter,
                rule_code, severity, message, action_taken)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now().isoformat(),
                stage,
                source,
                cbsa_code,
                year,
                quarter,
                rule_code,
                severity,
                message,
                action_taken,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def ensure_dq_log_table(db_path: str) -> None:
    """Create the dq_log table if it doesn't exist."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dq_log (
                log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_timestamp   TEXT,
                stage           TEXT,
                source          TEXT,
                cbsa_code       TEXT,
                year            INTEGER,
                quarter         INTEGER,
                rule_code       TEXT,
                severity        TEXT,
                message         TEXT,
                action_taken    TEXT
            )
        """)
        conn.commit()
    finally:
        conn.close()


def check_completeness(
    df: pd.DataFrame,
    cbsa_list: list[str],
    year: int,
    source: str,
) -> list[str]:
    """Check that all expected CBSAs are present. Returns list of missing codes."""
    present = set(df["cbsa_code"].astype(str).unique())
    missing = [c for c in cbsa_list if c not in present]
    return missing


def check_non_negative(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Flag rows where specified columns have negative values."""
    mask = pd.Series(False, index=df.index)
    for col in columns:
        if col in df.columns:
            mask |= df[col].fillna(0) < 0
    return df[mask]


def check_yoy_change(
    df: pd.DataFrame,
    column: str,
    max_increase_pct: float = 2.0,
    max_decrease_pct: float = 0.75,
) -> pd.DataFrame:
    """Flag rows with implausible YoY changes. Returns flagged rows."""
    df_sorted = df.sort_values(["cbsa_code", "year"])
    df_sorted["_prev"] = df_sorted.groupby("cbsa_code")[column].shift(1)
    df_sorted["_pct_change"] = (
        (df_sorted[column] - df_sorted["_prev"]) / df_sorted["_prev"].replace(0, float("nan"))
    )

    flagged = df_sorted[
        (df_sorted["_pct_change"] > max_increase_pct)
        | (df_sorted["_pct_change"] < -max_decrease_pct)
    ].copy()

    return flagged.drop(columns=["_prev", "_pct_change"], errors="ignore")


def check_range(
    df: pd.DataFrame,
    column: str,
    min_val: float,
    max_val: float,
) -> pd.DataFrame:
    """Flag rows where column value is outside plausible range."""
    return df[(df[column] < min_val) | (df[column] > max_val)]
