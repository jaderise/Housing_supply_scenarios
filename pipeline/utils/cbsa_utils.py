"""CBSA reference data utilities."""

import os
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

# Load .env from project root
_project_root = Path(__file__).parent.parent.parent
load_dotenv(_project_root / ".env")

CONFIG_DIR = Path(__file__).parent.parent / "config"


def load_cbsa_top50() -> pd.DataFrame:
    """Load the canonical top-50 CBSA reference list."""
    path = CONFIG_DIR / "cbsa_top50.csv"
    df = pd.read_csv(path, dtype={"cbsa_code": str})
    df["cbsa_code"] = df["cbsa_code"].str.zfill(5)
    return df


def get_cbsa_codes() -> list[str]:
    """Return a list of top-50 CBSA codes."""
    df = load_cbsa_top50()
    return df["cbsa_code"].tolist()


def get_sun_belt_codes() -> list[str]:
    """Return CBSA codes classified as Sun Belt."""
    df = load_cbsa_top50()
    return df[df["sun_belt"] == 1]["cbsa_code"].tolist()


def load_pipeline_config() -> dict:
    """Load the pipeline configuration YAML with env var substitution."""
    path = CONFIG_DIR / "pipeline_config.yaml"
    with open(path) as f:
        raw = f.read()

    # Substitute environment variables
    for key in ("CENSUS_API_KEY", "FRED_API_KEY", "HUD_API_KEY"):
        raw = raw.replace(f"${{{key}}}", os.environ.get(key, ""))

    return yaml.safe_load(raw)


def load_scenario_params() -> dict:
    """Load scenario parameter definitions."""
    path = CONFIG_DIR / "scenario_params.yaml"
    with open(path) as f:
        return yaml.safe_load(f)


def filter_to_top50(df: pd.DataFrame, cbsa_col: str = "cbsa_code") -> pd.DataFrame:
    """Filter a DataFrame to only include top-50 CBSAs."""
    codes = set(get_cbsa_codes())
    df[cbsa_col] = df[cbsa_col].astype(str).str.zfill(5)
    return df[df[cbsa_col].isin(codes)].copy()
