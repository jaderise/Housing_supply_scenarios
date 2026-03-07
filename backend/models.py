"""Pydantic response models for the API."""

from typing import Optional

from pydantic import BaseModel


class MetroRef(BaseModel):
    cbsa_code: str
    cbsa_name: str
    region: str
    sun_belt: bool
    population_rank: int


class MetadataResponse(BaseModel):
    last_run: Optional[str] = None
    latest_data_year: Optional[int] = None
    total_metros: int = 0


class NationalTimeseriesResponse(BaseModel):
    years: list[int]
    total_households: list[Optional[int]]
    total_completions: list[Optional[int]]
    total_permits: list[Optional[int]]
    hh_formation_rate: list[Optional[float]]
    cumulative_deficit_baseline: list[Optional[int]]
    mortgage_rate_annual_avg: list[Optional[float]]


class NationalScenarioResponse(BaseModel):
    hh_formation_assumption: str
    demolition_assumption: str
    current_deficit: Optional[int] = None
    end_state_deficit_1yr: Optional[int] = None
    end_state_deficit_2yr: Optional[int] = None
    end_state_deficit_3yr: Optional[int] = None


class MetroSummaryResponse(BaseModel):
    cbsa_code: str
    cbsa_name: str
    years: list[int]
    permits_per_1000: list[Optional[float]]
    permits_vs_national_avg_ratio: list[Optional[float]]
    vacancy_rate: list[Optional[float]]
    vacancy_rate_yoy_change: list[Optional[float]]
    mortgage_pct_median_income: list[Optional[float]]
    fmr_pct_median_income: list[Optional[float]]
    cumulative_deficit_since_2008: list[Optional[int]]
    domestic_migration_per_permit: list[Optional[float]]
    dq_flags: dict[int, str]


class MetroLatestResponse(BaseModel):
    cbsa_code: str
    cbsa_name: str
    latest_year: int
    permits_per_1000_residents: Optional[float] = None
    permits_vs_national_avg_ratio: Optional[float] = None
    vacancy_rate: Optional[float] = None
    vacancy_rate_yoy_change: Optional[float] = None
    mortgage_pct_median_income: Optional[float] = None
    fmr_pct_median_income: Optional[float] = None
    cumulative_deficit_since_2008: Optional[int] = None
    oversupply_signal: str = "yellow"
    dq_flag: Optional[str] = None


class ScenarioResponse(BaseModel):
    cbsa_code: str
    cbsa_name: str
    hh_formation_assumption: str
    demolition_assumption: str
    migration_assumption: str
    horizon_years: int
    current_deficit_baseline: Optional[int] = None
    projected_new_households: Optional[int] = None
    projected_completions: Optional[int] = None
    projected_surplus_deficit: Optional[int] = None
    end_state_deficit: Optional[int] = None
    scenario_label: Optional[str] = None


class InterpretRequest(BaseModel):
    cbsa_code: str
    cbsa_name: str
    scenario_params: dict
    scenario_output: dict
    metro_context: dict
    national_context: dict
    user_question: str


class DQSummaryResponse(BaseModel):
    last_run: Optional[str] = None
    total_warnings: int = 0
    total_errors: int = 0
    cbsas_with_errors: list[dict] = []
    benchmark_status: str = "ok"
