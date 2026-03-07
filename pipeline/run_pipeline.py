"""
Master pipeline orchestrator. Runs all four stages in sequence.
Usage: python -m pipeline.run_pipeline [--stage {fetch,transform,load,calculate,all}]
"""

import argparse
import logging
import sys
import time
from datetime import datetime

from pipeline.utils.cbsa_utils import load_pipeline_config
from pipeline.utils.logger import get_pipeline_logger, log_stage_start, log_stage_end
from pipeline.utils.dq_checks import ensure_dq_log_table


def run_fetch(config, logger):
    """Run Stage 1: Fetch raw data from APIs."""
    from pipeline.fetch import fetch_census_permits
    from pipeline.fetch import fetch_census_population
    from pipeline.fetch import fetch_census_acs
    from pipeline.fetch import fetch_hud_vacancy
    from pipeline.fetch import fetch_hud_fmr
    from pipeline.fetch import fetch_fred

    log_stage_start(logger, "FETCH")
    results = {}
    critical_failed = False

    fetchers = [
        ("census_permits", fetch_census_permits),
        ("census_population", fetch_census_population),
        ("census_acs", fetch_census_acs),
        ("hud_vacancy", fetch_hud_vacancy),
        ("hud_fmr", fetch_hud_fmr),
        ("fred", fetch_fred),
    ]

    critical_sources = {"census_permits", "census_population"}

    for name, module in fetchers:
        try:
            start = time.time()
            result = module.run(config)
            elapsed = time.time() - start
            result["elapsed"] = elapsed
            results[name] = result
            logger.info(f"  {name}: {result.get('status', 'UNKNOWN')} ({elapsed:.1f}s)")

            if result.get("status") == "FAILED" and name in critical_sources:
                critical_failed = True
                logger.error(f"  CRITICAL source {name} failed - pipeline will halt")
        except Exception as e:
            logger.error(f"  {name}: EXCEPTION - {e}")
            results[name] = {"status": "FAILED", "error": str(e)}
            if name in critical_sources:
                critical_failed = True

    status = "FAILED" if critical_failed else "SUCCESS"
    log_stage_end(logger, "FETCH", status, f"{len(results)} sources processed")
    return status, results


def run_transform(config, logger):
    """Run Stage 2: Transform raw data to processed CSVs."""
    from pipeline.transform import transform_permits
    from pipeline.transform import transform_population
    from pipeline.transform import transform_acs
    from pipeline.transform import transform_vacancy
    from pipeline.transform import transform_fmr
    from pipeline.transform import transform_fred

    log_stage_start(logger, "TRANSFORM")
    results = {}
    has_errors = False
    error_rate_threshold = 0.05

    transformers = [
        ("permits", transform_permits),
        ("population", transform_population),
        ("acs", transform_acs),
        ("vacancy", transform_vacancy),
        ("fmr", transform_fmr),
        ("fred", transform_fred),
    ]

    for name, module in transformers:
        try:
            result = module.run(config)
            results[name] = result
            logger.info(
                f"  {name}: {result.get('status', 'UNKNOWN')} "
                f"(in={result.get('rows_in', 0)}, out={result.get('rows_out', 0)}, "
                f"flagged={result.get('rows_flagged', 0)})"
            )

            rows_out = result.get("rows_out", 0)
            rows_flagged = result.get("rows_flagged", 0)
            if rows_out > 0 and rows_flagged / rows_out > error_rate_threshold:
                logger.error(f"  {name}: error rate {rows_flagged/rows_out:.1%} exceeds threshold")
                has_errors = True
        except Exception as e:
            logger.error(f"  {name}: EXCEPTION - {e}")
            results[name] = {"status": "FAILED", "error": str(e)}
            has_errors = True

    status = "WARNING" if has_errors else "SUCCESS"
    log_stage_end(logger, "TRANSFORM", status)
    return status, results


def run_load(config, logger):
    """Run Stage 3: Load processed CSVs into SQLite."""
    from pipeline.load import load_to_sqlite

    log_stage_start(logger, "LOAD")
    try:
        result = load_to_sqlite.run(config)
        logger.info(f"  Loaded {result.get('tables_loaded', 0)} tables, {result.get('total_rows', 0)} rows")
        log_stage_end(logger, "LOAD", result.get("status", "SUCCESS"))
        return result.get("status", "SUCCESS"), result
    except Exception as e:
        logger.error(f"  LOAD EXCEPTION: {e}")
        log_stage_end(logger, "LOAD", "FAILED", str(e))
        return "FAILED", {"status": "FAILED", "error": str(e)}


def run_calculate(config, logger):
    """Run Stage 4: Calculate metrics and scenario grid."""
    from pipeline.calculate import calculate_metrics
    from pipeline.calculate import calculate_scenarios

    log_stage_start(logger, "CALCULATE")
    try:
        metrics_result = calculate_metrics.run(config)
        logger.info(f"  Metrics: {metrics_result.get('rows_written', 0)} rows written")

        scenario_result = calculate_scenarios.run(config)
        logger.info(f"  Scenarios: {scenario_result.get('rows_written', 0)} rows written")

        status = "SUCCESS"
        log_stage_end(logger, "CALCULATE", status)
        return status, {"metrics": metrics_result, "scenarios": scenario_result}
    except Exception as e:
        logger.error(f"  CALCULATE EXCEPTION: {e}")
        log_stage_end(logger, "CALCULATE", "FAILED", str(e))
        return "FAILED", {"status": "FAILED", "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Housing data pipeline orchestrator")
    parser.add_argument(
        "--stage",
        choices=["fetch", "transform", "load", "calculate", "all"],
        default="all",
        help="Which stage to run (default: all)",
    )
    args = parser.parse_args()

    config = load_pipeline_config()
    logger = get_pipeline_logger(log_dir=config["data_paths"]["logs"])

    # Ensure dq_log table exists
    ensure_dq_log_table(config["data_paths"]["db"])

    logger.info(f"[RUN START] stage={args.stage} timestamp={datetime.now().isoformat()}")
    start_time = time.time()

    stages = {
        "fetch": run_fetch,
        "transform": run_transform,
        "load": run_load,
        "calculate": run_calculate,
    }

    if args.stage == "all":
        run_stages = ["fetch", "transform", "load", "calculate"]
    else:
        run_stages = [args.stage]

    overall_status = "SUCCESS"
    all_results = {}

    for stage_name in run_stages:
        status, result = stages[stage_name](config, logger)
        all_results[stage_name] = result

        if status == "FAILED":
            overall_status = "FAILED"
            logger.error(f"Stage {stage_name} FAILED — halting pipeline")
            break
        elif status == "WARNING" and overall_status == "SUCCESS":
            overall_status = "WARNING"

    elapsed = time.time() - start_time
    logger.info(
        f"[RUN END] status={overall_status} elapsed={elapsed:.1f}s"
    )

    exit_codes = {"SUCCESS": 0, "WARNING": 1, "FAILED": 2}
    sys.exit(exit_codes.get(overall_status, 2))


if __name__ == "__main__":
    main()
