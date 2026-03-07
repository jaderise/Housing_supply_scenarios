"""Pipeline logging utility. Creates structured logs for each pipeline run."""

import logging
import os
from datetime import datetime
from pathlib import Path


def get_pipeline_logger(
    name: str = "pipeline",
    log_dir: str = "./data/logs",
    level: int = logging.INFO,
) -> logging.Logger:
    """Create a logger that writes to both console and a timestamped log file."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"pipeline_{timestamp}.log")

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def log_stage_start(logger: logging.Logger, stage: str) -> None:
    logger.info(f"[STAGE START] {stage}")


def log_stage_end(logger: logging.Logger, stage: str, status: str, details: str = "") -> None:
    logger.info(f"[STAGE END] {stage} | status={status} | {details}")


def log_source_fetch(
    logger: logging.Logger,
    source: str,
    status: str,
    files_written: int = 0,
    rows_fetched: int = 0,
    elapsed: float = 0,
) -> None:
    logger.info(
        f"[FETCH] source={source} | status={status} | "
        f"files={files_written} | rows={rows_fetched} | elapsed={elapsed:.1f}s"
    )


def log_transform(
    logger: logging.Logger,
    source: str,
    rows_in: int,
    rows_out: int,
    rows_flagged: int = 0,
    rows_excluded: int = 0,
) -> None:
    logger.info(
        f"[TRANSFORM] source={source} | rows_in={rows_in} | rows_out={rows_out} | "
        f"flagged={rows_flagged} | excluded={rows_excluded}"
    )
