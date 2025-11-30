# source/logging_config.py

import os
import sys

from loguru import logger


def configure_logging() -> None:
    """
    Global logging configuration for the project.

    Responsibilities
    ----------------
    - Ensure logs directory exists
    - Remove default loguru handlers
    - Add console handler
    - Add file handler for pipeline logs
    """
    # --- 1) Pastikan folder logs/ ada ---
    os.makedirs("logs", exist_ok=True)

    # --- 2) Bersihkan handler default loguru ---
    logger.remove()

    # --- 3) Console handler (stdout) ---
    logger.add(
        sys.stdout,
        level="INFO",
    )

    # --- 4) File handler utama untuk pipeline ---
    logger.add(
        "logs/pipeline.log",
        level="DEBUG",
        rotation="5 MB",
        retention="7 days",
        compression="zip",
        enqueue=True,
    )
