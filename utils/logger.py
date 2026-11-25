"""
Standardized structured logging utility across all pipelines.
Formats logs with ISO timestamp, log level, module name, and message.
"""

import logging
import sys

def get_logger(name: str = "EcommerceDataEngine", level: int = logging.INFO) -> logging.Logger:
    """Return a configured logger with clean console output formatting."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
    return logger
