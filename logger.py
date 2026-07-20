"""
logger.py
=========
Daily-rotating file + console logging for the whole application.

Every module obtains its logger via :func:`get_logger`. A single file
handler writes to ``logs/YYYY-MM-DD.log`` (IST date). At midnight the
scheduler calls :func:`rotate_log_file` which swaps the handler to the
new day's file — no external log-rotation tooling required.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

import config

_FILE_HANDLER: logging.FileHandler | None = None
_CURRENT_LOG_DATE: str | None = None


def _log_path_for_today() -> Path:
    """Return logs/YYYY-MM-DD.log using the configured (IST) timezone."""
    today = datetime.now(config.TIMEZONE).strftime("%Y-%m-%d")
    return config.LOG_DIR / f"{today}.log"


def _build_file_handler() -> logging.FileHandler:
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(_log_path_for_today(), encoding="utf-8")
    handler.setFormatter(
        logging.Formatter(config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT)
    )
    return handler


def setup_logging() -> None:
    """Configure the root logger once, at application start."""
    global _FILE_HANDLER, _CURRENT_LOG_DATE

    root = logging.getLogger()
    if _FILE_HANDLER is not None:  # already configured
        return

    root.setLevel(config.LOG_LEVEL)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(
        logging.Formatter(config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT)
    )
    root.addHandler(console)

    _FILE_HANDLER = _build_file_handler()
    root.addHandler(_FILE_HANDLER)
    _CURRENT_LOG_DATE = datetime.now(config.TIMEZONE).strftime("%Y-%m-%d")


def rotate_log_file() -> None:
    """Swap the file handler to today's log file (called after midnight)."""
    global _FILE_HANDLER, _CURRENT_LOG_DATE

    today = datetime.now(config.TIMEZONE).strftime("%Y-%m-%d")
    if today == _CURRENT_LOG_DATE:
        return  # nothing to do

    root = logging.getLogger()
    if _FILE_HANDLER is not None:
        root.removeHandler(_FILE_HANDLER)
        _FILE_HANDLER.close()

    _FILE_HANDLER = _build_file_handler()
    root.addHandler(_FILE_HANDLER)
    _CURRENT_LOG_DATE = today
    root.info("Log rotated to %s", _log_path_for_today().name)


def get_logger(name: str) -> logging.Logger:
    """Return a namespaced logger; ensures logging is initialised."""
    setup_logging()
    return logging.getLogger(name)
