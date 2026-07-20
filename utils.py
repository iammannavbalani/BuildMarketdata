"""
utils.py
========
Shared helpers: IST time functions, trading-session checks, retry
decorator with exponential backoff, safe type coercion, and row
validation used by the storage layer before anything hits disk.
"""

from __future__ import annotations

import random
import time as time_mod
from collections.abc import Callable
from datetime import date, datetime, timedelta
from functools import wraps
from typing import Any, TypeVar

import config
from logger import get_logger

log = get_logger("utils")

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Time helpers — everything is Asia/Kolkata.
# ---------------------------------------------------------------------------


def now_ist() -> datetime:
    """Current timezone-aware datetime in Asia/Kolkata."""
    return datetime.now(config.TIMEZONE)


def today_ist() -> date:
    return now_ist().date()


def iso_ts(dt: datetime | None = None) -> str:
    """ISO-8601 timestamp string (seconds precision) in IST."""
    return (dt or now_ist()).strftime("%Y-%m-%d %H:%M:%S")


def is_trading_day(d: date | None = None) -> bool:
    """True on configured trading weekdays (holiday calendar can be added)."""
    d = d or today_ist()
    return d.weekday() in config.TRADING_WEEKDAYS


def in_trading_session(dt: datetime | None = None) -> bool:
    """True when `dt` falls inside [SESSION_START, SESSION_END] on a trading day."""
    dt = dt or now_ist()
    if not is_trading_day(dt.date()):
        return False
    return config.SESSION_START <= dt.time() <= config.SESSION_END


def next_session_start(dt: datetime | None = None) -> datetime:
    """Datetime of the next session start at or after `dt`."""
    dt = dt or now_ist()
    candidate = dt.replace(
        hour=config.SESSION_START.hour,
        minute=config.SESSION_START.minute,
        second=0,
        microsecond=0,
    )
    if dt.time() > config.SESSION_END or not is_trading_day(dt.date()):
        candidate += timedelta(days=1)
    while not is_trading_day(candidate.date()):
        candidate += timedelta(days=1)
    return candidate


def daily_folder(base: date | None = None) -> str:
    """Relative daily path 'YYYY/MM/DD' used under data/."""
    d = base or today_ist()
    return f"{d.year:04d}/{d.month:02d}/{d.day:02d}"


# ---------------------------------------------------------------------------
# Retry decorator
# ---------------------------------------------------------------------------


def retry(
    max_attempts: int = config.MAX_RETRIES,
    backoff_base: float = config.RETRY_BACKOFF_BASE,
    backoff_max: float = config.RETRY_BACKOFF_MAX,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Retry a function with jittered exponential backoff.

    Raises the final exception after `max_attempts` so callers can decide
    how to degrade (e.g. write a Missing=True placeholder row).
    """

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exc: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:  # noqa: PERF203
                    last_exc = exc
                    if attempt == max_attempts:
                        break
                    sleep_for = min(
                        backoff_max, backoff_base ** attempt + random.uniform(0, 0.5)
                    )
                    log.warning(
                        "%s failed (attempt %d/%d): %s — retrying in %.1fs",
                        fn.__name__, attempt, max_attempts, exc, sleep_for,
                    )
                    time_mod.sleep(sleep_for)
            assert last_exc is not None
            raise last_exc

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Safe coercion — API payloads are stringly-typed and inconsistent.
# ---------------------------------------------------------------------------


def to_float(value: Any, default: float | None = None) -> float | None:
    """Best-effort float conversion; returns `default` on failure."""
    if value is None or value == "" or value == "-":
        return default
    try:
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return default


def to_int(value: Any, default: int | None = None) -> int | None:
    """Best-effort int conversion; returns `default` on failure."""
    f = to_float(value)
    return int(f) if f is not None else default


def get_first(d: dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Return the first present, non-empty key from `d` (API field aliases)."""
    for k in keys:
        if k in d and d[k] not in (None, "", "-"):
            return d[k]
    return default


# ---------------------------------------------------------------------------
# Row validation — flag, never silently drop.
# ---------------------------------------------------------------------------


def validate_row(row: dict[str, Any], kind: str) -> list[str]:
    """
    Validate a row dict before writing. Returns a list of issue strings
    (empty list = clean). Issues are logged and recorded in metadata but
    the row is still written with its `valid` flag set accordingly —
    raw-data capture must never discard information.
    """
    issues: list[str] = []
    v = config.VALIDATION

    for price_field in ("ltp", "open", "high", "low", "close", "bid", "ask"):
        val = row.get(price_field)
        if val is not None and isinstance(val, (int, float)):
            if val < v.min_price or val > v.max_price:
                issues.append(f"invalid_{price_field}={val}")

    oi = row.get("oi")
    if oi is not None and isinstance(oi, (int, float)) and oi < v.min_oi:
        issues.append(f"invalid_oi={oi}")

    if kind == "option":
        iv = row.get("iv")
        if iv is not None and isinstance(iv, (int, float)):
            if iv < v.min_iv or iv > v.max_iv:
                issues.append(f"invalid_iv={iv}")

    return issues
