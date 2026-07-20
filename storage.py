"""
storage.py
==========
Storage abstraction layer.

Collectors never touch files directly — they call the abstract
:class:`Storage` interface. Today the only implementation is
:class:`CSVStorage` (append-only daily CSV files); swapping in DuckDB,
SQLite, PostgreSQL or ClickHouse later only requires a new subclass and
one line in :func:`build_storage`. Collector code never changes.

CSVStorage design
-----------------
* One folder per day: ``data/YYYY/MM/DD/``.
* Append-only. Files are opened once, kept open, and flushed after
  every batch (``FLUSH_EVERY_WRITE``) so a crash loses at most the rows
  of the current batch. Never overwrites.
* Header written exactly once per file.
* Duplicate-timestamp guard per (file, key) without re-reading files.
* Rows are plain dicts → ``csv.DictWriter`` — no per-minute DataFrame
  construction, O(1) memory regardless of file size.
"""

from __future__ import annotations

import csv
import json
import threading
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Any, IO

import config
import utils
from logger import get_logger

log = get_logger("storage")

# ---------------------------------------------------------------------------
# Column schemas — the single source of truth for on-disk layout.
# ---------------------------------------------------------------------------

SPOT_COLUMNS: list[str] = [
    "timestamp", "exchange_timestamp", "index", "open", "high", "low",
    "close", "ltp", "volume", "vwap", "average_price", "num_trades",
    "instrument_token", "missing", "valid", "issues",
]

FUTURE_COLUMNS: list[str] = [
    "timestamp", "exchange_timestamp", "index", "symbol", "expiry", "ltp",
    "open", "high", "low", "close", "volume", "oi", "oi_change", "bid",
    "ask", "vwap", "average_price", "instrument_token", "missing",
    "valid", "issues",
]

OPTION_COLUMNS: list[str] = [
    "timestamp", "exchange_timestamp", "index", "expiry", "strike", "type",
    "ltp", "bid", "ask", "bid_qty", "ask_qty", "last_qty", "volume", "oi",
    "oi_change", "iv", "delta", "gamma", "theta", "vega",
    "underlying_price", "instrument_token", "missing", "valid", "issues",
]

VIX_COLUMNS: list[str] = [
    "timestamp", "exchange_timestamp", "open", "high", "low", "close",
    "ltp", "volume", "vwap", "instrument_token", "missing", "valid",
    "issues",
]


class Storage(ABC):
    """
    Abstract storage backend.

    Adding a new *dataset* later (market breadth, FII flows, news,
    order book, ...) does not require touching existing collectors:
    implement :meth:`write_rows` for a new dataset name and register a
    schema — the generic path handles everything.
    """

    # -- generic -----------------------------------------------------------
    @abstractmethod
    def write_rows(self, dataset: str, key: str, rows: list[dict[str, Any]]) -> int:
        """Append `rows` to `dataset` partitioned by `key`. Returns rows written."""

    @abstractmethod
    def write_metadata(self, meta: dict[str, Any]) -> None:
        """Persist the daily metadata document."""

    @abstractmethod
    def rotate_day(self, new_day: date) -> None:
        """Close current day's outputs and prepare the next day's."""

    @abstractmethod
    def close(self) -> None:
        """Flush and close everything (shutdown)."""

    # -- typed convenience wrappers (what collectors call) -----------------
    def write_spot(self, index: str, rows: list[dict[str, Any]]) -> int:
        return self.write_rows("spot", index, rows)

    def write_future(self, index: str, rows: list[dict[str, Any]]) -> int:
        return self.write_rows("future", index, rows)

    def write_option_chain(self, index: str, rows: list[dict[str, Any]]) -> int:
        return self.write_rows("option_chain", index, rows)

    def write_vix(self, rows: list[dict[str, Any]]) -> int:
        return self.write_rows("vix", "india_vix", rows)


class CSVStorage(Storage):
    """Append-only daily CSV files with persistent handles and safe flushes."""

    #: dataset -> (filename template, column schema)
    DATASETS: dict[str, tuple[str, list[str]]] = {
        "spot": (config.SPOT_FILE_TMPL, SPOT_COLUMNS),
        "future": (config.FUTURE_FILE_TMPL, FUTURE_COLUMNS),
        "option_chain": (config.OPTION_FILE_TMPL, OPTION_COLUMNS),
        "vix": ("{index}.csv", VIX_COLUMNS),
    }

    def __init__(self, base_dir: Path | None = None) -> None:
        self._base = base_dir or config.DATA_DIR
        self._day: date = utils.today_ist()
        self._lock = threading.Lock()
        # (dataset, key) -> (file handle, DictWriter)
        self._handles: dict[tuple[str, str], tuple[IO[str], csv.DictWriter]] = {}
        # (dataset, key) -> last written timestamp string (duplicate guard)
        self._last_ts: dict[tuple[str, str], str] = {}

    # -- internals ---------------------------------------------------------
    def _day_dir(self) -> Path:
        d = self._base / utils.daily_folder(self._day)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _get_writer(self, dataset: str, key: str) -> tuple[IO[str], csv.DictWriter]:
        """Open (or reuse) the day's file for (dataset, key); write header once."""
        hkey = (dataset, key)
        if hkey in self._handles:
            return self._handles[hkey]

        tmpl, columns = self.DATASETS[dataset]
        path = self._day_dir() / tmpl.format(index=key)
        is_new = not path.exists() or path.stat().st_size == 0

        fh = path.open("a", newline="", encoding="utf-8")
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        if is_new:
            writer.writeheader()
            fh.flush()
        self._handles[hkey] = (fh, writer)
        return fh, writer

    # -- Storage interface -------------------------------------------------
    def write_rows(self, dataset: str, key: str, rows: list[dict[str, Any]]) -> int:
        """Validate, dedupe and append rows; flush so data survives crashes."""
        if not rows:
            return 0
        if dataset not in self.DATASETS:
            raise ValueError(f"Unknown dataset '{dataset}'. Register it in DATASETS.")

        kind = "option" if dataset == "option_chain" else dataset
        written = 0
        with self._lock:
            fh, writer = self._get_writer(dataset, key)
            batch_ts = rows[0].get("timestamp", "")

            # Duplicate-timestamp guard: a whole batch shares one collection
            # timestamp; if it equals the previous batch we would double-write.
            if (
                not config.VALIDATION.allow_duplicate_timestamps
                and dataset in ("spot", "future", "vix")
                and self._last_ts.get((dataset, key)) == batch_ts
            ):
                log.warning("Duplicate timestamp %s for %s/%s — batch skipped",
                            batch_ts, dataset, key)
                return 0

            for row in rows:
                issues = utils.validate_row(row, kind)
                row.setdefault("missing", False)
                row["valid"] = not issues
                row["issues"] = ";".join(issues)
                if issues:
                    log.warning("Validation issues %s/%s @ %s: %s",
                                dataset, key, row.get("timestamp"), row["issues"])
                writer.writerow(row)
                written += 1

            self._last_ts[(dataset, key)] = batch_ts
            if config.FLUSH_EVERY_WRITE:
                fh.flush()
        return written

    def write_metadata(self, meta: dict[str, Any]) -> None:
        """Write metadata.json into the daily folder (atomic replace)."""
        path = self._day_dir() / config.METADATA_FILE
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
        tmp.replace(path)

    def rotate_day(self, new_day: date) -> None:
        """Close all handles; next write lazily opens files under the new date."""
        with self._lock:
            for fh, _ in self._handles.values():
                try:
                    fh.flush()
                    fh.close()
                except OSError as exc:
                    log.error("Error closing file during rotation: %s", exc)
            self._handles.clear()
            self._last_ts.clear()
            self._day = new_day
        log.info("Storage rotated to daily folder %s", utils.daily_folder(new_day))

    def close(self) -> None:
        self.rotate_day(self._day)


def build_storage(backend: str | None = None) -> Storage:
    """
    Storage factory. Extend with new backends without touching collectors:

        if backend == "duckdb":
            from duckdb_storage import DuckDBStorage
            return DuckDBStorage(...)
    """
    backend = backend or config.STORAGE_BACKEND
    if backend == "csv":
        return CSVStorage()
    raise ValueError(f"Unsupported storage backend: {backend!r}")
