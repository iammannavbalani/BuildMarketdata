"""
api.py
======
FastAPI service exposing the collected data to the Firebase-hosted
frontend. Designed for a single Railway service that also runs the
collector loop in a background thread (Railway volumes mount to one
service only, so collector + API share the process and the volume).

Endpoints
---------
GET /health                          liveness probe (Railway healthcheck)
GET /status                          today's metadata + live collector state
GET /days                            available data days (from data/YYYY/MM/DD)
GET /days/{date}/files               files + sizes for one day
GET /days/{date}/files/{name}        download a CSV / metadata.json
GET /days/{date}/files/{name}/tail   last N rows as JSON (dashboard tables)

Security: set the API_KEY env var to require an ``X-API-Key`` header on
every endpoint except /health. CORS origins come from API_CORS_ORIGINS.

Run locally:  uvicorn api:app --reload
On Railway:   uvicorn api:app --host 0.0.0.0 --port $PORT
"""

from __future__ import annotations

import csv
import json
import re
import threading
from collections import deque
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

import config
import utils
from logger import get_logger

log = get_logger("api")

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_FILE_RE = re.compile(r"^[A-Za-z0-9_.-]+$")  # blocks path traversal

# ---------------------------------------------------------------------------
# Background collector (optional, controlled by RUN_COLLECTOR_IN_API)
# ---------------------------------------------------------------------------
_collector_thread: threading.Thread | None = None
_scheduler: Any = None  # CollectionScheduler; imported lazily


def _start_collector() -> None:
    """Launch the collection loop in a daemon thread."""
    global _collector_thread, _scheduler
    from scheduler import CollectionScheduler  # lazy: keeps API importable alone

    _scheduler = CollectionScheduler()
    _collector_thread = threading.Thread(
        target=_scheduler.run, name="collector", daemon=True
    )
    _collector_thread.start()
    log.info("Collector loop started in background thread.")


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    if config.RUN_COLLECTOR_IN_API:
        _start_collector()
    yield
    if _scheduler is not None:
        log.info("API shutting down — stopping collector.")
        _scheduler.stop()
        if _collector_thread is not None:
            _collector_thread.join(timeout=30)


app = FastAPI(title="MarketData Collector API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.API_CORS_ORIGINS,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """Enforce X-API-Key when API_KEY is configured; open otherwise."""
    if config.API_KEY and x_api_key != config.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def _day_dir(day: str) -> Path:
    """Validated data directory for a YYYY-MM-DD day string."""
    if not _DATE_RE.match(day):
        raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD")
    y, m, d = day.split("-")
    p = config.DATA_DIR / y / m / d
    if not p.is_dir():
        raise HTTPException(status_code=404, detail=f"No data for {day}")
    return p


def _safe_file(day_dir: Path, name: str) -> Path:
    """Validated file path inside a day directory (no traversal)."""
    if not _FILE_RE.match(name):
        raise HTTPException(status_code=400, detail="Invalid file name")
    p = day_dir / name
    if not p.is_file():
        raise HTTPException(status_code=404, detail=f"File not found: {name}")
    return p


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health() -> dict[str, Any]:
    """Liveness probe — no auth so Railway healthchecks work out of the box."""
    return {"ok": True, "time": utils.iso_ts(), "timezone": str(config.TIMEZONE)}


@app.get("/status", dependencies=[Depends(require_api_key)])
def status() -> dict[str, Any]:
    """Today's metadata.json plus live process state for the dashboard."""
    today = utils.today_ist()
    meta_path = (
        config.DATA_DIR / f"{today.year:04d}" / f"{today.month:02d}"
        / f"{today.day:02d}" / config.METADATA_FILE
    )
    meta: dict[str, Any] = {}
    if meta_path.is_file():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            log.warning("metadata.json unreadable at %s", meta_path)

    return {
        "server_time": utils.iso_ts(),
        "in_trading_session": utils.in_trading_session(),
        "is_trading_day": utils.is_trading_day(),
        "collector_running": bool(
            _collector_thread and _collector_thread.is_alive()
        ),
        "collection_interval_seconds": config.COLLECTION_INTERVAL,
        "session_window": f"{config.SESSION_START:%H:%M}-{config.SESSION_END:%H:%M}",
        "storage_backend": config.STORAGE_BACKEND,
        "metadata": meta,
    }


@app.get("/days", dependencies=[Depends(require_api_key)])
def list_days(limit: int = 60) -> dict[str, Any]:
    """Available data days, newest first (walks data/YYYY/MM/DD)."""
    days: list[str] = []
    if config.DATA_DIR.is_dir():
        for y in sorted(config.DATA_DIR.iterdir(), reverse=True):
            if not (y.is_dir() and y.name.isdigit()):
                continue
            for m in sorted(y.iterdir(), reverse=True):
                if not (m.is_dir() and m.name.isdigit()):
                    continue
                for d in sorted(m.iterdir(), reverse=True):
                    if d.is_dir() and d.name.isdigit() and any(d.iterdir()):
                        days.append(f"{y.name}-{m.name}-{d.name}")
                        if len(days) >= limit:
                            return {"days": days}
    return {"days": days}


@app.get("/days/{day}/files", dependencies=[Depends(require_api_key)])
def list_files(day: str) -> dict[str, Any]:
    """Files and sizes for one day."""
    d = _day_dir(day)
    files = [
        {
            "name": f.name,
            "size_bytes": f.stat().st_size,
            "modified": datetime.fromtimestamp(
                f.stat().st_mtime, tz=config.TIMEZONE
            ).strftime("%Y-%m-%d %H:%M:%S"),
        }
        for f in sorted(d.iterdir())
        if f.is_file()
    ]
    return {"day": day, "files": files}


@app.get("/days/{day}/files/{name}", dependencies=[Depends(require_api_key)])
def download_file(day: str, name: str) -> FileResponse:
    """Download a raw file (CSV or metadata.json)."""
    path = _safe_file(_day_dir(day), name)
    media = "application/json" if path.suffix == ".json" else "text/csv"
    return FileResponse(path, media_type=media, filename=path.name)


@app.get("/days/{day}/files/{name}/tail", dependencies=[Depends(require_api_key)])
def tail_file(day: str, name: str, n: int = 100) -> dict[str, Any]:
    """
    Last `n` CSV rows as JSON records — powers dashboard tables without
    shipping multi-hundred-MB files. Bounded memory via deque.
    """
    path = _safe_file(_day_dir(day), name)
    if path.suffix != ".csv":
        raise HTTPException(status_code=400, detail="tail supports CSV files only")
    n = max(1, min(n, 5000))
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = deque(reader, maxlen=n)
    return {"day": day, "file": name, "rows": list(rows), "count": len(rows)}


def _today_str() -> str:
    return utils.today_ist().isoformat()


@app.get("/latest/{name}", dependencies=[Depends(require_api_key)])
def latest_tail(name: str, n: int = 100) -> dict[str, Any]:
    """Convenience: tail of today's file (e.g. /latest/nifty_spot.csv)."""
    return tail_file(_today_str(), name, n)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api:app", host=config.API_HOST, port=config.API_PORT)
