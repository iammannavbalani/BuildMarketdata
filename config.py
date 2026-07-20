"""
config.py
=========
Central configuration for the MarketData collection system.

Everything tunable lives here: credentials, instruments, collection
interval, trading-session windows, paths, retry policy, validation
bounds. No other module hardcodes values.

SECURITY
--------
Credentials are read from environment variables first and fall back to
the placeholders below. NEVER commit real credentials. Prefer a `.env`
file (git-ignored) loaded by your shell, or OS-level env vars:

    set KOTAK_CONSUMER_KEY=...        (Windows)
    export KOTAK_CONSUMER_KEY=...     (Linux/macOS)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import time
from pathlib import Path
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR: Path = Path(__file__).resolve().parent
# DATA_ROOT lets deployments point storage at a mounted volume
# (e.g. Railway volume at /data) without code changes.
_DATA_ROOT: Path = Path(os.getenv("DATA_ROOT", str(BASE_DIR)))
DATA_DIR: Path = _DATA_ROOT / "data"
LOG_DIR: Path = _DATA_ROOT / "logs"
ARCHIVE_DIR: Path = _DATA_ROOT / "archive"

# ---------------------------------------------------------------------------
# Timezone — every timestamp in this system is Asia/Kolkata.
# ---------------------------------------------------------------------------
TIMEZONE = ZoneInfo("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Credentials (env-var first, placeholder fallback)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Credentials:
    """
    Kotak Neo API credentials. Populate via environment variables.

    Matches neo_api_client SDK v2.0.x's TOTP login flow:
      NeoAPI(consumer_key=...)
      client.totp_login(mobile_number=, ucc=, totp=<generated each call>)
      client.totp_validate(mpin=)
    consumer_key is the single application token from the Neo web/app
    "Trade API" card (v2.0.0 dropped consumer_secret/password entirely).
    """

    consumer_key: str = os.getenv("KOTAK_CONSUMER_KEY", "YOUR_CONSUMER_KEY")
    mobile_number: str = os.getenv("KOTAK_MOBILE", "+91XXXXXXXXXX")
    ucc: str = os.getenv("KOTAK_UCC", "YOUR_UCC")
    mpin: str = os.getenv("KOTAK_MPIN", "")
    totp_secret: str = os.getenv("KOTAK_TOTP_SECRET", "")
    # "prod" or "uat"
    environment: str = os.getenv("KOTAK_ENV", "prod")
    # neo_api_client version string, recorded into metadata.json
    api_version: str = os.getenv("KOTAK_API_VERSION", "neo_api_client")


CREDENTIALS = Credentials()

# ---------------------------------------------------------------------------
# Collection settings
# ---------------------------------------------------------------------------
COLLECTION_INTERVAL: int = int(os.getenv("COLLECTION_INTERVAL", "60"))  # seconds

# Trading session (IST). Collection starts/stops automatically.
SESSION_START: time = time(9, 14)   # start collecting just before open
SESSION_END: time = time(15, 31)    # final snapshot just after close

# Weekdays on which the market trades (Mon=0 ... Sun=6)
TRADING_WEEKDAYS: frozenset[int] = frozenset({0, 1, 2, 3, 4})

# ---------------------------------------------------------------------------
# Retry / reconnect policy
# ---------------------------------------------------------------------------
MAX_RETRIES: int = 3                 # per-request retries within a minute
RETRY_BACKOFF_BASE: float = 1.5      # seconds; exponential backoff base
RETRY_BACKOFF_MAX: float = 10.0      # cap on a single backoff sleep
RELOGIN_ON_AUTH_ERROR: bool = True   # re-login automatically on 401/session expiry
MAX_RELOGIN_ATTEMPTS: int = 5        # consecutive re-login attempts before pausing
RELOGIN_PAUSE_SECONDS: float = 30.0  # pause between re-login bursts

# Requests to the quote API are chunked (option chains are large).
QUOTE_BATCH_SIZE: int = 50           # instrument tokens per quote request
REQUEST_TIMEOUT: float = 15.0        # seconds

# ---------------------------------------------------------------------------
# Instruments
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IndexConfig:
    """Configuration for one index we track."""

    name: str                 # canonical name used in filenames
    spot_symbol: str          # symbol of the spot index in the scrip master
    spot_exchange: str        # exchange segment of the spot index
    derivative_symbol: str    # underlying symbol for futures/options
    derivative_exchange: str  # exchange segment for derivatives
    collect_futures: bool = True
    collect_options: bool = True
    # How many nearest expiries of the option chain to collect.
    # 0 means "all available expiries".
    option_expiries: int = 2
    strike_step: int = 50     # spacing between listed strikes for this index


# Number of strikes to collect on each side of the ATM strike (ATM itself
# always included), per expiry — e.g. 5 means 11 strikes total per expiry
# (5 ITM + ATM + 5 OTM). Set to 0 to capture the entire available chain.
# Recomputed every tick from the live underlying price, so the window
# tracks the market instead of staying fixed to one strike ladder.
OPTION_STRIKE_WINDOW: int = int(os.getenv("OPTION_STRIKE_WINDOW", "5"))


INDICES: tuple[IndexConfig, ...] = (
    IndexConfig("nifty", "Nifty 50", "nse_cm", "NIFTY", "nse_fo", strike_step=50),
    IndexConfig("banknifty", "Nifty Bank", "nse_cm", "BANKNIFTY", "nse_fo", strike_step=100),
    # FINNIFTY and MIDCPNIFTY removed from collection (2026-07-20) to cut
    # per-cycle API call volume and bring real collection cadence back
    # toward the configured 60s interval. Re-add here if needed later:
    #   IndexConfig("finnifty", "Nifty Fin Service", "nse_cm", "FINNIFTY", "nse_fo", strike_step=50),
    #   IndexConfig("midcpnifty", "NIFTY MID SELECT", "nse_cm", "MIDCPNIFTY", "nse_fo", strike_step=25),
    # SENSEX: spot + options (bse_fo) — options confirmed working with this
    # account per the neogreeks project. Futures stay off.
    IndexConfig(
        "sensex", "SENSEX", "bse_cm", "SENSEX", "bse_fo",
        collect_futures=False, collect_options=True, strike_step=100,
    ),
)

# India VIX (spot-style index, no derivatives collected here).
# Exact casing "INDIA VIX" matches the confirmed-working neosymbol query
# from the neogreeks/oi_monitor.py production code.
VIX_SYMBOL: str = "INDIA VIX"
VIX_EXCHANGE: str = "nse_cm"

# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------
# Backend name — the storage factory maps this to a Storage implementation.
# Supported today: "csv". Future: "duckdb", "sqlite", "postgres", "clickhouse".
STORAGE_BACKEND: str = os.getenv("STORAGE_BACKEND", "csv")

# Flush the CSV file handle after every write batch (never lose data).
FLUSH_EVERY_WRITE: bool = True

# File names inside each daily folder
SPOT_FILE_TMPL: str = "{index}_spot.csv"
FUTURE_FILE_TMPL: str = "{index}_future.csv"
OPTION_FILE_TMPL: str = "{index}_option_chain.csv"
VIX_FILE: str = "india_vix.csv"
METADATA_FILE: str = "metadata.json"

# ---------------------------------------------------------------------------
# Validation bounds — rows outside these are flagged, not dropped.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ValidationConfig:
    max_iv: float = 500.0        # % — IV above this is treated as corrupt
    min_iv: float = 0.0
    min_oi: int = 0              # negative OI is invalid
    min_price: float = 0.0       # negative prices are invalid
    max_price: float = 1_000_000.0
    allow_duplicate_timestamps: bool = False


VALIDATION = ValidationConfig()

# ---------------------------------------------------------------------------
# API server (FastAPI on Railway)
# ---------------------------------------------------------------------------
API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
API_PORT: int = int(os.getenv("PORT", "8000"))  # Railway injects PORT
# Comma-separated allowed origins for CORS (your Firebase Hosting URLs).
# "*" is fine while prototyping; lock down before real use.
API_CORS_ORIGINS: list[str] = [
    o.strip() for o in os.getenv("API_CORS_ORIGINS", "*").split(",") if o.strip()
]
# Optional shared-secret: if set, requests must send X-API-Key header.
API_KEY: str = os.getenv("API_KEY", "")
# Start the collector loop inside the API process (single Railway service
# sharing one volume). Set "0" to run the API alone.
RUN_COLLECTOR_IN_API: bool = os.getenv("RUN_COLLECTOR_IN_API", "1") == "1"

# ---------------------------------------------------------------------------
# Google Drive daily backup (optional)
# ---------------------------------------------------------------------------
# After each trading day finalizes (midnight rollover), the day's CSVs +
# metadata.json are uploaded to a Google Drive folder via a service
# account — Railway's volume stays the source of truth, Drive is a
# visible, syncable copy.
DRIVE_BACKUP_ENABLED: bool = os.getenv("DRIVE_BACKUP_ENABLED", "0") == "1"
# Full service-account JSON key content (paste the whole file as one
# env var value) — NOT a file path, since Railway has no persistent
# secret-file storage outside the volume.
GOOGLE_SERVICE_ACCOUNT_JSON: str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
# Target Drive folder ID (share this folder with the service account's
# client_email as Editor). Each trading day gets a "YYYY-MM-DD" subfolder.
GOOGLE_DRIVE_FOLDER_ID: str = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
