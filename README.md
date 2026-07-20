# MarketData — Kotak Neo Raw Data Collector

Production-grade minute-level raw market data capture for quantitative
research and ML. Collects spot indices, near-month futures, India VIX,
and the **complete option chain** (every strike, CE+PE, configurable
expiries) and appends to daily CSV files. No indicators, no filtering —
raw capture only.

## Layout

```
MarketData/
  config.py        # ALL settings: credentials (env vars), instruments, intervals
  login.py         # Kotak Neo session: login, 2FA, auto re-login/reconnect
  collector.py     # Instrument resolution + spot/future/vix/option collectors
  storage.py       # Abstract Storage + CSVStorage (swap for DuckDB/Postgres later)
  scheduler.py     # Entrypoint: session windows, minute ticks, daily rotation
  logger.py        # Daily-rotating logs -> logs/YYYY-MM-DD.log
  utils.py         # IST time helpers, retry/backoff, coercion, validation
  data/YYYY/MM/DD/ # nifty_spot.csv, nifty_future.csv, nifty_option_chain.csv,
                   # india_vix.csv, metadata.json (+ other indices)
  logs/            # one log per day
  archive/         # compress old months here (not automated yet)
```

## Setup

```bash
python -m venv .venv && .venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

Set credentials as environment variables (never commit them):

```
KOTAK_CONSUMER_KEY, KOTAK_CONSUMER_SECRET, KOTAK_MOBILE,
KOTAK_PASSWORD, KOTAK_MPIN, KOTAK_ENV (prod|uat)
```

## Run

```bash
python scheduler.py
```

The process idles outside 09:14–15:31 IST, logs in ~60 s before open,
collects every `COLLECTION_INTERVAL` (default 60 s) on aligned minute
boundaries, writes a final snapshot after close, and rotates
folder/logs/counters at midnight. Ctrl-C shuts down cleanly with a
final flush.

## Reliability guarantees

- Per-request retries with jittered exponential backoff.
- Auto re-login on session expiry; reconnects counted in metadata.
- If all retries fail: a `missing=True` placeholder row is written and
  collection continues — the time series never silently gaps.
- Every batch is flushed to disk immediately; append-only, never overwrite.
- Validation (duplicate timestamps, negative OI, absurd IV/prices) flags
  rows via `valid`/`issues` columns — raw data is never dropped.
- Skipped minutes (outage/host sleep) are detected and logged.

## Swapping storage backends

Collectors only call the `Storage` interface (`write_spot`,
`write_future`, `write_option_chain`, `write_vix`, `write_rows`).
To move to DuckDB/SQLite/Postgres/ClickHouse: subclass `Storage`,
register it in `build_storage()` in `storage.py`, set
`STORAGE_BACKEND=duckdb`. No collector code changes.

## Extending with new datasets

Market breadth, FII flows, news, economic calendar, order book:
subclass `BaseCollector`, add a schema entry in `storage.py`
(`CSVStorage.DATASETS`), append the collector in
`MarketDataCollector.__init__`. Existing collectors are untouched.

## Notes / caveats

- **Greeks/IV**: recorded when the Neo quote payload supplies them;
  otherwise stored as empty and can be recomputed offline from the raw
  chain (LTP, strike, expiry, underlying are always captured).
- **SENSEX** is configured but disabled by default (`config.INDICES`);
  enable `collect_futures/collect_options` if your subscription covers BSE.
- **Holiday calendar** is not built in — weekdays are assumed trading
  days; on holidays the API returns stale/empty quotes which are flagged.
- Field names in Neo API payloads vary across SDK versions; parsing is
  alias-tolerant (see `collector.py`). Verify one day of output before
  relying on any column.
