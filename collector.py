"""
collector.py
============
Data collectors: spot indices, index futures, India VIX, and the FULL
option chain (every strike, every configured expiry).

Architecture
------------
* :class:`InstrumentCache` — resolves instrument tokens from the daily
  scrip master (refreshed once per day). Collectors never guess tokens.
* :class:`BaseCollector` — one `collect()` per minute returning row
  dicts; on total failure emits placeholder rows (``missing=True``) so
  the time series never has silent holes.
* :class:`MarketDataCollector` — orchestrates all collectors for one
  tick and hands rows to the :class:`storage.Storage` abstraction.

Adding a new data source later (market breadth, FII flows, news, order
book…) = subclass :class:`BaseCollector`, register a dataset schema in
storage, append the instance to ``MarketDataCollector.collectors``.
Nothing existing changes.

Field-name notes
----------------
The Neo quote payload is stringly-typed and field names differ across
SDK versions; :func:`utils.get_first` tries known aliases for each
field. Unknown fields simply come back as None — raw capture never
crashes on a schema drift, it records what it can and flags the rest.
"""

from __future__ import annotations

import io
from datetime import datetime
from typing import Any

import pandas as pd

import config
import utils
from config import IndexConfig
from login import NeoSession
from logger import get_logger
from storage import Storage

log = get_logger("collector")


# ---------------------------------------------------------------------------
# Instrument resolution
# ---------------------------------------------------------------------------


class InstrumentCache:
    """
    Loads and caches the scrip master per exchange segment (once per day)
    and resolves tokens for spot indices, futures and full option chains.
    """

    # Common scrip-master column aliases across SDK releases.
    COL_ALIASES: dict[str, tuple[str, ...]] = {
        "token": ("pSymbol", "instrumenttoken", "token", "pscripcode"),
        "symbol": ("pSymbolName", "symbolname", "psymbolname"),
        "trading_symbol": ("pTrdSymbol", "tradingsymbol", "ptrdsymbol"),
        "inst_type": ("pInstType", "instrumenttype", "pinsttype", "instname"),
        "expiry": ("pExpiryDate", "expiry", "pexpirydate", "lexpirydate"),
        "strike": ("dStrikePrice;", "dStrikePrice", "strikeprice", "dstrikeprice"),
        "option_type": ("pOptionType", "optiontype", "poptiontype"),
        "desc": ("pDesc", "pdesc", "description"),
    }

    def __init__(self, session: NeoSession) -> None:
        self._session = session
        self._frames: dict[str, pd.DataFrame] = {}
        self._loaded_on: str | None = None

    # -- loading -----------------------------------------------------------
    def refresh_if_stale(self) -> None:
        """Reload scrip masters if not yet loaded today."""
        today = utils.today_ist().isoformat()
        if self._loaded_on != today:
            self._frames.clear()
            self._loaded_on = today

    def _load_segment(self, segment: str) -> pd.DataFrame:
        """Fetch + normalise one segment's scrip master (cached for the day)."""
        if segment in self._frames:
            return self._frames[segment]

        raw = self._session.scrip_master(exchange_segment=segment)
        df = self._to_dataframe(raw)
        df = self._normalise(df)
        self._frames[segment] = df
        log.info("Scrip master loaded for %s: %d rows", segment, len(df))
        return df

    @staticmethod
    def _to_dataframe(raw: Any) -> pd.DataFrame:
        """The SDK returns either a URL, CSV text, a path, or a DataFrame."""
        if isinstance(raw, pd.DataFrame):
            return raw
        if isinstance(raw, dict):
            for k in ("filesPaths", "data", "message"):
                if k in raw:
                    raw = raw[k]
                    break
        if isinstance(raw, list) and raw:
            raw = raw[0]
        if isinstance(raw, str):
            if raw.startswith("http"):
                return pd.read_csv(raw, low_memory=False)
            if "\n" in raw:  # inline CSV text
                return pd.read_csv(io.StringIO(raw), low_memory=False)
            return pd.read_csv(raw, low_memory=False)  # local path
        raise RuntimeError(f"Unrecognised scrip-master payload: {type(raw)}")

    @classmethod
    def _normalise(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Map vendor column names to canonical names; keep originals too."""
        lower_map = {c.strip().lower(): c for c in df.columns}
        out = df.copy()
        for canon, aliases in cls.COL_ALIASES.items():
            for alias in aliases:
                src = lower_map.get(alias.strip().lower())
                if src is not None:
                    out[canon] = df[src]
                    break
        for col in ("symbol", "trading_symbol", "inst_type", "option_type", "desc"):
            if col in out.columns:
                out[col] = out[col].astype(str).str.strip().str.upper()
        if "strike" in out.columns:
            out["strike"] = pd.to_numeric(out["strike"], errors="coerce")
        return out

    # -- resolution --------------------------------------------------------
    def spot_token(self, idx: IndexConfig) -> dict[str, str] | None:
        """
        Instrument-token dict for a spot index.

        Per the Neo SDK v2 quotes example, index quotes are fetched by
        passing the index's display name directly as `instrument_token`
        (e.g. "Nifty 50", "Nifty Bank") rather than a numeric scrip-master
        token — no scrip-master lookup needed for spot indices.
        """
        return {
            "instrument_token": idx.spot_symbol,
            "exchange_segment": idx.spot_exchange,
        }

    def _derivatives(self, idx: IndexConfig, inst_prefix: str) -> pd.DataFrame:
        """All derivative rows for an underlying (FUT* or OPT*), expiry-sorted."""
        df = self._load_segment(idx.derivative_exchange)
        if "inst_type" not in df.columns or "symbol" not in df.columns:
            return pd.DataFrame()
        sel = df[
            df["inst_type"].str.startswith(inst_prefix, na=False)
            & (df["symbol"] == idx.derivative_symbol.upper())
        ].copy()
        if sel.empty or "expiry" not in sel.columns:
            return sel
        sel["_expiry_dt"] = pd.to_datetime(
            pd.to_numeric(sel["expiry"], errors="coerce"), unit="s", errors="coerce"
        )
        if sel["_expiry_dt"].isna().all():  # expiry stored as a date string
            sel["_expiry_dt"] = pd.to_datetime(sel["expiry"], errors="coerce")
        return sel.sort_values("_expiry_dt")

    def future_instruments(self, idx: IndexConfig) -> list[dict[str, Any]]:
        """Near-month future contract(s) for an index (nearest expiry first)."""
        sel = self._derivatives(idx, "FUT")
        out: list[dict[str, Any]] = []
        for _, row in sel.iterrows():
            out.append({
                "instrument_token": str(row["token"]),
                "exchange_segment": idx.derivative_exchange,
                "expiry": self._expiry_str(row),
                "symbol": str(row.get("trading_symbol", "")),
            })
        return out[:1]  # near-month only; widen the slice for more expiries

    def option_instruments(self, idx: IndexConfig) -> list[dict[str, Any]]:
        """
        The COMPLETE option chain: every strike, both CE and PE, for the
        nearest ``idx.option_expiries`` expiries (0 = all expiries).
        """
        sel = self._derivatives(idx, "OPT")
        if sel.empty:
            log.error("No option instruments found for %s", idx.name)
            return []
        # .unique() on a datetime column returns a pandas DatetimeArray,
        # which has no in-place .sort() (that's an ndarray/list method) —
        # sort via a DatetimeIndex instead.
        expiries = pd.DatetimeIndex(sel["_expiry_dt"].dropna().unique()).sort_values()
        if idx.option_expiries > 0:
            expiries = expiries[: idx.option_expiries]
            sel = sel[sel["_expiry_dt"].isin(expiries)]

        out: list[dict[str, Any]] = []
        for _, row in sel.iterrows():
            out.append({
                "instrument_token": str(row["token"]),
                "exchange_segment": idx.derivative_exchange,
                "expiry": self._expiry_str(row),
                "strike": utils.to_float(row.get("strike")),
                "option_type": str(row.get("option_type", "")),
            })
        return out

    @staticmethod
    def _expiry_str(row: pd.Series) -> str:
        dt = row.get("_expiry_dt")
        if pd.notna(dt):
            return pd.Timestamp(dt).strftime("%Y-%m-%d")
        return str(row.get("expiry", ""))


# ---------------------------------------------------------------------------
# Quote parsing helpers
# ---------------------------------------------------------------------------


def _q(quote: dict[str, Any], *aliases: str) -> Any:
    """Alias-tolerant field lookup, descending into common nested dicts."""
    val = utils.get_first(quote, *aliases)
    if val is not None:
        return val
    for nest in ("ohlc", "depth", "data"):
        inner = quote.get(nest)
        if isinstance(inner, dict):
            val = utils.get_first(inner, *aliases)
            if val is not None:
                return val
    return None


def _best_bid_ask(quote: dict[str, Any]) -> tuple[Any, Any, Any, Any]:
    """(bid, ask, bid_qty, ask_qty) from either flat fields or a depth book."""
    bid = _q(quote, "bid", "best_bid_price", "bp", "buyPrice")
    ask = _q(quote, "ask", "best_ask_price", "sp", "sellPrice")
    bid_qty = _q(quote, "bid_qty", "best_bid_quantity", "bq", "buyQty")
    ask_qty = _q(quote, "ask_qty", "best_ask_quantity", "bs", "sellQty")
    depth = quote.get("depth")
    if isinstance(depth, dict):
        buys, sells = depth.get("buy") or [], depth.get("sell") or []
        if buys and isinstance(buys[0], dict):
            bid = bid or utils.get_first(buys[0], "price")
            bid_qty = bid_qty or utils.get_first(buys[0], "quantity", "qty")
        if sells and isinstance(sells[0], dict):
            ask = ask or utils.get_first(sells[0], "price")
            ask_qty = ask_qty or utils.get_first(sells[0], "quantity", "qty")
    return bid, ask, bid_qty, ask_qty


def _exchange_ts(quote: dict[str, Any]) -> str | None:
    return _q(quote, "exchange_timestamp", "ltt", "last_trade_time",
              "exchange_time", "feed_time", "timestamp")


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------


class BaseCollector:
    """One collector = one dataset for one underlying."""

    dataset: str = ""

    def __init__(self, session: NeoSession, instruments: InstrumentCache) -> None:
        self.session = session
        self.instruments = instruments

    def collect(self, ts: datetime) -> list[dict[str, Any]]:
        """Return the rows for this minute. Must not raise on data problems."""
        raise NotImplementedError

    def placeholder_rows(self, ts: datetime) -> list[dict[str, Any]]:
        """Rows written when the API is down: Missing=True, series continues."""
        raise NotImplementedError

    def write(self, storage: Storage, rows: list[dict[str, Any]]) -> int:
        raise NotImplementedError


class SpotCollector(BaseCollector):
    """Per-minute OHLCV snapshot of one spot index."""

    dataset = "spot"

    def __init__(self, session: NeoSession, instruments: InstrumentCache,
                 idx: IndexConfig) -> None:
        super().__init__(session, instruments)
        self.idx = idx

    @utils.retry()
    def _fetch(self) -> list[dict[str, Any]]:
        # Confirmed-working path (per neogreeks/oi_monitor.py production
        # code): spot indices are fetched by NAME via the raw REST
        # "neosymbol" endpoint, not the SDK's token-based quotes().
        return self.session.quotes_neo_symbol(
            [(self.idx.spot_exchange, self.idx.spot_symbol)], quote_type="all"
        )

    def collect(self, ts: datetime) -> list[dict[str, Any]]:
        quotes = self._fetch()
        rows = []
        for q in quotes:
            rows.append({
                "timestamp": utils.iso_ts(ts),
                "exchange_timestamp": _exchange_ts(q),
                "index": self.idx.name,
                "open": utils.to_float(_q(q, "open", "open_price", "o")),
                "high": utils.to_float(_q(q, "high", "high_price", "h")),
                "low": utils.to_float(_q(q, "low", "low_price", "l")),
                "close": utils.to_float(_q(q, "close", "close_price", "c", "prev_close")),
                "ltp": utils.to_float(_q(q, "ltp", "last_traded_price", "last_price")),
                "volume": utils.to_int(_q(q, "volume", "vol", "total_traded_volume", "vtt")),
                "vwap": utils.to_float(_q(q, "vwap", "average_traded_price", "atp")),
                "average_price": utils.to_float(_q(q, "average_price", "avg_price", "atp")),
                "num_trades": utils.to_int(_q(q, "num_trades", "total_trades", "no_of_trades")),
                "instrument_token": _q(q, "instrument_token", "tk", "token"),
            })
        return rows

    def placeholder_rows(self, ts: datetime) -> list[dict[str, Any]]:
        return [{"timestamp": utils.iso_ts(ts), "index": self.idx.name,
                 "missing": True}]

    def write(self, storage: Storage, rows: list[dict[str, Any]]) -> int:
        return storage.write_spot(self.idx.name, rows)


class FutureCollector(BaseCollector):
    """Per-minute snapshot of the near-month index future."""

    dataset = "future"

    def __init__(self, session: NeoSession, instruments: InstrumentCache,
                 idx: IndexConfig) -> None:
        super().__init__(session, instruments)
        self.idx = idx
        self._prev_oi: dict[str, int] = {}   # token -> last OI (for oi_change)

    @utils.retry()
    def _fetch(self) -> list[tuple[dict[str, Any], dict[str, Any]]]:
        insts = self.instruments.future_instruments(self.idx)
        if not insts:
            raise RuntimeError(f"No future instruments for {self.idx.name}")
        tokens = [{"instrument_token": i["instrument_token"],
                   "exchange_segment": i["exchange_segment"]} for i in insts]
        quotes = self.session.quotes(tokens, quote_type="all")
        by_token = {str(_q(q, "instrument_token", "tk", "token")): q for q in quotes}
        return [(i, by_token.get(i["instrument_token"], {})) for i in insts]

    def collect(self, ts: datetime) -> list[dict[str, Any]]:
        rows = []
        for inst, q in self._fetch():
            token = inst["instrument_token"]
            oi = utils.to_int(_q(q, "oi", "open_interest", "openInterest"))
            oi_change = utils.to_int(_q(q, "oi_change", "change_in_oi", "oiChg"))
            if oi_change is None and oi is not None and token in self._prev_oi:
                oi_change = oi - self._prev_oi[token]
            if oi is not None:
                self._prev_oi[token] = oi
            bid, ask, _, _ = _best_bid_ask(q)
            rows.append({
                "timestamp": utils.iso_ts(ts),
                "exchange_timestamp": _exchange_ts(q),
                "index": self.idx.name,
                "symbol": inst["symbol"],
                "expiry": inst["expiry"],
                "ltp": utils.to_float(_q(q, "ltp", "last_traded_price", "last_price")),
                "open": utils.to_float(_q(q, "open", "open_price", "o")),
                "high": utils.to_float(_q(q, "high", "high_price", "h")),
                "low": utils.to_float(_q(q, "low", "low_price", "l")),
                "close": utils.to_float(_q(q, "close", "close_price", "c", "prev_close")),
                "volume": utils.to_int(_q(q, "volume", "vol", "total_traded_volume", "vtt")),
                "oi": oi,
                "oi_change": oi_change,
                "bid": utils.to_float(bid),
                "ask": utils.to_float(ask),
                "vwap": utils.to_float(_q(q, "vwap", "average_traded_price", "atp")),
                "average_price": utils.to_float(_q(q, "average_price", "avg_price", "atp")),
                "instrument_token": token,
            })
        return rows

    def placeholder_rows(self, ts: datetime) -> list[dict[str, Any]]:
        return [{"timestamp": utils.iso_ts(ts), "index": self.idx.name,
                 "missing": True}]

    def write(self, storage: Storage, rows: list[dict[str, Any]]) -> int:
        return storage.write_future(self.idx.name, rows)


class VixCollector(BaseCollector):
    """Per-minute India VIX snapshot."""

    dataset = "vix"

    @utils.retry()
    def _fetch(self) -> list[dict[str, Any]]:
        return self.session.quotes_neo_symbol(
            [(config.VIX_EXCHANGE, config.VIX_SYMBOL)], quote_type="all"
        )

    def collect(self, ts: datetime) -> list[dict[str, Any]]:
        rows = []
        for q in self._fetch():
            rows.append({
                "timestamp": utils.iso_ts(ts),
                "exchange_timestamp": _exchange_ts(q),
                "open": utils.to_float(_q(q, "open", "open_price", "o")),
                "high": utils.to_float(_q(q, "high", "high_price", "h")),
                "low": utils.to_float(_q(q, "low", "low_price", "l")),
                "close": utils.to_float(_q(q, "close", "close_price", "c", "prev_close")),
                "ltp": utils.to_float(_q(q, "ltp", "last_traded_price", "last_price")),
                "volume": utils.to_int(_q(q, "volume", "vol", "total_traded_volume", "vtt")),
                "vwap": utils.to_float(_q(q, "vwap", "average_traded_price", "atp")),
                "instrument_token": _q(q, "instrument_token", "tk", "token"),
            })
        return rows

    def placeholder_rows(self, ts: datetime) -> list[dict[str, Any]]:
        return [{"timestamp": utils.iso_ts(ts), "missing": True}]

    def write(self, storage: Storage, rows: list[dict[str, Any]]) -> int:
        return storage.write_vix(rows)


class OptionChainCollector(BaseCollector):
    """
    THE core collector: every strike, CE and PE, for the configured
    expiries — every minute. Greeks/IV are recorded when the API supplies
    them; None otherwise (they can be recomputed offline from raw data).
    """

    dataset = "option_chain"

    def __init__(self, session: NeoSession, instruments: InstrumentCache,
                 idx: IndexConfig, spot: SpotCollector) -> None:
        super().__init__(session, instruments)
        self.idx = idx
        self._spot = spot                    # for underlying price
        self._prev_oi: dict[str, int] = {}

    @utils.retry()
    def _fetch(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        insts = self.instruments.option_instruments(self.idx)
        if not insts:
            raise RuntimeError(f"No option instruments for {self.idx.name}")
        tokens = [{"instrument_token": i["instrument_token"],
                   "exchange_segment": i["exchange_segment"]} for i in insts]
        quotes = self.session.quotes(tokens, quote_type="all")
        return insts, quotes

    def collect(self, ts: datetime) -> list[dict[str, Any]]:
        insts, quotes = self._fetch()
        by_token = {str(_q(q, "instrument_token", "tk", "token")): q for q in quotes}

        # Underlying price: reuse this minute's spot fetch (one extra call max).
        underlying: float | None = None
        try:
            spot_rows = self._spot.collect(ts)
            if spot_rows:
                underlying = spot_rows[0].get("ltp") or spot_rows[0].get("close")
        except Exception as exc:  # noqa: BLE001
            log.warning("Underlying price unavailable for %s: %s", self.idx.name, exc)

        rows: list[dict[str, Any]] = []
        ts_str = utils.iso_ts(ts)
        for inst in insts:
            token = inst["instrument_token"]
            q = by_token.get(token, {})
            oi = utils.to_int(_q(q, "oi", "open_interest", "openInterest"))
            oi_change = utils.to_int(_q(q, "oi_change", "change_in_oi", "oiChg"))
            if oi_change is None and oi is not None and token in self._prev_oi:
                oi_change = oi - self._prev_oi[token]
            if oi is not None:
                self._prev_oi[token] = oi
            bid, ask, bid_qty, ask_qty = _best_bid_ask(q)
            rows.append({
                "timestamp": ts_str,
                "exchange_timestamp": _exchange_ts(q),
                "index": self.idx.name,
                "expiry": inst["expiry"],
                "strike": inst["strike"],
                "type": inst["option_type"],
                "ltp": utils.to_float(_q(q, "ltp", "last_traded_price", "last_price")),
                "bid": utils.to_float(bid),
                "ask": utils.to_float(ask),
                "bid_qty": utils.to_int(bid_qty),
                "ask_qty": utils.to_int(ask_qty),
                "last_qty": utils.to_int(_q(q, "last_qty", "last_traded_quantity", "ltq")),
                "volume": utils.to_int(_q(q, "volume", "vol", "total_traded_volume", "vtt")),
                "oi": oi,
                "oi_change": oi_change,
                "iv": utils.to_float(_q(q, "iv", "implied_volatility", "impliedVolatility")),
                "delta": utils.to_float(_q(q, "delta")),
                "gamma": utils.to_float(_q(q, "gamma")),
                "theta": utils.to_float(_q(q, "theta")),
                "vega": utils.to_float(_q(q, "vega")),
                "underlying_price": underlying,
                "instrument_token": token,
                "missing": not bool(q),   # instrument had no quote this minute
            })
        return rows

    def placeholder_rows(self, ts: datetime) -> list[dict[str, Any]]:
        return [{"timestamp": utils.iso_ts(ts), "index": self.idx.name,
                 "missing": True}]

    def write(self, storage: Storage, rows: list[dict[str, Any]]) -> int:
        return storage.write_option_chain(self.idx.name, rows)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


class MarketDataCollector:
    """Runs every collector once per tick and accumulates daily stats."""

    def __init__(self, session: NeoSession, storage: Storage) -> None:
        self.session = session
        self.storage = storage
        self.instruments = InstrumentCache(session)

        self.collectors: list[BaseCollector] = []
        for idx in config.INDICES:
            spot = SpotCollector(session, self.instruments, idx)
            self.collectors.append(spot)
            if idx.collect_futures:
                self.collectors.append(FutureCollector(session, self.instruments, idx))
            if idx.collect_options:
                self.collectors.append(
                    OptionChainCollector(session, self.instruments, idx, spot)
                )
        self.collectors.append(VixCollector(session, self.instruments))

        # Daily statistics for metadata.json
        self.total_records: int = 0
        self.missing_count: int = 0
        self.skipped_minutes: int = 0

    def reset_daily_stats(self) -> None:
        self.total_records = 0
        self.missing_count = 0
        self.skipped_minutes = 0

    def collect_tick(self, ts: datetime) -> None:
        """
        One full collection cycle. Each collector fails independently;
        an API outage produces placeholder rows, never a stopped system.
        """
        self.instruments.refresh_if_stale()
        for coll in self.collectors:
            name = f"{coll.dataset}:{getattr(getattr(coll, 'idx', None), 'name', 'vix')}"
            try:
                rows = coll.collect(ts)
            except Exception as exc:  # noqa: BLE001 — degrade, never die
                log.error("%s failed after retries: %s — writing placeholder", name, exc)
                rows = coll.placeholder_rows(ts)
                self.missing_count += 1
            try:
                written = coll.write(self.storage, rows)
                self.total_records += written
                log.debug("%s: wrote %d rows", name, written)
            except Exception as exc:  # noqa: BLE001
                # A storage failure is serious but must not kill other datasets.
                log.critical("%s: STORAGE WRITE FAILED: %s", name, exc)
