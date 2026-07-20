"""
scheduler.py
============
Application entrypoint and main loop.

Responsibilities
----------------
* Sleep until the trading session opens (09:14 IST), log in ahead of it.
* Fire one collection tick aligned to every minute boundary during the
  session (interval configurable via ``COLLECTION_INTERVAL``).
* Take a final snapshot at/after session end (15:31 IST).
* Write ``metadata.json`` after every tick (crash-safe running totals).
* Rotate storage folder + log file at midnight IST.
* Track skipped minutes (host slept / long outage) for metadata.
* Shut down cleanly on Ctrl-C / SIGTERM without losing buffered rows.

Run:
    python scheduler.py
"""

from __future__ import annotations

import signal
import sys
import threading
import time
from datetime import date, datetime, timedelta
from types import FrameType

import config
import utils
from collector import MarketDataCollector
from drive_backup import DriveBackup
from login import NeoSession
from logger import get_logger, rotate_log_file
from storage import Storage, build_storage

log = get_logger("scheduler")


class CollectionScheduler:
    """Owns the run loop, daily rotation and graceful shutdown."""

    def __init__(self) -> None:
        self.session = NeoSession()
        self.storage: Storage = build_storage()
        self.collector = MarketDataCollector(self.session, self.storage)
        self.drive = DriveBackup()
        self._running = True
        self._current_day: date = utils.today_ist()
        self._last_tick: datetime | None = None
        self._session_open_ts: str | None = None
        self._session_close_ts: str | None = None

        # Signal handlers can only be installed from the main thread; when
        # the scheduler runs inside the API process (background thread on
        # Railway), shutdown is driven by stop() instead.
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)

    def stop(self) -> None:
        """Request a clean shutdown (thread-safe)."""
        self._running = False

    # ------------------------------------------------------------------ #
    # Signals / shutdown
    # ------------------------------------------------------------------ #
    def _handle_signal(self, signum: int, _frame: FrameType | None) -> None:
        log.info("Signal %d received — shutting down after current tick.", signum)
        self._running = False

    # ------------------------------------------------------------------ #
    # Metadata
    # ------------------------------------------------------------------ #
    def _write_metadata(self) -> None:
        """Persist running daily totals so a crash never loses bookkeeping."""
        meta = {
            "date": self._current_day.isoformat(),
            "market_open_time": self._session_open_ts,
            "market_close_time": self._session_close_ts,
            "session_start_config": config.SESSION_START.strftime("%H:%M"),
            "session_end_config": config.SESSION_END.strftime("%H:%M"),
            "collection_interval_seconds": config.COLLECTION_INTERVAL,
            "api_version": config.CREDENTIALS.api_version,
            "timezone": str(config.TIMEZONE),
            "missing_data_count": self.collector.missing_count,
            "reconnect_count": self.session.reconnect_count,
            "total_requests": self.session.total_requests,
            "total_records": self.collector.total_records,
            "skipped_minutes": self.collector.skipped_minutes,
            "storage_backend": config.STORAGE_BACKEND,
            "last_updated": utils.iso_ts(),
        }
        try:
            self.storage.write_metadata(meta)
        except Exception as exc:  # noqa: BLE001
            log.error("Failed to write metadata.json: %s", exc)

    # ------------------------------------------------------------------ #
    # Daily rotation
    # ------------------------------------------------------------------ #
    def _rotate_if_new_day(self) -> None:
        """At midnight IST: new data folder, new log file, fresh counters."""
        today = utils.today_ist()
        if today == self._current_day:
            return
        log.info("Midnight rollover: %s -> %s", self._current_day, today)
        self._write_metadata()                     # finalise yesterday
        if self.drive.enabled:
            finished_dir = config.DATA_DIR / utils.daily_folder(self._current_day)
            if finished_dir.is_dir():
                self.drive.backup_day(finished_dir, self._current_day.isoformat())
        self.storage.rotate_day(today)
        rotate_log_file()
        self.collector.reset_daily_stats()
        self.session.reconnect_count = 0
        self.session.total_requests = 0
        self._session_open_ts = None
        self._session_close_ts = None
        self._current_day = today

    # ------------------------------------------------------------------ #
    # Tick timing
    # ------------------------------------------------------------------ #
    @staticmethod
    def _next_boundary(after: datetime) -> datetime:
        """Next COLLECTION_INTERVAL-aligned instant strictly after `after`."""
        interval = config.COLLECTION_INTERVAL
        epoch = after.timestamp()
        return datetime.fromtimestamp(
            (int(epoch // interval) + 1) * interval, tz=config.TIMEZONE
        )

    def _track_skipped(self, now: datetime) -> None:
        """Count minute slots lost between ticks (outage / host sleep)."""
        if self._last_tick is not None:
            gap = (now - self._last_tick).total_seconds()
            missed = int(gap // config.COLLECTION_INTERVAL) - 1
            if missed > 0:
                self.collector.skipped_minutes += missed
                log.warning("Skipped %d collection slot(s) between %s and %s",
                            missed, utils.iso_ts(self._last_tick), utils.iso_ts(now))
        self._last_tick = now

    # ------------------------------------------------------------------ #
    # Phases
    # ------------------------------------------------------------------ #
    def _sleep_until_session(self) -> None:
        """Idle (interruptibly) until the next session start; pre-login 60s early."""
        start = utils.next_session_start()
        log.info("Next session starts at %s — sleeping.", utils.iso_ts(start))
        while self._running:
            now = utils.now_ist()
            self._rotate_if_new_day()
            remaining = (start - now).total_seconds()
            if remaining <= 60:
                break
            time.sleep(min(remaining - 60, 30))
        if self._running:
            try:
                self.session.ensure_login()   # authenticated before the bell
            except Exception as exc:  # noqa: BLE001
                log.error("Pre-session login failed: %s (will retry in-session)", exc)
            while self._running and utils.now_ist() < start:
                time.sleep(1)

    def _run_session(self) -> None:
        """Collect every interval from session start through session end."""
        log.info("Session started — collecting every %ds.", config.COLLECTION_INTERVAL)
        if self._session_open_ts is None:
            self._session_open_ts = utils.iso_ts()

        while self._running and utils.in_trading_session():
            tick_ts = utils.now_ist()
            self._track_skipped(tick_ts)
            t0 = time.monotonic()
            self.collector.collect_tick(tick_ts)
            self._write_metadata()
            elapsed = time.monotonic() - t0
            if elapsed > config.COLLECTION_INTERVAL:
                log.warning("Tick took %.1fs (> interval %ds) — next tick immediate",
                            elapsed, config.COLLECTION_INTERVAL)
            # Sleep to the next aligned boundary, waking early on shutdown.
            target = self._next_boundary(utils.now_ist())
            while self._running and utils.now_ist() < target:
                time.sleep(min(1.0, (target - utils.now_ist()).total_seconds() + 0.01))

        if self._running:  # natural session end → final snapshot
            log.info("Session ended — taking final snapshot.")
            final_ts = utils.now_ist()
            self.collector.collect_tick(final_ts)
            self._session_close_ts = utils.iso_ts(final_ts)
            self._write_metadata()

    # ------------------------------------------------------------------ #
    # Main loop
    # ------------------------------------------------------------------ #
    def run(self) -> None:
        log.info("MarketData collector starting (backend=%s, interval=%ds)",
                 config.STORAGE_BACKEND, config.COLLECTION_INTERVAL)
        try:
            while self._running:
                self._rotate_if_new_day()
                if utils.in_trading_session():
                    self._run_session()
                else:
                    self._sleep_until_session()
        finally:
            log.info("Shutting down — flushing storage.")
            self._write_metadata()
            self.storage.close()
            log.info("Shutdown complete. No data lost.")


def main() -> int:
    CollectionScheduler().run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
