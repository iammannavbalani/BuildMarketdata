"""
login.py
========
Kotak Neo API session management.

Wraps ``neo_api_client.NeoAPI`` behind :class:`NeoSession` so the rest
of the application never talks to the SDK directly. Responsibilities:

* Interactive-free login (mobile + password, then MPIN as 2FA;
  falls back to prompting for an OTP if MPIN 2FA is rejected).
* Thread-safe quote fetching with batching.
* Automatic re-login when the session expires or the API returns
  authentication errors — collectors just retry and keep going.
* Reconnect accounting for metadata.json.

NOTE: field/method names in the Neo SDK have changed between releases.
Everything SDK-specific is confined to this file so an SDK upgrade is a
one-file fix.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import config
from logger import get_logger

log = get_logger("login")


class AuthenticationError(Exception):
    """Raised when the API rejects our session and re-login also failed."""


class NeoSession:
    """Managed Kotak Neo API session with automatic reconnect."""

    # Substrings in error payloads that indicate an expired/invalid session.
    _AUTH_ERROR_MARKERS = (
        "invalid token", "token expired", "session expired", "unauthorized",
        "not authorized", "invalid session", "401", "invalid jwt",
    )

    def __init__(self) -> None:
        self._client: Any = None
        self._lock = threading.Lock()
        self.reconnect_count: int = 0
        self.total_requests: int = 0

    # ------------------------------------------------------------------ #
    # Login / reconnect
    # ------------------------------------------------------------------ #
    def _generate_totp_code(self) -> str:
        """
        Generate this instant's 6-digit TOTP code from KOTAK_TOTP_SECRET
        (the base32 key shown once when linking an authenticator app
        during Kotak's TOTP registration). Regenerated on every login/
        reconnect so there is nothing stale to store.
        """
        creds = config.CREDENTIALS
        if not creds.totp_secret:
            raise AuthenticationError(
                "KOTAK_TOTP_SECRET is not set — required for totp_login()."
            )
        import pyotp  # imported lazily: optional dep unless TOTP is used

        return pyotp.TOTP(creds.totp_secret).now()

    def login(self) -> None:
        """
        Perform the SDK v2 two-step TOTP login. Raises on hard failure.

        Matches neo_api_client v2.0.x:
            NeoAPI(environment=, access_token=None, neo_fin_key=None, consumer_key=)
            client.totp_login(mobile_number=, ucc=, totp=)   # freshly generated code
            client.totp_validate(mpin=)                      # static MPIN
        """
        from neo_api_client import NeoAPI  # imported lazily: SDK optional at test time

        creds = config.CREDENTIALS
        log.info("Logging in to Kotak Neo API (%s)…", creds.environment)

        self._client = NeoAPI(
            environment=creds.environment,
            access_token=None,
            neo_fin_key=None,
            consumer_key=creds.consumer_key,
        )

        # Step 1: TOTP login — identifies the account and starts the session.
        step1 = self._client.totp_login(
            mobile_number=creds.mobile_number,
            ucc=creds.ucc,
            totp=self._generate_totp_code(),
        )
        if isinstance(step1, dict) and step1.get("error"):
            raise AuthenticationError(f"totp_login failed: {step1['error']}")
        log.debug("totp_login response: %s", step1)

        # Step 2: TOTP validate with the account MPIN — issues the trade token.
        if not creds.mpin:
            raise AuthenticationError("KOTAK_MPIN is not set — required for totp_validate().")
        step2 = self._client.totp_validate(mpin=creds.mpin)
        if isinstance(step2, dict) and step2.get("error"):
            raise AuthenticationError(f"totp_validate failed: {step2['error']}")
        log.info("Login successful.")

    def ensure_login(self) -> None:
        """Login if we have no live client (idempotent)."""
        with self._lock:
            if self._client is None:
                self._login_with_retries()

    def _login_with_retries(self) -> None:
        """Attempt login up to MAX_RELOGIN_ATTEMPTS with pauses."""
        last_exc: Exception | None = None
        for attempt in range(1, config.MAX_RELOGIN_ATTEMPTS + 1):
            try:
                self.login()
                return
            except Exception as exc:  # noqa: BLE001 — SDK raises bare Exceptions
                last_exc = exc
                log.error("Login attempt %d/%d failed: %s",
                          attempt, config.MAX_RELOGIN_ATTEMPTS, exc)
                time.sleep(config.RELOGIN_PAUSE_SECONDS)
        raise AuthenticationError(f"Login failed after retries: {last_exc}")

    def reconnect(self) -> None:
        """Drop the current client and log in again (counts as a reconnect)."""
        log.warning("Reconnecting to Kotak Neo API…")
        with self._lock:
            self._client = None
            self._login_with_retries()
            self.reconnect_count += 1
        log.info("Reconnected (total reconnects today: %d)", self.reconnect_count)

    # ------------------------------------------------------------------ #
    # API calls
    # ------------------------------------------------------------------ #
    def _looks_like_auth_error(self, payload: Any) -> bool:
        text = str(payload).lower()
        return any(marker in text for marker in self._AUTH_ERROR_MARKERS)

    def _call(self, fn_name: str, /, **kwargs: Any) -> Any:
        """
        Invoke an SDK method with auth-error detection. On auth failure,
        re-login once and repeat the call.
        """
        self.ensure_login()
        self.total_requests += 1
        try:
            result = getattr(self._client, fn_name)(**kwargs)
        except Exception as exc:  # noqa: BLE001
            if config.RELOGIN_ON_AUTH_ERROR and self._looks_like_auth_error(exc):
                self.reconnect()
                self.total_requests += 1
                result = getattr(self._client, fn_name)(**kwargs)
            else:
                raise

        if isinstance(result, dict) and result.get("error") \
                and self._looks_like_auth_error(result):
            self.reconnect()
            self.total_requests += 1
            result = getattr(self._client, fn_name)(**kwargs)
        return result

    def quotes(
        self,
        instrument_tokens: list[dict[str, str]],
        quote_type: str = "all",
        is_index: bool = False,  # noqa: ARG002 — kept for call-site compatibility;
                                 # SDK v2's quotes() takes no isIndex param. Index
                                 # quotes work by passing the index name itself
                                 # (e.g. "Nifty 50") as instrument_token.
    ) -> list[dict[str, Any]]:
        """
        Fetch full quotes for a list of instruments, transparently batched
        (option chains routinely exceed per-request limits).

        `instrument_tokens` items: {"instrument_token": str, "exchange_segment": str}
        Returns a flat list of quote dicts (SDK 'message'/'data' unwrapped).
        """
        out: list[dict[str, Any]] = []
        batch = config.QUOTE_BATCH_SIZE
        for i in range(0, len(instrument_tokens), batch):
            chunk = instrument_tokens[i : i + batch]
            resp = self._call(
                "quotes",
                instrument_tokens=chunk,
                quote_type=quote_type,
            )
            out.extend(self._unwrap_quotes(resp))
        return out

    @staticmethod
    def _unwrap_quotes(resp: Any) -> list[dict[str, Any]]:
        """Normalise SDK response shapes to a plain list of quote dicts."""
        if resp is None:
            return []
        if isinstance(resp, list):
            return [r for r in resp if isinstance(r, dict)]
        if isinstance(resp, dict):
            for key in ("message", "data", "quotes"):
                inner = resp.get(key)
                if isinstance(inner, list):
                    return [r for r in inner if isinstance(r, dict)]
                if isinstance(inner, dict):
                    return [inner]
            if resp.get("error"):
                raise RuntimeError(f"Quote API error: {resp['error']}")
            return [resp]
        return []

    def scrip_master(self, exchange_segment: str) -> Any:
        """Return the scrip-master for an exchange segment (URL, path or data)."""
        return self._call("scrip_master", exchange_segment=exchange_segment)

    def search_scrip(self, **kwargs: Any) -> Any:
        """Pass-through to the SDK's search_scrip (used as a token fallback)."""
        return self._call("search_scrip", **kwargs)
