"""
drive_backup.py
================
Optional daily backup of collected data to Google Drive.

Railway's volume remains the source of truth for the live collector;
this module gives you a second, human-browsable copy in Drive without
making Drive part of the collection hot path (uploads happen once per
day, after a trading day's files are finalized — never per-tick).

Setup
-----
1. Google Cloud Console → create a project (or reuse one) → enable the
   "Google Drive API".
2. IAM & Admin → Service Accounts → create one → Keys → Add key → JSON.
   Download the key file.
3. In Drive, create (or pick) a folder for backups, open its share
   dialog, and add the service account's `client_email` (from the JSON)
   as Editor.
4. Set on Railway:
     DRIVE_BACKUP_ENABLED=1
     GOOGLE_SERVICE_ACCOUNT_JSON=<paste the ENTIRE key file content>
     GOOGLE_DRIVE_FOLDER_ID=<the folder's ID from its Drive URL>

Failure handling: a Drive outage must never affect data collection.
Every method here catches and logs; callers (scheduler.py) treat backup
as best-effort and proceed regardless of the outcome.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import config
from logger import get_logger

log = get_logger("drive_backup")

_FOLDER_MIME = "application/vnd.google-apps.folder"


class DriveBackup:
    """Uploads one day's data folder to a Drive folder, idempotently."""

    def __init__(self) -> None:
        self._service: Any = None
        if config.DRIVE_BACKUP_ENABLED:
            self._init_service()

    def _init_service(self) -> None:
        """Build an authenticated Drive API client from the service-account JSON."""
        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build

            if not config.GOOGLE_SERVICE_ACCOUNT_JSON or not config.GOOGLE_DRIVE_FOLDER_ID:
                log.warning(
                    "DRIVE_BACKUP_ENABLED=1 but GOOGLE_SERVICE_ACCOUNT_JSON or "
                    "GOOGLE_DRIVE_FOLDER_ID is missing — backup disabled."
                )
                return

            info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_JSON)
            creds = service_account.Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/drive.file"]
            )
            self._service = build("drive", "v3", credentials=creds, cache_discovery=False)
            log.info("Google Drive backup initialised.")
        except Exception as exc:  # noqa: BLE001 — backup must never crash the app
            log.error("Drive backup init failed (backup disabled): %s", exc)
            self._service = None

    @property
    def enabled(self) -> bool:
        return self._service is not None

    def _find_or_create_subfolder(self, name: str) -> str | None:
        """Return the Drive folder ID for `name` under the root backup folder."""
        assert self._service is not None
        q = (
            f"name = '{name}' and mimeType = '{_FOLDER_MIME}' and "
            f"'{config.GOOGLE_DRIVE_FOLDER_ID}' in parents and trashed = false"
        )
        resp = self._service.files().list(q=q, fields="files(id)").execute()
        files = resp.get("files", [])
        if files:
            return files[0]["id"]

        meta = {
            "name": name,
            "mimeType": _FOLDER_MIME,
            "parents": [config.GOOGLE_DRIVE_FOLDER_ID],
        }
        created = self._service.files().create(body=meta, fields="id").execute()
        return created["id"]

    def _upload_or_replace(self, parent_id: str, path: Path) -> None:
        """Upload one file, overwriting a same-named file if already backed up."""
        from googleapiclient.http import MediaFileUpload

        assert self._service is not None
        q = (
            f"name = '{path.name}' and '{parent_id}' in parents and trashed = false"
        )
        existing = self._service.files().list(q=q, fields="files(id)").execute().get("files", [])
        media = MediaFileUpload(str(path), resumable=True)

        if existing:
            self._service.files().update(fileId=existing[0]["id"], media_body=media).execute()
        else:
            meta = {"name": path.name, "parents": [parent_id]}
            self._service.files().create(body=meta, media_body=media, fields="id").execute()

    def backup_day(self, day_dir: Path, day_str: str) -> int:
        """
        Upload every file in `day_dir` (one trading day's CSVs + metadata)
        into a Drive subfolder named `day_str` (YYYY-MM-DD). Returns the
        number of files uploaded; 0 and a logged error on any failure —
        never raises, since backup is best-effort.
        """
        if not self.enabled:
            return 0
        try:
            folder_id = self._find_or_create_subfolder(day_str)
            if folder_id is None:
                return 0
            count = 0
            for f in sorted(day_dir.iterdir()):
                if f.is_file():
                    self._upload_or_replace(folder_id, f)
                    count += 1
            log.info("Drive backup: uploaded %d file(s) for %s", count, day_str)
            return count
        except Exception as exc:  # noqa: BLE001
            log.error("Drive backup failed for %s: %s", day_str, exc)
            return 0
