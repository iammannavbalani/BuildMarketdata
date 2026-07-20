# Deployment — Railway (backend) + Firebase Hosting (frontend)

## Architecture

```
Firebase Hosting (static dashboard)
        │  HTTPS fetch (CORS + optional X-API-Key)
        ▼
Railway service ── uvicorn api:app
        ├─ FastAPI  (/health /status /days /days/.../tail, downloads)
        ├─ Collector loop (background thread, RUN_COLLECTOR_IN_API=1)
        └─ Railway Volume mounted at /data  (DATA_ROOT=/data)
              └─ data/YYYY/MM/DD/*.csv, logs/, archive/
```

One Railway service runs both API and collector because a Railway volume
can only attach to a single service. `stop()`-based shutdown keeps the
final flush guarantee.

## Backend on Railway

1. Push the `MarketData/` folder to a GitHub repo (Dockerfile +
   railway.json are picked up automatically).
2. Railway → New Project → Deploy from GitHub repo.
3. **Add a Volume**: service → Settings → Volumes → mount path `/data`.
   Size for ~1 GB/month of option-chain CSVs; start with 5 GB.
4. **Variables** (service → Variables):

   | Variable | Value |
   |---|---|
   | `KOTAK_CONSUMER_KEY` | Neo app/web → Invest tab → Trade API card → Generate application → copy the token |
   | `KOTAK_MOBILE` | registered mobile number |
   | `KOTAK_UCC` | your Kotak trading account UCC |
   | `KOTAK_TOTP_SECRET` | base32 secret from your Neo authenticator-app setup — the 2FA code is generated from this automatically on every login/reconnect |
   | `KOTAK_ENV` | `prod` |
   | `DATA_ROOT` | `/data` (already set in Dockerfile) |
   | `API_KEY` | long random string — protects your data endpoints |
   | `API_CORS_ORIGINS` | `https://YOUR_PROJECT.web.app,https://YOUR_PROJECT.firebaseapp.com` |
   | `RUN_COLLECTOR_IN_API` | `1` |
   | `COLLECTION_INTERVAL` | `60` (optional override) |

5. Deploy. `/health` is the healthcheck; logs show login + tick activity.
6. Note the public URL, e.g. `https://marketdata-production.up.railway.app`.

## Google Drive daily backup (optional)

Railway's volume is the live source of truth; this adds a once-a-day
copy of each finished trading day's CSVs to a Drive folder you own —
uploads happen at midnight rollover, never during collection, so a
Drive hiccup can't affect data capture.

1. Google Cloud Console → create/select a project → enable the
   **Google Drive API**.
2. IAM & Admin → Service Accounts → Create → Keys → Add key → JSON.
   Download the key file.
3. In Google Drive, create a folder for backups → Share → add the
   service account's `client_email` (inside the JSON) as **Editor**.
4. Copy the folder's ID from its Drive URL
   (`drive.google.com/drive/folders/<THIS_PART>`).
5. Railway → Variables, add:

   | Variable | Value |
   |---|---|
   | `DRIVE_BACKUP_ENABLED` | `1` |
   | `GOOGLE_SERVICE_ACCOUNT_JSON` | paste the **entire** downloaded JSON file content |
   | `GOOGLE_DRIVE_FOLDER_ID` | the folder ID from step 4 |

6. Redeploy. After the first trading day completes (midnight IST),
   check the Drive folder for a new `YYYY-MM-DD` subfolder containing
   that day's CSVs + `metadata.json`.

Re-running backup for the same day (e.g. after a restart) overwrites
same-named files in place rather than duplicating them.

### Railway notes

- Server TZ is set to Asia/Kolkata in the image; all app logic is
  timezone-aware anyway.
- Keep `numReplicas: 1` — two collectors would double-write.
- Redeploys restart the process; the volume persists, and appends resume
  in the same daily files. Placeholder/skipped-minute accounting covers
  the restart gap.
- Watch volume usage; move old months to `archive/` or download and
  clear them periodically (a cron/APScheduler job can automate this).

## Frontend on Firebase Hosting

```bash
cd MarketData/frontend
npm i -g firebase-tools
firebase login
# create a project at console.firebase.google.com, then:
#   edit .firebaserc → your project id
firebase deploy --only hosting
```

Open the hosting URL, paste the Railway base URL and your `API_KEY`
into the top bar, Connect. Settings persist in localStorage.

The dashboard shows: collector running / in-session, records, missing,
reconnects, skipped minutes, full daily metadata, plus a browser for
every saved day — preview last N rows or download whole CSVs.

## Security checklist

- Set `API_KEY` (endpoints other than `/health` then require the header).
- Set `API_CORS_ORIGINS` to your exact Firebase URLs, not `*`.
- Never put Kotak credentials anywhere in the frontend.
- The API is read-only (GET only), with filename validation against
  path traversal.
