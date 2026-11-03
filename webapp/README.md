# DataCruises Customer Insight — React + FastAPI

A Databricks App with a **React 18 / Vite / TypeScript frontend** and a
**FastAPI backend**. Python deps are managed by **uv**; the Node side uses
npm. The same codebase runs locally for dev and deploys to Databricks Apps.

Separate from the Dash prototype at `../app/` — this one is kept side-by-side
during the transition.

## Layout

```
webapp/
├── app.yaml                  # Databricks Apps config (uvicorn)
├── pyproject.toml            # Python deps (fastapi, uvicorn, databricks-sdk)
├── uv.lock                   # locked Python versions
├── requirements.txt          # exported from uv.lock for Databricks Apps
├── .env.example              # local env template
├── run_local.sh              # starts backend + frontend together
│
├── backend/
│   └── main.py               # FastAPI: /api/health, /api/config, SPA static mount
│
└── frontend/
    ├── package.json          # React + Vite + Router + TS
    ├── vite.config.ts        # dev proxies /api → :8000; build → ./dist
    ├── tsconfig.json
    ├── index.html            # loads EB Garamond + Open Sans
    ├── public/logo.svg       # DataCruises inline SVG logo
    └── src/
        ├── main.tsx          # React root + <BrowserRouter>
        ├── App.tsx           # layout + routes + /api/config fetch
        ├── components/
        │   └── TopBar.tsx    # navy top bar, logo, nav links
        ├── pages/
        │   ├── Dashboard.tsx # Page 1 — embeds the AI/BI dashboard via iframe
        │   └── Explore.tsx   # Page 2 — placeholder, filled in next step
        └── styles/theme.css  # Viking palette + layout
```

## Prerequisites

```bash
# uv — Python package manager / interpreter manager
brew install uv            # or: curl -LsSf https://astral.sh/uv/install.sh | sh

# node — frontend tooling (npm ships with it)
brew install node          # or your package manager of choice
```

Backend runs on **Python 3.11** (uv fetches it automatically on first `uv sync`).

## Run locally

```bash
cd /Users/Craig.Kovar/Desktop/Demo/survey_demo/webapp
cp .env.example .env       # once; edit values if the dashboard URL changes

./run_local.sh             # starts backend (:8000) + frontend (:5173) in parallel
```

Open **http://localhost:5173**. Ctrl-C stops both.

### What `run_local.sh` does

1. Loads `.env` (or `.env.example` if `.env` doesn't exist yet)
2. `uv sync` — creates `.venv` with Python 3.11 and installs locked backend deps
3. `npm install` in `frontend/` — first run only
4. Launches **both** in the same terminal:
   - `uv run uvicorn backend.main:app --reload --port 8000`
   - `cd frontend && npm run dev` (Vite on `:5173` with hot reload)
5. Vite proxies `/api/*` → `http://127.0.0.1:8000` so there's no CORS fiddling

### Dashboard embedding — how it works

The first page embeds the AI/BI dashboard with a plain `<iframe>` pointed at
the dashboard's `/embed/dashboardsv3/<id>?o=<org>` URL. The embed endpoint
sends iframe-friendly response headers and uses the viewer's existing
Databricks session for auth — no token exchange on this side.

The URL is built server-side in `/api/config` from three env vars so the
same bundle targets any workspace:

| Env var | Purpose |
|---|---|
| `DATABRICKS_HOST` | Workspace URL (no trailing slash). |
| `DASHBOARD_ID` | AI/BI dashboard ID. |
| `WORKSPACE_ID` | Deployment / org id (the `?o=…` value in the dashboard URL). |

Locally, if the browser isn't signed into the workspace yet, the iframe
shows a Databricks login prompt the first time. Click through once in a new
tab (there's an "Open full dashboard ↗" button for exactly this), and the
embed loads inline on reload.

## Deploying to Databricks Apps (deferred — don't run yet)

Everything is wired up for deploy; we're intentionally holding off until the
second page is done.

When ready:

```bash
# 1. Build the frontend so FastAPI can static-mount it
cd frontend && npm run build && cd ..    # → frontend/dist/

# 2. Create the app with its resources (dashboard, future warehouse, etc.)
databricks --profile DEFAULT apps create datacruises-webapp --no-compute

# 3. Sync + deploy
databricks --profile DEFAULT sync . /Workspace/Users/craig.kovar@databricks.com/survey-demo/webapp
databricks --profile DEFAULT apps deploy datacruises-webapp \
  --source-code-path /Workspace/Users/craig.kovar@databricks.com/survey-demo/webapp

# 4. Start
databricks --profile DEFAULT apps start datacruises-webapp
```

The same `backend/main.py` serves `frontend/dist/` as static assets with an
SPA fallback, so React Router works for deep-links without extra config.

## Dep management

**Python** — managed by `uv` the same way as `/app/`:

```bash
uv add some-package
uv remove unneeded
uv export --quiet --no-hashes --no-emit-project --format requirements-txt > requirements.txt
```

**Frontend** — standard npm:

```bash
cd frontend
npm install some-package
```

## Styling

Theme (`frontend/src/styles/theme.css`) uses the same Viking-inspired palette
as the Dash app at `../app/`:

- `#8B0000` — Viking red (primary)
- `#0A2A4A` — navy
- `#B8860B` — gold accent
- `#F8F5F0` — cream canvas
- EB Garamond (headings), Open Sans (body) — EB Garamond is the free-fonts
  equivalent of Adobe Garamond Pro, which Viking's Typekit CSS actually serves

## Pages

| Route | Component | Status |
|---|---|---|
| `/dashboard` (default) | `pages/Dashboard.tsx` | Live — embeds the AI/BI dashboard |
| `/explore` | `pages/Explore.tsx` | Placeholder — content TBD |
