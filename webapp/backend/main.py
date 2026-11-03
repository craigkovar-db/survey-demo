"""DataCruises webapp — FastAPI backend.

- Serves the built React SPA from `frontend/dist` when deployed.
- During local dev, Vite serves the SPA at :5173 and proxies /api/* here (:8000).
- Exposes /api/config so the frontend can pick up the dashboard URL (and any
  other workspace-specific values) without baking them into the JS bundle.
"""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

# Load .env for local dev (no-op if the file is absent, e.g. on Databricks Apps
# where env vars are injected via app.yaml).
try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except Exception:
    pass


app = FastAPI(title="DataCruises Customer Insight")


# Local dev: Vite is on :5173, backend on :8000 — needs CORS to call /api/*
# directly. On Databricks Apps the SPA is same-origin so CORS is a no-op.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- API routes ----------

@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/search")
def api_search(
    q: str = Query("", description="Search query"),
    limit: int = Query(20, ge=1, le=100),
) -> JSONResponse:
    """Hybrid search over Lakebase + UC enrichment (blocking, non-streaming).

    Returns a JSON array of result rows ordered by fused score (desc)."""
    try:
        from backend.search import search as run_search

        return JSONResponse(run_search(q, limit))
    except Exception as e:  # surface the full error to the frontend for now
        raise HTTPException(
            status_code=500,
            detail=f"{type(e).__name__}: {e}",
        )


@app.get("/api/search-stream")
def api_search_stream(
    q: str = Query("", description="Search query"),
    limit: int = Query(20, ge=1, le=100),
) -> StreamingResponse:
    """Streaming (SSE) version of /api/search — yields per-step status events
    so the UI can show which stage of the pipeline is running (embedding →
    Lakebase hybrid search → UC enrichment) and how long each took."""
    from backend.search import search_stream

    return StreamingResponse(
        search_stream(q, limit),
        media_type="text/event-stream",
        headers={
            # Disable proxy buffering so events arrive incrementally.
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/config")
def config() -> JSONResponse:
    """Config the frontend needs at runtime, kept out of the JS bundle so
    the same build can target different workspaces.

    Builds the iframe `/embed/` URL from the host + ids so swapping workspace
    is a one-env-var change."""
    host = os.environ.get(
        "DATABRICKS_HOST",
        "https://fevm-serverless-stable-yrljz5.cloud.databricks.com",
    ).rstrip("/")
    dashboard_id = os.environ.get(
        "DASHBOARD_ID", "01f1d713815e18f8ab64879f1b9fce66"
    )
    workspace_id = os.environ.get("WORKSPACE_ID", "7474649976481459")
    dashboard_url = f"{host}/embed/dashboardsv3/{dashboard_id}?o={workspace_id}"

    return JSONResponse(
        {
            "appName": os.environ.get("APP_NAME", "DataCruises Customer Insight"),
            "dashboardUrl": dashboard_url,
        }
    )


# ---------- Static SPA (production serving) ----------
# When the frontend has been built (`npm run build`), `frontend/dist` holds the
# compiled assets. In local dev the folder may be absent — Vite serves the UI
# directly — so this mount is best-effort.

_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"

if _DIST.is_dir():
    # Serve the asset files (JS, CSS, images) under their hashed paths.
    app.mount(
        "/assets",
        StaticFiles(directory=_DIST / "assets"),
        name="assets",
    )

    @app.get("/{full_path:path}")
    def spa_fallback(full_path: str):
        """SPA fallback — any non-/api/* path returns index.html so React
        Router handles the route client-side."""
        if full_path.startswith("api/"):
            return JSONResponse({"detail": "Not Found"}, status_code=404)
        # If the request is for a real file (favicon, logo.svg), serve it.
        candidate = _DIST / full_path
        if full_path and candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(_DIST / "index.html")
else:
    @app.get("/")
    def dev_index() -> dict:
        """Fallback when the SPA isn't built yet — points at the Vite dev server."""
        return {
            "message": (
                "Frontend not built. During local dev, open http://localhost:5173 "
                "(Vite proxies /api/* to this backend)."
            )
        }
