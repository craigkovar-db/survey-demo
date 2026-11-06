"""Hybrid search over the Lakebase amenity index + enrichment from UC.

Flow for one search request:

  1. embed_query(text) — gte-large-en via the model-serving endpoint
                        → 1024-dim query vector
  2. hybrid_search     — Postgres (Lakebase) query that combines:
                           - vector ANN on `river_amenity_search_synced.embedding`
                           - BM25 (ts_rank_cd) on `river_amenity_bm25.tsv`
                         fused with Reciprocal Rank Fusion, k=60
                         → ranked list of survey_ids with fused scores
  3. enrich_survey_details — SQL warehouse query against
                             `river_survey_enriched_joined` to pull ship,
                             sailing, satisfaction and the matched comment
                             for each hit survey_id. Returns rows in the
                             rank order from step 2.

Auth modes (both supported, per-request):
  - OBO (production on Databricks Apps): every surface call — serving
    endpoint, Lakebase credential minting, Lakebase Postgres connection,
    SQL warehouse — authenticates as the end user via their forwarded
    access token, so UC row-level grants are enforced end to end.
  - App/profile (local dev): user_token=None and each surface falls back to
    the module-level WorkspaceClient() (profile or SP creds).
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta
from threading import RLock
from typing import Any, Iterator

import psycopg2
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient

from backend.auth import workspace_for


# ---------- Config ----------

# Lakebase Autoscaling (projects / branches / endpoints) — not the standard
# instance-based Lakebase. The SDK service is `w.postgres`, and credentials
# are minted per *endpoint*, not per instance.
LAKEBASE_PROJECT = os.environ.get("LAKEBASE_PROJECT", "survey-demo-lakebase")
LAKEBASE_BRANCH = os.environ.get("LAKEBASE_BRANCH", "production")
LAKEBASE_ENDPOINT = os.environ.get("LAKEBASE_ENDPOINT", "primary")
LAKEBASE_HOST = os.environ.get(
    "LAKEBASE_HOST",
    "ep-calm-moon-d8gqqayh.database.us-east-2.cloud.databricks.com",
)
LAKEBASE_DB = os.environ.get("LAKEBASE_DB", "databricks_postgres")
LAKEBASE_SCHEMA = os.environ.get("LAKEBASE_SCHEMA", "survey_demo")
# How long to wait on psycopg2.connect before giving up (seconds).
LAKEBASE_CONNECT_TIMEOUT = int(os.environ.get("LAKEBASE_CONNECT_TIMEOUT", "10"))
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "5f583e258cc6613a")
SURVEY_TABLE = os.environ.get(
    "SURVEY_TABLE",
    "serverless_stable_yrljz5_catalog.survey_demo.river_survey_enriched_joined",
)
EMBED_ENDPOINT = os.environ.get("EMBED_ENDPOINT", "databricks-gte-large-en")
RRF_K = int(os.environ.get("RRF_K", "60"))
TOP_K_PER_SIDE = int(os.environ.get("TOP_K_PER_SIDE", "100"))


# ---------- Lazy singletons (app-auth only) ----------
#
# When OBO is active we DON'T cache — the token belongs to a specific user and
# sharing it across requests would leak permissions. The caches below are
# scoped to the app-identity path (local dev or SP fallback on Databricks
# Apps) where it's safe to reuse.
#
# RLock (not Lock) so nested helpers can re-acquire from the same thread
# without deadlocking.
_lock = RLock()
_w: WorkspaceClient | None = None
_cred_token: str | None = None
_cred_expires: datetime = datetime.min
_user_name: str | None = None


def _workspace(user_token: str | None = None) -> WorkspaceClient:
    """User-scoped client when `user_token` is set (not cached), otherwise the
    app-auth singleton."""
    if user_token:
        return workspace_for(user_token)
    global _w
    with _lock:
        if _w is None:
            _w = WorkspaceClient()
        return _w


def _lakebase_user(user_token: str | None = None) -> str:
    """Postgres user is the Databricks identity's email / SP app id."""
    if user_token:
        # OBO: fetch fresh — this IS the end user's identity.
        return workspace_for(user_token).current_user.me().user_name
    global _user_name
    with _lock:
        if _user_name is None:
            _user_name = _workspace().current_user.me().user_name
        return _user_name


def _lakebase_endpoint_path() -> str:
    return (
        f"projects/{LAKEBASE_PROJECT}"
        f"/branches/{LAKEBASE_BRANCH}"
        f"/endpoints/{LAKEBASE_ENDPOINT}"
    )


def _lakebase_token(user_token: str | None = None) -> str:
    """Short-lived OAuth password for Lakebase Autoscaling.

    - OBO: mint fresh per-call (can't cache — different user per request).
    - App-auth: cache for 55 minutes.
    """
    endpoint = _lakebase_endpoint_path()
    if user_token:
        cred = workspace_for(user_token).postgres.generate_database_credential(
            endpoint=endpoint,
        )
        return cred.token

    global _cred_token, _cred_expires
    now = datetime.utcnow()
    with _lock:
        if _cred_token and _cred_expires > now + timedelta(minutes=5):
            return _cred_token
        # ttl expects a protobuf Duration in this SDK — safer to omit it and
        # let the server use its default (1h). Credentials get re-minted every
        # 55 min anyway via `_cred_expires` below.
        cred = _workspace().postgres.generate_database_credential(endpoint=endpoint)
        _cred_token = cred.token
        _cred_expires = now + timedelta(minutes=55)
        return _cred_token


def _lakebase_connection(user_token: str | None = None):
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=5432,
        dbname=LAKEBASE_DB,
        user=_lakebase_user(user_token),
        password=_lakebase_token(user_token),
        sslmode="require",
        connect_timeout=LAKEBASE_CONNECT_TIMEOUT,
    )


def _warehouse_connection(user_token: str | None = None):
    """Warehouse connection. OBO uses the user's token directly; app-auth
    uses the SDK's credentials provider."""
    if user_token:
        host = (
            (os.environ.get("DATABRICKS_HOST") or "")
            .replace("https://", "")
            .rstrip("/")
        )
        return dbsql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
            access_token=user_token,
        )
    cfg = _workspace().config
    host = (cfg.host or "").replace("https://", "").rstrip("/")
    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


# ---------- Core pipeline ----------

def embed_query(text: str, user_token: str | None = None) -> list[float]:
    """Call the gte-large-en serving endpoint via the OpenAI-compatible client
    (returns 1024-dim embedding).

    ALWAYS uses app auth (the app's service principal), not OBO. Reason:
    model-serving endpoint invocation requires the `model-serving` scope,
    which is NOT in the documented Databricks Apps user_api_scopes list
    (only sql / files.files / dashboards.genie / iam.* are requestable).
    An OBO token therefore gets back
        403 "Provided OAuth token does not have required scopes:
             model-serving"
    even if you try to declare it.

    This is fine for the hybrid-search pipeline: the embedding is just
    query vectorization — no user data is read here. User-permission
    enforcement happens on the DOWNSTREAM calls (Lakebase credential +
    Postgres connection + SQL warehouse), which DO use OBO and respect
    the end user's UC grants row-by-row.

    `user_token` is kept in the signature for interface symmetry with the
    rest of the pipeline, but it is intentionally unused here.
    """
    del user_token  # app-auth only for this call — see docstring
    client = _workspace().serving_endpoints.get_open_ai_client()
    resp = client.embeddings.create(model=EMBED_ENDPOINT, input=[text])
    return list(resp.data[0].embedding)


def _hybrid_search_sql(
    emb: list[float],
    q: str,
    limit: int,
    user_token: str | None = None,
) -> list[dict[str, Any]]:
    """Lakebase hybrid-search half of the pipeline. Takes a pre-computed query
    embedding so the embedding call can be timed/surfaced separately."""
    vec_literal = "[" + ",".join(f"{x:.6f}" for x in emb) + "]"

    sql = f"""
    WITH vec AS (
      SELECT survey_id,
             ROW_NUMBER() OVER (
               ORDER BY amenity_embedding <=> %s::vector
             ) AS rnk
      FROM {LAKEBASE_SCHEMA}.river_amenity_search_synced
      ORDER BY amenity_embedding <=> %s::vector
      LIMIT %s
    ),
    bm AS (
      SELECT survey_id,
             ROW_NUMBER() OVER (
               ORDER BY ts_rank_cd(amenity_tsv, plainto_tsquery('english', %s)) DESC
             ) AS rnk
      FROM {LAKEBASE_SCHEMA}.river_amenity_bm25
      WHERE amenity_tsv @@ plainto_tsquery('english', %s)
      LIMIT %s
    )
    SELECT COALESCE(vec.survey_id, bm.survey_id) AS survey_id,
           (1.0 / ({RRF_K} + COALESCE(vec.rnk, {TOP_K_PER_SIDE} + 1))) +
           (1.0 / ({RRF_K} + COALESCE(bm.rnk,  {TOP_K_PER_SIDE} + 1))) AS score
    FROM vec FULL OUTER JOIN bm USING (survey_id)
    ORDER BY score DESC
    LIMIT %s
    """
    params = (vec_literal, vec_literal, TOP_K_PER_SIDE,
              q, q, TOP_K_PER_SIDE,
              limit)

    with _lakebase_connection(user_token) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [{"survey_id": str(r[0]), "score": float(r[1])} for r in rows]


def hybrid_search(
    query: str, limit: int = 20, user_token: str | None = None,
) -> list[dict[str, Any]]:
    """One-shot embed + Lakebase search. Preserved for the non-streaming endpoint."""
    q = query.strip()
    if not q:
        return []
    emb = embed_query(q, user_token)
    return _hybrid_search_sql(emb, q, limit, user_token)


def enrich_survey_details(
    hits: list[dict], user_token: str | None = None,
) -> list[dict]:
    """Join the ranked survey_ids back to UC to get ship, sailing, satisfaction,
    and the matched comment. Preserves hit order."""
    if not hits:
        return []
    # survey_id is bigint in both Lakebase and UC — coerce to int for the warehouse
    # parameter binding (databricks-sql-connector binds Python ints as BIGINT).
    ids = [int(h["survey_id"]) for h in hits]
    # databricks-sql-connector uses `?` for parameter placeholders, not `%s`.
    placeholders = ",".join(["?"] * len(ids))
    sql = f"""
    SELECT
      survey_id,
      ship_name,
      ship_class,
      itinerary_name,
      sailing_date,
      overall_experience,
      likelihood_to_recommend,
      amenities_satisfaction,
      amenity_sentiment,
      amenity_names,
      amenities_comment
    FROM {SURVEY_TABLE}
    WHERE survey_id IN ({placeholders})
    """
    with _warehouse_connection(user_token) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, ids)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

    by_id = {str(r[0]): dict(zip(cols, r)) for r in rows}
    merged: list[dict] = []
    for h in hits:
        d = by_id.get(h["survey_id"])
        if d is None:
            continue
        # JSON-safe transforms — the warehouse driver returns dates / numpy
        # arrays / Decimals that json.dumps can't handle out of the box.
        d["survey_id"] = str(d["survey_id"])
        if d.get("sailing_date") is not None:
            d["sailing_date"] = str(d["sailing_date"])
        names = d.get("amenity_names")
        if names is not None and not isinstance(names, (str, bytes)):
            try:
                d["amenity_names"] = [str(n) for n in names]
            except TypeError:
                d["amenity_names"] = None
        d["score"] = h["score"]
        merged.append(d)
    return merged


def search(
    query: str, limit: int = 20, user_token: str | None = None,
) -> list[dict]:
    """One-shot API: query → hybrid search → UC enrichment."""
    hits = hybrid_search(query, limit, user_token)
    return enrich_survey_details(hits, user_token)


# ---------- Streaming variant (SSE) ----------

def _sse(data: dict[str, Any]) -> str:
    """Format a Server-Sent Event message. Each ends with a blank line."""
    return f"data: {json.dumps(data)}\n\n"


def search_stream(
    query: str, limit: int = 20, user_token: str | None = None,
) -> Iterator[str]:
    """Yield SSE events while the pipeline runs so the UI can show per-step
    progress. Events have the shape:

      {"step": "embedding"|"lakebase"|"enriching"|"complete"|"error",
       "status": "started"|"done"|...,
       "duration_ms": <int>,
       <extras>...}

    On completion, the final event is `{"step": "complete", "results": [...]}`.
    Any raised exception becomes `{"step": "error", "message": "..."}`.

    When `user_token` is provided, every surface (serving endpoint, Lakebase
    credential + connection, warehouse) authenticates as the end user (OBO).
    """
    q = query.strip()
    if not q:
        yield _sse({"step": "error", "message": "Empty query."})
        return

    try:
        # Step 1 — embed via gte-large-en
        t0 = time.time()
        yield _sse({
            "step": "embedding", "status": "started",
            "label": f"Embedding query via {EMBED_ENDPOINT}",
        })
        emb = embed_query(q, user_token)
        yield _sse({
            "step": "embedding", "status": "done",
            "duration_ms": int((time.time() - t0) * 1000),
            "dims": len(emb),
        })

        # Step 2 — hybrid search in Lakebase (broken into sub-steps so a hang
        # at any point is visible from the UI).
        step2_start = time.time()
        yield _sse({
            "step": "lakebase", "status": "started",
            "label": f"Hybrid search in Lakebase (vector + BM25 → RRF, k={RRF_K})",
        })

        # 2a — mint OAuth credential (user-scoped when OBO)
        t0 = time.time()
        yield _sse({"step": "lakebase_cred", "status": "started",
                    "label": "Minting Lakebase OAuth credential"})
        token = _lakebase_token(user_token)
        yield _sse({"step": "lakebase_cred", "status": "done",
                    "duration_ms": int((time.time() - t0) * 1000)})

        # 2b — connect to Postgres (will time out after LAKEBASE_CONNECT_TIMEOUT)
        t0 = time.time()
        yield _sse({"step": "lakebase_conn", "status": "started",
                    "label": f"Connecting to Postgres ({LAKEBASE_HOST})"})
        conn = psycopg2.connect(
            host=LAKEBASE_HOST,
            port=5432,
            dbname=LAKEBASE_DB,
            user=_lakebase_user(user_token),
            password=token,
            sslmode="require",
            connect_timeout=LAKEBASE_CONNECT_TIMEOUT,
        )
        yield _sse({"step": "lakebase_conn", "status": "done",
                    "duration_ms": int((time.time() - t0) * 1000)})

        # 2c — execute the RRF query
        t0 = time.time()
        yield _sse({"step": "lakebase_query", "status": "started",
                    "label": "Running vector + BM25 query"})
        try:
            vec_literal = "[" + ",".join(f"{x:.6f}" for x in emb) + "]"
            # Column names in Lakebase: `amenity_embedding` (vector) and
            # `amenity_tsv` (tsvector), both keyed on `survey_id` (bigint).
            sql = f"""
            WITH vec AS (
              SELECT survey_id,
                     ROW_NUMBER() OVER (
                       ORDER BY amenity_embedding <=> %s::vector
                     ) AS rnk
              FROM {LAKEBASE_SCHEMA}.river_amenity_search_synced
              ORDER BY amenity_embedding <=> %s::vector
              LIMIT %s
            ),
            bm AS (
              SELECT survey_id,
                     ROW_NUMBER() OVER (
                       ORDER BY ts_rank_cd(amenity_tsv, plainto_tsquery('english', %s)) DESC
                     ) AS rnk
              FROM {LAKEBASE_SCHEMA}.river_amenity_bm25
              WHERE amenity_tsv @@ plainto_tsquery('english', %s)
              LIMIT %s
            )
            SELECT COALESCE(vec.survey_id, bm.survey_id) AS survey_id,
                   (1.0 / ({RRF_K} + COALESCE(vec.rnk, {TOP_K_PER_SIDE} + 1))) +
                   (1.0 / ({RRF_K} + COALESCE(bm.rnk,  {TOP_K_PER_SIDE} + 1))) AS score
            FROM vec FULL OUTER JOIN bm USING (survey_id)
            ORDER BY score DESC
            LIMIT %s
            """
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (vec_literal, vec_literal, TOP_K_PER_SIDE,
                     q, q, TOP_K_PER_SIDE, limit),
                )
                rows = cur.fetchall()
            hits = [{"survey_id": str(r[0]), "score": float(r[1])} for r in rows]
        finally:
            conn.close()
        yield _sse({"step": "lakebase_query", "status": "done",
                    "duration_ms": int((time.time() - t0) * 1000),
                    "hits": len(hits)})

        # Close the parent lakebase step too so the UI can collapse the sub-stages.
        yield _sse({
            "step": "lakebase", "status": "done",
            "duration_ms": int((time.time() - step2_start) * 1000),
            "hits": len(hits),
        })

        # Step 3 — enrich from UC via warehouse (OBO when user_token is set)
        t0 = time.time()
        yield _sse({
            "step": "enriching", "status": "started",
            "label": f"Fetching {len(hits)} surveys from Unity Catalog",
        })
        results = enrich_survey_details(hits, user_token)
        yield _sse({
            "step": "enriching", "status": "done",
            "duration_ms": int((time.time() - t0) * 1000),
            "returned": len(results),
        })

        # Final event
        yield _sse({"step": "complete", "results": results})
    except Exception as e:  # noqa: BLE001
        yield _sse({
            "step": "error",
            "message": f"{type(e).__name__}: {e}",
        })
