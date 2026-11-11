# survey-demo

End-to-end demo for turning free-text post-cruise surveys into governed
analytics on Databricks. The surveys arrive as semi-structured events — a
mix of numeric ratings, categorical answers, and (crucially) open-ended
comments where guests describe what they actually experienced. The goal of
this demo is to show how Databricks surfaces the signal buried in those
unstructured comments and makes it queryable three different ways: as
governed metrics, as hybrid semantic + keyword search, and as a
conversational Genie agent.

---

## 1. Project overview

### The problem

Survey data is only partially structured. The 0–10 overall-experience
score is easy; the paragraph of free text where the guest explains *why*
is where the business value lives. Flat Delta tables can't answer
questions like "which amenities drove negative sentiment on Mekong-class
sailings in Q2?" without pre-processing the comments into something
analyzable.

### The pipeline

1. **Generate** — synthetic Viking River Cruise surveys are produced in
   `resources/generate_synthetic_data` so the demo is self-contained. Ship
   rosters, itineraries, and amenity names come from `data_notes.txt`, a
   late-2026 research snapshot used so the data is realistic.

2. **Enrich with AI Functions** — `resources/enrich_survey_incremental`
   runs on top of the raw survey table and uses Databricks
   [AI Functions](https://docs.databricks.com/sql/language-manual/functions/ai_functions.html)
   to turn the unstructured text into columns a BI tool can group by:
     - `ai_classify` → `stateroom_sentiment`, `amenity_sentiment`
       (positive / neutral / negative on each free-text comment).
     - `ai_extract` → structured `amenity_names` list from the amenity
       comment (what was actually mentioned).
     - `ai_query` with the `databricks-gte-large-en` endpoint → a
       1024-dim embedding of each amenity comment, written back to Delta
       so Lakebase can pick it up.
   The notebook is wrapped in a **Lakeflow Job** (`resources/enrich_job.json`)
   that processes only incremental changes against a watermark, so the
   enrichment keeps up with new surveys without re-scanning history.

3. **Govern as Metric Views** — the enriched Delta table is projected
   into two Unity Catalog metric views:
     - `river_survey_enriched_mv` — the default view, with dimensions for
       ship / class / itinerary / sailing / amenity names and measures
       for NPS, overall experience, likelihood to recommend, stateroom
       satisfaction, amenities satisfaction, and amount paid.
     - `river_amenity_metrics_mv` — amenity-level view, one row per
       mentioned amenity, used when questions are explicitly about
       amenities.
   Metric views give every downstream consumer (dashboards, Genie, BI
   tools) a single governed definition of NPS, satisfaction, etc. — no
   two tiles can disagree.

4. **Hybrid search in Lakebase** — the enriched Delta table is also synced
   to **Lakebase Autoscaling** (`river_amenity_search_synced`), which
   carries the amenity embeddings as a `VECTOR(1024)` column with a
   `lakebase_ann` index for cosine ANN. A companion non-synced table
   `river_amenity_bm25` carries a `tsvector` with a `lakebase_bm25` index
   for keyword search. The webapp fuses the two with **Reciprocal Rank
   Fusion** (k = 60) so a query like *"Aquavit Terrace was broken"* finds
   surveys that mention the broken terrace even when the exact phrase
   isn't present.

5. **Conversational analytics via Genie** — a Genie Agent space over the
   two metric views (exported in `resources/genie_space_export.json`)
   answers natural-language questions. The agent is instructed to route
   amenity-specific questions to `river_amenity_metrics_mv` and
   everything else to `river_survey_enriched_mv`.

6. **Serve to users** — all of this is exposed by a React + FastAPI
   Databricks App (`webapp/`). The app embeds a published AI/BI dashboard
   on one page, a hybrid-search explorer on another, and a global Genie
   chat drawer. User identity flows through on every call via
   **on-behalf-of (OBO)** auth, so Unity Catalog permissions are enforced
   on the entire read path.

### Architecture at a glance

```
  Raw surveys (Delta)
          │
          │  resources/enrich_survey_incremental  (notebook)
          │  Lakeflow Job: resources/enrich_job.json
          │  AI Functions: ai_classify, ai_extract, ai_query (gte-large-en)
          ▼
  Enriched surveys (Delta)
          │
          ├──► Metric Views ────► Genie Agent ────► ChatDrawer (webapp)
          │     (02, 04 .sql)      (genie_space_
          │                        export.json)
          │
          ├──► AI/BI Dashboard ────────────────► Dashboard page (webapp)
          │     (datacruises_dashboard
          │      .lvdash.json)
          │
          └──► Lakebase sync ────► Hybrid search ────► Explore page (webapp)
                (river_amenity_        (vector <=> ::vector
                 search_synced +        + ts_rank_cd(tsv, ...)
                 _bm25 companion)       → RRF)
```

---

## 2. Folder and file layout

```
survey-demo/
├── README.md               ← this file
├── build_log.txt           ← session-by-session change log (prompts + notes)
├── data_notes.txt          ← cruise-fleet research notes used for data gen
│
├── resources/              ← build artifacts: notebooks, Jobs, SQL, exports
├── dashboards/             ← AI/BI dashboard sources
├── genie/                  ← Genie Agent workspace folder (space lives in-workspace)
├── proof/                  ← end-to-end verification artifacts, one per layer
└── webapp/                 ← React + FastAPI Databricks App
```

### `resources/` — the pipeline, as code

Everything needed to rebuild the data layer from scratch. Pair the `*.sql`
files with the matching metric-view / notebook they define.

| File                                            | What it is                                                                 |
|-------------------------------------------------|----------------------------------------------------------------------------|
| `generate_synthetic_data` (notebook)            | Produces the synthetic survey Delta table (Viking River fleet, 2026).      |
| `job.json`                                      | Lakeflow Job wrapping the synthetic data generator.                        |
| `enrich_survey_incremental` (notebook)          | Incremental AI enrichment (ai_classify, ai_extract, ai_query embedding).   |
| `enrich_job.json`                               | Lakeflow Job wrapping the enrichment notebook.                             |
| `01_river_survey_enriched_joined.sql`           | View that joins the enriched survey rows with dim tables.                  |
| `02_river_survey_enriched_mv.sql`               | **Metric view** — NPS, satisfaction, revenue; the Genie default.           |
| `03_river_survey_enriched_amenity_exploded.sql` | Explodes `amenity_names[]` to one-row-per-amenity.                         |
| `04_river_amenity_metrics_mv.sql`               | **Metric view** — per-amenity NPS, satisfaction, sentiment counts.         |
| `add_new_amenity_search_rows` (notebook)        | Appends new rows into the Lakebase sync table + embeddings.                |
| `refresh_bm25_companion` (notebook)             | Refreshes `tsvector` in the BM25 companion table.                          |
| `sync_bm25_job.json`                            | Lakeflow Job wrapping the BM25 refresh (runs after each enrichment batch). |
| `genie_space_export.json`                       | Exported configuration of the Genie Agent space (routing hints + examples).|
| `datacruises_dashboard.lvdash.json`             | Exported AI/BI dashboard (the one embedded in the webapp Dashboard page).  |

### `dashboards/`

| File                   | What it is                                       |
|------------------------|--------------------------------------------------|
| `Insight.lvdash.json`  | AI/BI dashboard source (`lakeview` serialized).  |

The published dashboard embedded by the webapp lives in the workspace at
dashboard id `01f1d713815e18f8ab64879f1b9fce66`.

### `genie/`

Workspace folder for the Genie Agent. The space itself lives in-workspace
(id `01f1d6f7660a1f65a083b31406069c4b`); the folder here is mostly a
reference point. The authoritative export is
`resources/genie_space_export.json`.

### `proof/` — artifacts that show each layer works

One file (or pair) per build phase. Every entry includes the exact query
run, the result returned, and a verdict.

| File                                        | What it proves                                                   |
|---------------------------------------------|------------------------------------------------------------------|
| `synthetic_data_proof` / `.html`            | Synthetic generator produces a realistic fleet / schedule.       |
| `enriched_data_proof.txt` / `.html`         | AI Functions produce sentiment / amenity extraction / embeddings.|
| `lakebase_proof.txt`                        | Lakebase hybrid search (ANN + BM25 + RRF) returns relevant hits. |
| `Genie_Test.txt`                            | Genie Agent routes correctly and answers survey questions.       |
| `webapp_proof.txt`                          | The deployed Databricks App serves hybrid search + Genie chat    |
|                                             | end-to-end under OBO.                                            |

### `webapp/` — React + FastAPI Databricks App

Can be run locally (`run_local.sh`) and deployed as a Databricks App from
the same source tree. User identity flows via
`X-Forwarded-Access-Token` on every request; locally that header is
absent and the backend falls back to profile auth.

```
webapp/
├── app.yaml              ← Databricks Apps config: command, env, user_api_scopes
├── pyproject.toml        ← uv-managed Python deps
├── uv.lock
├── requirements.txt      ← exported from uv.lock (what Databricks Apps installs)
├── run_local.sh          ← one-command local dev: uv sync + npm install + run both
├── .env.example          ← template for local env vars
├── README.md             ← webapp-specific build / deploy notes
├── backend/              ← FastAPI (uvicorn)
│   ├── main.py           ← routes: /api/health, /api/config,
│   │                                /api/search(+stream),
│   │                                /api/genie-one/ask(+stream)
│   ├── auth.py           ← OBO helper: X-Forwarded-Access-Token →
│   │                       user-scoped WorkspaceClient (auth_type="pat")
│   ├── search.py         ← Hybrid search pipeline:
│   │                       embed (gte-large-en, app-auth)
│   │                       → Lakebase RRF (OBO)
│   │                       → UC warehouse enrichment (OBO)
│   └── genie_one.py      ← Genie One client (data-rooms /onechat/responses),
│                           SSE streaming, OBO Bearer auth
└── frontend/             ← React 18 + Vite + TypeScript
    ├── package.json, tsconfig.json, vite.config.ts
    ├── index.html
    ├── public/logo.svg
    └── src/
        ├── main.tsx, App.tsx       ← router + global ChatDrawer mount
        ├── pages/
        │   ├── Dashboard.tsx       ← embedded AI/BI dashboard (iframe)
        │   └── Explore.tsx         ← hybrid search UI with streaming pipeline
        ├── components/
        │   ├── TopBar.tsx          ← navy top bar with DataCruises logo
        │   └── ChatDrawer.tsx      ← global Genie One drawer with live
        │                             reasoning timeline + markdown answer
        └── styles/theme.css        ← Viking-inspired palette (red / navy /
                                       gold / cream), EB Garamond + Open Sans
```

#### Auth model inside the webapp

| Call                                   | Auth                | Why                               |
|----------------------------------------|---------------------|-----------------------------------|
| gte-large-en embedding                 | App SP (DATABRICKS_CLIENT_ID / SECRET) | Query vectorization only; no user data read. |
| Lakebase credential (`generate_db_cred`) | **OBO**           | End user identity → Postgres user.|
| Lakebase Postgres connection           | **OBO**             | Each user authenticates to PG as themselves. |
| SQL warehouse (survey enrichment)      | **OBO**             | UC row/column grants enforced.    |
| Genie One (`onechat/responses`)        | **OBO**             | Genie answers with user's grants. |

`app.yaml` requests the matching OBO scopes:
`sql`, `postgres`, `genie`, `model-serving` (the first three are needed
for the OBO path above; `model-serving` is declared for completeness and
in case the embedding call is ever switched to OBO).

---

## 3. Rebuild from zero

The usual order (see `build_log.txt` for session-by-session detail):

1. Generate synthetic surveys → `resources/generate_synthetic_data`.
2. Run the enrichment Lakeflow Job → `resources/enrich_job.json`.
3. Create the metric views → `resources/02_*.sql`, `resources/04_*.sql`.
4. Create the Lakebase sync + BM25 companion tables; run
   `resources/sync_bm25_job.json`.
5. Import the Genie Agent space from `resources/genie_space_export.json`.
6. Create (or update) the AI/BI dashboard from
   `resources/datacruises_dashboard.lvdash.json`.
7. Deploy the webapp (`databricks apps deploy survey-demo
   --source-code-path /Workspace/.../survey-demo/webapp`).

Each step has a matching artifact in `proof/` so you can verify it before
moving on.
