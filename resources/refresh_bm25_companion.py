# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Lakebase BM25 Companion Table
# MAGIC
# MAGIC Run **after** the sync pipeline updates `river_amenity_search_synced` to
# MAGIC keep the BM25 companion `river_amenity_bm25` and its `lakebase_bm25` index
# MAGIC aligned with the latest comments.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Pull a short-lived OAuth token for the Lakebase endpoint
# MAGIC 2. `TRUNCATE` + `INSERT INTO ... SELECT to_tsvector(...)` from the synced table
# MAGIC 3. Create the `lakebase_bm25` index if it does not exist
# MAGIC 4. `VACUUM ANALYZE` so BM25 corpus statistics refresh

# COMMAND ----------

# MAGIC %pip install -q psycopg2-binary "databricks-sdk>=0.96,<1.0"
# MAGIC
# MAGIC # ^ pin databricks-sdk explicitly. The serverless runtime's pre-installed
# MAGIC #   SDK is older and does NOT have `WorkspaceClient.postgres` (the attribute
# MAGIC #   for Lakebase Autoscaling projects/branches/endpoints). Without this pin
# MAGIC #   the next cell fails with:
# MAGIC #     AttributeError: 'WorkspaceClient' object has no attribute 'postgres'

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import psycopg2
from databricks.sdk import WorkspaceClient

PROJECT_ID    = "survey-demo-lakebase"
BRANCH_ID     = "production"
ENDPOINT_ID   = "primary"
DB_NAME       = "databricks_postgres"
PG_SCHEMA     = "survey_demo"
SYNCED_TABLE  = "river_amenity_search_synced"
BM25_TABLE    = "river_amenity_bm25"
BM25_INDEX    = "river_amenity_bm25_idx"

endpoint_path = f"projects/{PROJECT_ID}/branches/{BRANCH_ID}/endpoints/{ENDPOINT_ID}"
print(f"endpoint = {endpoint_path}")

# COMMAND ----------

# MAGIC %md ## 1. Resolve host, user, and OAuth token via the SDK

# COMMAND ----------

w = WorkspaceClient()

# Hard-coded Lakebase host for this project (matches the webapp's env config).
# `w.postgres.get_endpoint(endpoint_path)` returns the same value, but keeping
# it static here means the notebook does not need a second SDK call to run.
host = "ep-calm-moon-d8gqqayh.database.us-east-2.cloud.databricks.com"
print(f"host = {host}")

# IMPORTANT: `endpoint` must be passed as a KEYWORD argument. The SDK method
# signature is `generate_database_credential(*, endpoint, ...)` — a positional
# string is rejected with a TypeError on current SDK versions.
cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
token = cred.token

# generate_database_credential may or may not return a username; fall back to
# the running identity (works for interactive user runs; for a job running as a
# service principal, service_principal_client_id is the right login value).
username = getattr(cred, "username", None)
if not username:
    me = w.current_user.me()
    username = (
        getattr(me, "service_principal_client_id", None)
        or me.user_name
    )
print(f"user = {username}")

# COMMAND ----------

# MAGIC %md ## 2. Refresh the BM25 companion table

# COMMAND ----------

conn = psycopg2.connect(
    host=host,
    user=username,
    password=token,
    dbname=DB_NAME,
    sslmode="require",
)
conn.autocommit = True  # VACUUM cannot run inside a transaction block

try:
    with conn.cursor() as cur:
        # Idempotent create
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.{BM25_TABLE} (
                survey_id   BIGINT PRIMARY KEY,
                amenity_tsv TSVECTOR
            )
        """)

        # Repopulate from the synced table
        cur.execute(f"TRUNCATE TABLE {PG_SCHEMA}.{BM25_TABLE}")
        cur.execute(f"""
            INSERT INTO {PG_SCHEMA}.{BM25_TABLE} (survey_id, amenity_tsv)
            SELECT survey_id, to_tsvector('english', amenities_comment)
            FROM   {PG_SCHEMA}.{SYNCED_TABLE}
            WHERE  amenities_comment IS NOT NULL
        """)

        cur.execute(f"SELECT COUNT(*) FROM {PG_SCHEMA}.{BM25_TABLE}")
        bm25_rows = cur.fetchone()[0]
        print(f"loaded {bm25_rows:,} rows into {PG_SCHEMA}.{BM25_TABLE}")

        # Create BM25 index if it doesn't already exist
        cur.execute(
            """SELECT 1 FROM pg_indexes
               WHERE schemaname = %s AND indexname = %s""",
            (PG_SCHEMA, BM25_INDEX),
        )
        if cur.fetchone():
            print(f"index {BM25_INDEX} already exists")
        else:
            print(f"creating index {BM25_INDEX}")
            cur.execute(f"""
                CREATE INDEX {BM25_INDEX}
                ON {PG_SCHEMA}.{BM25_TABLE}
                USING lakebase_bm25 (amenity_tsv)
            """)

        # Keep BM25 autovacuum aggressive on inserts
        cur.execute(f"""
            ALTER TABLE {PG_SCHEMA}.{BM25_TABLE}
            SET (autovacuum_vacuum_insert_scale_factor = 0)
        """)

        # Refresh BM25 corpus statistics
        cur.execute(f"VACUUM ANALYZE {PG_SCHEMA}.{BM25_TABLE}")
        print("VACUUM ANALYZE complete")
finally:
    conn.close()

print("Done.")