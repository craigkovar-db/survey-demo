# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Survey Enrichment
# MAGIC
# MAGIC Reads **new rows** from the survey table (anti-join against the target so
# MAGIC each run only processes survey_ids that have not been enriched yet) and
# MAGIC applies three AI functions:
# MAGIC
# MAGIC | Column | Function | Notes |
# MAGIC |--------|----------|-------|
# MAGIC | `stateroom_sentiment` | `ai_classify` -> positive / neutral / negative | NULL when `stateroom_comment` is NULL |
# MAGIC | `amenity_sentiment` | `ai_classify` -> positive / neutral / negative | NULL when `amenities_comment` is NULL |
# MAGIC | `amenity_names` | `ai_extract` -> ARRAY<STRING> of amenities named in the comment | NULL when `amenities_comment` is NULL |
# MAGIC | `amenity_embedding` | `ai_query` on `databricks-gte-large-en` | NULL when `amenities_comment` is NULL |
# MAGIC
# MAGIC Target table: `<catalog>.<schema>.<prefix>_survey_enriched`.
# MAGIC Parameterized by `cruise_type` (same as the generator) and a
# MAGIC `max_new_rows` safety cap for demo runs.

# COMMAND ----------

# MAGIC %md ## 1. Config

# COMMAND ----------

dbutils.widgets.dropdown("cruise_type", "River", ["River", "Ocean"], "Cruise type")
dbutils.widgets.text("max_new_rows", "1000", "Max new rows per run (0 = unlimited)")

cruise_type   = dbutils.widgets.get("cruise_type")
max_new_rows  = int(dbutils.widgets.get("max_new_rows"))

CATALOG       = "serverless_stable_yrljz5_catalog"
SCHEMA        = "survey_demo"
PREFIX        = cruise_type.lower()
SOURCE_TABLE  = f"{CATALOG}.{SCHEMA}.{PREFIX}_survey"
TARGET_TABLE  = f"{CATALOG}.{SCHEMA}.{PREFIX}_survey_enriched"
EMBED_MODEL   = "databricks-gte-large-en"

print(f"cruise_type     = {cruise_type}")
print(f"source          = {SOURCE_TABLE}")
print(f"target          = {TARGET_TABLE}")
print(f"embed_model     = {EMBED_MODEL}")
print(f"max_new_rows    = {max_new_rows} (0 = unlimited)")

# COMMAND ----------

# MAGIC %md ## 2. Create enriched target table (if not exists)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    survey_id                   BIGINT,
    booking_id                  BIGINT,
    overall_experience          INT,
    likelihood_to_recommend     INT,
    likelihood_to_book_another  INT,
    stateroom_satisfaction      INT,
    stateroom_comment           STRING,
    stateroom_sentiment         STRING,
    amenities_satisfaction      INT,
    amenities_comment           STRING,
    amenity_sentiment           STRING,
    amenity_names               ARRAY<STRING>,
    amenity_embedding           ARRAY<FLOAT>,
    enriched_at                 TIMESTAMP
) USING DELTA
""")

print(f"ensured {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md ## 3. Identify new survey rows (not yet in target)

# COMMAND ----------

# Rows in source that have no matching survey_id in target.
# Prefer rows that have at least one populated comment so the AI functions
# produce meaningful enrichment; the rest are still included so the enriched
# table is a complete mirror.
new_rows_query = f"""
WITH new_rows AS (
    SELECT s.*
    FROM {SOURCE_TABLE} s
    LEFT ANTI JOIN {TARGET_TABLE} t ON s.survey_id = t.survey_id
)
SELECT *
FROM new_rows
ORDER BY
    CASE WHEN stateroom_comment IS NOT NULL
           OR amenities_comment IS NOT NULL THEN 0 ELSE 1 END,
    survey_id
"""

if max_new_rows > 0:
    new_rows_query += f"\nLIMIT {max_new_rows}"

new_rows_df = spark.sql(new_rows_query)
new_count = new_rows_df.count()
print(f"new rows to enrich: {new_count:,}")

if new_count == 0:
    dbutils.notebook.exit("No new rows to enrich. Done.")

new_rows_df.createOrReplaceTempView("new_surveys")

# COMMAND ----------

# MAGIC %md ## 4. Apply AI functions
# MAGIC
# MAGIC `ai_classify` and `ai_extract` return VARIANT; we read sub-fields with the
# MAGIC colon operator and cast to the final column type. All three AI calls are
# MAGIC gated by a `CASE WHEN comment IS NOT NULL` so no inference happens on
# MAGIC rows without a comment.

# COMMAND ----------

enriched_df = spark.sql(f"""
SELECT
    survey_id,
    booking_id,
    overall_experience,
    likelihood_to_recommend,
    likelihood_to_book_another,
    stateroom_satisfaction,
    stateroom_comment,
    CASE WHEN stateroom_comment IS NOT NULL
         THEN ai_classify(
                stateroom_comment,
                '["positive","neutral","negative"]',
                map('version','2.0')
              ):response[0]::STRING
    END AS stateroom_sentiment,
    amenities_satisfaction,
    amenities_comment,
    CASE WHEN amenities_comment IS NOT NULL
         THEN ai_classify(
                amenities_comment,
                '["positive","neutral","negative"]',
                map('version','2.0')
              ):response[0]::STRING
    END AS amenity_sentiment,
    CASE WHEN amenities_comment IS NOT NULL
         THEN from_json(
                ai_extract(
                    amenities_comment,
                    '{{"amenity_names":{{"type":"array","items":{{"type":"string"}}}}}}',
                    map('version','2.0')
                ):response:amenity_names::STRING,
                'ARRAY<STRING>'
              )
    END AS amenity_names,
    CASE WHEN amenities_comment IS NOT NULL
         THEN ai_query(
                '{EMBED_MODEL}',
                amenities_comment,
                returnType => 'ARRAY<FLOAT>'
              )
    END AS amenity_embedding,
    current_timestamp() AS enriched_at
FROM new_surveys
""")

# COMMAND ----------

# MAGIC %md ## 5. Insert into enriched table

# COMMAND ----------

(enriched_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(TARGET_TABLE))

print(f"inserted {new_count:,} rows into {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md ## 6. Verification

# COMMAND ----------

# MAGIC %md ### Row counts

# COMMAND ----------

display(spark.sql(f"""
SELECT
    (SELECT COUNT(*) FROM {SOURCE_TABLE})                             AS source_rows,
    (SELECT COUNT(*) FROM {TARGET_TABLE})                             AS enriched_rows,
    (SELECT COUNT(*) FROM {SOURCE_TABLE})
    - (SELECT COUNT(*) FROM {TARGET_TABLE})                           AS remaining_to_enrich
"""))

# COMMAND ----------

# MAGIC %md ### Sentiment distribution for the latest batch

# COMMAND ----------

display(spark.sql(f"""
SELECT
    stateroom_sentiment,
    amenity_sentiment,
    COUNT(*) AS n
FROM {TARGET_TABLE}
WHERE enriched_at >= (SELECT MAX(enriched_at) FROM {TARGET_TABLE})
GROUP BY stateroom_sentiment, amenity_sentiment
ORDER BY n DESC
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md ### Sample of enriched rows with comments

# COMMAND ----------

display(spark.sql(f"""
SELECT
    survey_id,
    amenities_comment,
    amenity_sentiment,
    amenity_names,
    size(amenity_embedding) AS embedding_dim
FROM {TARGET_TABLE}
WHERE amenities_comment IS NOT NULL
ORDER BY survey_id DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md ### Top amenities mentioned

# COMMAND ----------

display(spark.sql(f"""
SELECT amenity, COUNT(*) AS mentions
FROM (
    SELECT explode(amenity_names) AS amenity
    FROM {TARGET_TABLE}
    WHERE amenity_names IS NOT NULL
)
GROUP BY amenity
ORDER BY mentions DESC
LIMIT 15
"""))

# COMMAND ----------

print(f"Done. {new_count:,} rows enriched into {TARGET_TABLE}.")
