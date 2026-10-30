# Databricks notebook source
# MAGIC %md
# MAGIC # Incrementally add new rows to `river_survey_amenity_search`
# MAGIC
# MAGIC Picks up any newly enriched survey rows from
# MAGIC `river_survey_enriched` (those with an embedding, i.e. an amenity
# MAGIC comment) that are not yet present in `river_survey_amenity_search`,
# MAGIC and inserts them. The downstream Lakebase synced table then re-reads
# MAGIC this table on its next sync run.
# MAGIC
# MAGIC Uses `MERGE INTO ... WHEN NOT MATCHED THEN INSERT *` so the operation is
# MAGIC safe to re-run and never duplicates on `survey_id`.

# COMMAND ----------

CATALOG = "serverless_stable_yrljz5_catalog"
SCHEMA  = "survey_demo"
SOURCE  = f"{CATALOG}.{SCHEMA}.river_survey_enriched"
TARGET  = f"{CATALOG}.{SCHEMA}.river_survey_amenity_search"

print(f"source = {SOURCE}")
print(f"target = {TARGET}")

# COMMAND ----------

# MAGIC %md ## 1. Row counts BEFORE

# COMMAND ----------

before = spark.sql(f"""
SELECT
    (SELECT COUNT(*) FROM {SOURCE} WHERE amenity_embedding IS NOT NULL) AS source_eligible,
    (SELECT COUNT(*) FROM {TARGET})                                     AS target_before
""").collect()[0]
print(f"source_eligible rows : {before['source_eligible']:,}")
print(f"target rows (before) : {before['target_before']:,}")

# COMMAND ----------

# MAGIC %md ## 2. MERGE new rows in

# COMMAND ----------

merge_result = spark.sql(f"""
MERGE INTO {TARGET} AS tgt
USING (
    SELECT
        survey_id,
        booking_id,
        amenities_comment,
        amenity_sentiment,
        amenity_names,
        amenity_embedding,
        enriched_at
    FROM {SOURCE}
    WHERE amenity_embedding IS NOT NULL
) AS src
ON tgt.survey_id = src.survey_id
WHEN NOT MATCHED THEN INSERT *
""")

# Delta's MERGE returns rows: num_affected_rows, num_updated_rows, num_deleted_rows, num_inserted_rows
display(merge_result)

# COMMAND ----------

# MAGIC %md ## 3. Row counts AFTER

# COMMAND ----------

after = spark.sql(f"SELECT COUNT(*) AS target_after FROM {TARGET}").collect()[0]
added = after["target_after"] - before["target_before"]
print(f"target rows (after)  : {after['target_after']:,}")
print(f"rows added this run  : {added:,}")

# COMMAND ----------

print(f"Done. Added {added:,} new rows to {TARGET}.")
