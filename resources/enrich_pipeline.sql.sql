-- Databricks notebook source
-- Spark Declarative Pipeline: incremental survey enrichment on serverless.
--
-- Converts the previous `enrich_survey_incremental` notebook (which did a manual
-- anti-join + append) into a declarative Streaming Table. The pipeline:
--
--   1. Reads NEW rows from the source Delta table via STREAM() — Delta's native
--      change feed drives incremental processing automatically. No anti-join,
--      no watermark table, no manual bookkeeping.
--   2. Applies three AI Functions per row:
--        - ai_classify  → stateroom_sentiment and amenity_sentiment
--        - ai_extract   → amenity_names (ARRAY<STRING>)
--        - ai_query     → amenity_embedding (1024-dim ARRAY<FLOAT>) via the
--                         databricks-gte-large-en Foundation Model endpoint
--   3. Writes to river_survey_enriched_sdp. Separate table name from the
--      notebook target so this proof build runs cleanly alongside the existing
--      Delta table that downstream metric views / Lakebase sync already read
--      from. Switching production downstream to the SDP target is a one-line
--      change to the metric view SQL.
--
-- Catalog / schema come from the pipeline configuration
-- (serverless_stable_yrljz5_catalog.survey_demo), so dataset names stay bare.

CREATE OR REFRESH STREAMING TABLE river_survey_enriched_sdp
(
  CONSTRAINT valid_survey_id EXPECT (survey_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Enriched surveys produced by a Spark Declarative Pipeline. Replaces the enrich_survey_incremental notebook/Job. Each new survey row is streamed in, run through ai_classify + ai_extract + ai_query, and appended here. Downstream metric views and Lakebase sync can repoint to this table when ready.'
AS SELECT
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
                '{"amenity_names":{"type":"array","items":{"type":"string"}}}',
                map('version','2.0')
              ):response:amenity_names::STRING,
              'ARRAY<STRING>'
            )
  END AS amenity_names,
  CASE WHEN amenities_comment IS NOT NULL
       THEN ai_query(
              'databricks-gte-large-en',
              amenities_comment,
              returnType => 'ARRAY<FLOAT>'
            )
  END AS amenity_embedding,
  current_timestamp() AS enriched_at
FROM STREAM river_survey;
