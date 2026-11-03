-- Supporting view: explode amenity_names + the same joins as river_survey_enriched_joined.
-- Grain: one row per (survey, amenity_name). Only surveys with amenity_names populated.
-- Explode happens inside a CTE because LATERAL VIEW must come before LEFT JOIN in Spark SQL.
CREATE OR REPLACE VIEW serverless_stable_yrljz5_catalog.survey_demo.river_survey_enriched_amenity_exploded AS
WITH exploded AS (
    SELECT
        e.survey_id,
        e.booking_id,
        e.overall_experience,
        e.likelihood_to_recommend,
        e.likelihood_to_book_another,
        e.stateroom_satisfaction,
        e.amenities_satisfaction,
        e.amenity_sentiment,
        LOWER(TRIM(a.amenity_name)) AS amenity_name
    FROM serverless_stable_yrljz5_catalog.survey_demo.river_survey_enriched e
         LATERAL VIEW EXPLODE(e.amenity_names) a AS amenity_name
    WHERE e.amenity_names IS NOT NULL
)
SELECT
    ex.survey_id,
    ex.booking_id,
    ex.overall_experience,
    ex.likelihood_to_recommend,
    ex.likelihood_to_book_another,
    ex.stateroom_satisfaction,
    ex.amenities_satisfaction,
    ex.amenity_sentiment,
    ex.amenity_name,
    b.amount_paid,
    sa.itinerary_name,
    sa.sailing_date,
    sa.sailing_days,
    sh.ship_name,
    sh.ship_class
FROM exploded ex
LEFT JOIN serverless_stable_yrljz5_catalog.survey_demo.river_booking     b  ON ex.booking_id = b.booking_id
LEFT JOIN serverless_stable_yrljz5_catalog.survey_demo.river_sailing_dim sa ON b.sailing_id   = sa.sailing_id
LEFT JOIN serverless_stable_yrljz5_catalog.survey_demo.river_ship_dim    sh ON sa.ship_id    = sh.ship_id;
