-- Supporting view: pre-join survey_enriched with booking + sailing + ship.
-- Grain: one row per completed survey. Used as the source for river_survey_enriched_mv.
CREATE OR REPLACE VIEW serverless_stable_yrljz5_catalog.survey_demo.river_survey_enriched_joined AS
SELECT
    e.survey_id,
    e.booking_id,
    e.overall_experience,
    e.likelihood_to_recommend,
    e.likelihood_to_book_another,
    e.stateroom_satisfaction,
    e.stateroom_sentiment,
    e.stateroom_comment,
    e.amenities_satisfaction,
    e.amenity_sentiment,
    e.amenity_names,
    e.amenities_comment,
    e.enriched_at,
    b.pax_id,
    b.amount_paid,
    sa.sailing_id,
    sa.itinerary_name,
    sa.sailing_date,
    sa.sailing_days,
    sa.end_date,
    sh.ship_id,
    sh.ship_name,
    sh.ship_class,
    sh.berths
FROM serverless_stable_yrljz5_catalog.survey_demo.river_survey_enriched  e
LEFT JOIN serverless_stable_yrljz5_catalog.survey_demo.river_booking     b  ON e.booking_id = b.booking_id
LEFT JOIN serverless_stable_yrljz5_catalog.survey_demo.river_sailing_dim sa ON b.sailing_id = sa.sailing_id
LEFT JOIN serverless_stable_yrljz5_catalog.survey_demo.river_ship_dim    sh ON sa.ship_id   = sh.ship_id;
