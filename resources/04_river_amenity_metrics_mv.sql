-- Amenity-grain metric view: measures broken down by amenity_name.
-- Grain: one row per (survey, amenity_name). Because surveys mentioning N amenities
-- contribute N rows, every measure here is weighted by amenity mentions, not by
-- distinct surveys. For survey-grain unbiased measures use river_survey_enriched_mv.
CREATE OR REPLACE VIEW serverless_stable_yrljz5_catalog.survey_demo.river_amenity_metrics_mv
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: serverless_stable_yrljz5_catalog.survey_demo.river_survey_enriched_amenity_exploded
comment: "Amenity-grain metrics for River cruises. One row per (survey, amenity_name) after exploding amenity_names. Lets you GROUP BY `Amenity Name`. CAVEAT: a survey mentioning N amenities contributes N rows, so measures here are weighted by amenity mentions, not by distinct surveys. For survey-grain unbiased measures use river_survey_enriched_mv."
dimensions:
  - name: Amenity Name
    expr: amenity_name
    comment: "Lower-cased, trimmed amenity name extracted by ai_extract."
  - name: Amenity Sentiment
    expr: amenity_sentiment
    comment: "ai_classify sentiment for the full amenity comment (positive/neutral/negative)."
  - name: Ship Name
    expr: ship_name
  - name: Ship Class
    expr: ship_class
  - name: Itinerary Name
    expr: itinerary_name
  - name: Sailing Days
    expr: sailing_days
  - name: Sailing Date
    expr: sailing_date
measures:
  - name: Mention Count
    expr: COUNT(1)
    comment: "Rows at the (survey, amenity) grain = total amenity mentions."
  - name: Distinct Surveys Mentioning
    expr: COUNT(DISTINCT survey_id)
    comment: "Number of distinct surveys that mentioned the amenity."
  - name: NPS
    expr: 100.0 * (SUM(CASE WHEN likelihood_to_recommend >= 9 THEN 1 ELSE 0 END) - SUM(CASE WHEN likelihood_to_recommend <= 6 THEN 1 ELSE 0 END)) / NULLIF(COUNT(1), 0)
    comment: "NPS of the subset of surveys that mentioned a given amenity. Weighted by mentions, not by distinct surveys."
  - name: Total Amount Paid
    expr: SUM(amount_paid)
    comment: "Sum of amount_paid attributed per amenity mention. CAVEAT: same booking's amount_paid is counted once per amenity mentioned, so this is NOT the true revenue total."
  - name: Avg Amount Paid
    expr: AVG(amount_paid)
    comment: "Average amount paid per mention of this amenity."
  - name: Avg Overall Experience
    expr: AVG(overall_experience)
  - name: Avg Likelihood to Book Another
    expr: AVG(likelihood_to_book_another)
  - name: Avg Stateroom Satisfaction
    expr: AVG(stateroom_satisfaction)
  - name: Avg Amenities Satisfaction
    expr: AVG(amenities_satisfaction)
$$;
