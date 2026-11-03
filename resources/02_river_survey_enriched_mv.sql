-- Main metric view: survey-grain KPIs for River cruises.
-- Grain: one row per completed survey. Measures: Survey Count, NPS, Total/Avg
-- Amount Paid, and 0-10 averages for overall / book-another / stateroom / amenities.
-- For per-amenity breakdowns, use river_amenity_metrics_mv.
CREATE OR REPLACE VIEW serverless_stable_yrljz5_catalog.survey_demo.river_survey_enriched_mv
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: serverless_stable_yrljz5_catalog.survey_demo.river_survey_enriched_joined
comment: "Survey-grain metrics for River cruises. One row per completed survey, joined to booking, sailing, and ship dimensions. For per-amenity breakdowns, use river_amenity_metrics_mv."
dimensions:
  - name: Ship Name
    expr: ship_name
    comment: "Ship name (Viking prefix already stripped in the ship dim)."
  - name: Ship Class
    expr: ship_class
    comment: "Longship / Longship-Seine / Douro / Nile / Mekong / Mississippi."
  - name: Itinerary Name
    expr: itinerary_name
    comment: "Named Viking itinerary (e.g. Rhine Getaway, Grand European Tour)."
  - name: Sailing Days
    expr: sailing_days
    comment: "Duration of the sailing in days."
  - name: Amenities Mentioned
    expr: array_join(amenity_names, ', ')
    comment: "Amenities named in the amenity comment, joined with ', '. Display-only. For per-amenity analysis, use river_amenity_metrics_mv."
  - name: Sailing Date
    expr: sailing_date
  - name: Sailing Month
    expr: DATE_TRUNC('MONTH', sailing_date)
measures:
  - name: Survey Count
    expr: COUNT(1)
    comment: "Number of surveys."
  - name: NPS
    expr: 100.0 * (SUM(CASE WHEN likelihood_to_recommend >= 9 THEN 1 ELSE 0 END) - SUM(CASE WHEN likelihood_to_recommend <= 6 THEN 1 ELSE 0 END)) / NULLIF(COUNT(1), 0)
    comment: "Standard Net Promoter Score: %Promoters(9-10) - %Detractors(0-6), Passives(7-8) excluded. Range: -100 to +100."
  - name: Total Amount Paid
    expr: SUM(amount_paid)
    comment: "Sum of amount_paid across surveyed bookings (not the whole fleet)."
  - name: Avg Amount Paid
    expr: AVG(amount_paid)
    comment: "Average amount paid per surveyed booking."
  - name: Avg Overall Experience
    expr: AVG(overall_experience)
    comment: "Average 0-10 overall-experience rating."
  - name: Avg Likelihood to Book Another
    expr: AVG(likelihood_to_book_another)
    comment: "Average 0-10 likelihood-to-book-another rating."
  - name: Avg Stateroom Satisfaction
    expr: AVG(stateroom_satisfaction)
    comment: "Average 0-10 stateroom-satisfaction rating."
  - name: Avg Amenities Satisfaction
    expr: AVG(amenities_satisfaction)
    comment: "Average 0-10 amenities-satisfaction rating."
$$;
