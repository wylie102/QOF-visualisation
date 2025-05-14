{{
  config(
    materialized = 'table'
  )
}}

WITH practice_metrics AS (
    SELECT
        p.organisation_code,
        p.organisation_name,
        p.indicator_code,
        p.group_code,
        p.group_description,
        p.percentage_patients_achieved as practice_achievement,
        p.percentage_points_achieved as practice_points,
        p.lat,
        p.lng,
        p.reporting_year,
        n.percentage_patients_achieved as national_achievement,
        n.percentage_points_achieved as national_points
    FROM {{ ref('fct__practice_achievement') }} p
    LEFT JOIN {{ ref('fct__national_achievement') }} n
        ON p.indicator_code = n.indicator_code
        AND p.reporting_year = n.reporting_year
        AND p.group_code = n.group_code
)

SELECT *,
       practice_achievement - national_achievement as achievement_variance,
       practice_points - national_points as points_variance
FROM practice_metrics
