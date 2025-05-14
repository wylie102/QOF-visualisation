{{
  config(
    materialized = 'table',
    unique_key = ['organisation_code', 'indicator_code', 'reporting_year', 'level']
  )
}}

WITH base_indicator_groups AS (
    -- Get the standardized indicator group mappings
    SELECT DISTINCT
        indicator_group_code,
        indicator_group_description
    FROM {{ ref('dim__indicator_groups') }}
),

practice_ach AS (
    SELECT
        organisation_code,
        organisation_name,
        indicator_code,
        ig.indicator_group_description as group_description,
        reporting_year,
        AVG(percentage_patients_achieved) as avg_achievement,
        'Practice' as level
    FROM {{ ref('fct__practice_achievement') }} p
    LEFT JOIN base_indicator_groups ig 
        ON p.group_code = ig.indicator_group_code
    WHERE percentage_patients_achieved IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),

pcn_ach AS (
    SELECT
        organisation_code,
        organisation_name,
        indicator_code,
        ig.indicator_group_description as group_description,
        reporting_year,
        AVG(percentage_patients_achieved) as avg_achievement,
        'PCN' as level
    FROM {{ ref('fct__pcn_achievement') }} p
    LEFT JOIN base_indicator_groups ig 
        ON p.group_code = ig.indicator_group_code
    WHERE percentage_patients_achieved IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),

sub_icb_ach AS (
    SELECT
        organisation_code,
        organisation_name,
        indicator_code,
        ig.indicator_group_description as group_description,
        reporting_year,
        AVG(percentage_patients_achieved) as avg_achievement,
        'Sub-ICB' as level
    FROM {{ ref('fct__sub_icb_achievement') }} p
    LEFT JOIN base_indicator_groups ig 
        ON p.group_code = ig.indicator_group_code
    WHERE percentage_patients_achieved IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),

icb_ach AS (
    SELECT
        organisation_code,
        organisation_name,
        indicator_code,
        ig.indicator_group_description as group_description,
        reporting_year,
        AVG(percentage_patients_achieved) as avg_achievement,
        'ICB' as level
    FROM {{ ref('fct__icb_achievement') }} p
    LEFT JOIN base_indicator_groups ig 
        ON p.group_code = ig.indicator_group_code
    WHERE percentage_patients_achieved IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),

region_ach AS (
    SELECT
        organisation_code,
        organisation_name,
        indicator_code,
        ig.indicator_group_description as group_description,
        reporting_year,
        AVG(percentage_patients_achieved) as avg_achievement,
        'Region' as level
    FROM {{ ref('fct__region_achievement') }} p
    LEFT JOIN base_indicator_groups ig 
        ON p.group_code = ig.indicator_group_code
    WHERE percentage_patients_achieved IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),

national_ach AS (
    SELECT
        'National' as organisation_code, -- Placeholder for national level
        'National Average' as organisation_name,
        indicator_code,
        ig.indicator_group_description as group_description,
        reporting_year,
        AVG(percentage_patients_achieved) as avg_achievement,
        'National' as level
    FROM {{ ref('fct__national_achievement') }} p
    LEFT JOIN base_indicator_groups ig 
        ON p.group_code = ig.indicator_group_code
    WHERE percentage_patients_achieved IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM practice_ach
UNION ALL
SELECT * FROM pcn_ach
UNION ALL
SELECT * FROM sub_icb_ach
UNION ALL
SELECT * FROM icb_ach
UNION ALL
SELECT * FROM region_ach
UNION ALL
SELECT * FROM national_ach
