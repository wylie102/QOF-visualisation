{{
  config(
    materialized = 'table',
    unique_key = ['indicator_group_code']
  )
}}

WITH indicator_groups AS (
    SELECT DISTINCT
        group_code as indicator_group_code,
        group_description as indicator_group_description
    FROM {{ ref('int_union__indicators') }}
    WHERE group_code IS NOT NULL
)

SELECT
    indicator_group_code,
    indicator_group_description
FROM indicator_groups
