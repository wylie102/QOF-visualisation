with pivoted as (
    select
        practice_code,
        indicator_code,
        max(case when measure = 'NUMERATOR' then value end) as numerator,
        max(case when measure = 'DENOMINATOR' then value end) as denominator,
        max(case when measure = 'REGISTER' then value end) as register,
        max(case when measure = 'ACHIEVED_POINTS' then value end) as achieved_points
    from
        {{ ref('stg_qof__achievement_2023_2024') }}
    group by
        all
),

indicators as (
    select
        indicator_code,
        indicator_point_value,
        group_code,
        group_description,
        domain_code,
        domain_description,
        patient_list_type
    from
        {{ ref('stg_ind__achievement_2023_2024') }}
)

select
    pivoted.practice_code,
    pivoted.indicator_code,
    pivoted.numerator,
    pivoted.denominator,
    pivoted.register,
    pivoted.achieved_points,
    indicators.indicator_point_value,
    indicators.group_code,
    indicators.group_description,
    indicators.domain_code,
    indicators.domain_description,
    indicators.patient_list_type,
    round((pivoted.numerator / pivoted.denominator) * 100, 1) as percentage_patients_achieved,
    round((pivoted.achieved_points / indicators.indicator_point_value) * 100, 1) as percentage_points_achieved
from pivoted
left join indicators
    on pivoted.practice_code = indicators.practice_code
