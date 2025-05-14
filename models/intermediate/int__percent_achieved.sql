with pivoted as (
    select
        practice_code,
        indicator_code,
        max(case when measure = 'NUMERATOR' then value end) as numerator,
        max(case when measure = 'DENOMINATOR' then value end) as denominator,
        max(case when measure = 'REGISTER' then value end) as register,
        max(case when measure = 'ACHIEVED_POINTS' then value end) as achieved_points,
        reporting_year
    from
        {{ ref('int_union__qof_achievement') }}
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
        patient_list_type,
        reporting_year
    from
        {{ ref('int_union__indicators') }}
),

reference_set as (
    select
        indicator_code,
        service_id,
        ruleset_id,
        output_description,
        type
    from
        {{ ref('stg_pcd__reference_set') }}
)

select
    pivoted.practice_code,
    pivoted.indicator_code,
    reference_set.service_id,
    reference_set.ruleset_id,
    reference_set.output_description,
    reference_set.type,
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
    case
        when pivoted.numerator is not null and pivoted.denominator is not null and pivoted.denominator != 0
            then (pivoted.numerator * 100 / pivoted.denominator)
    end as percentage_patients_achieved,
    case
        when pivoted.achieved_points is not null and indicators.indicator_point_value is not null and indicators.indicator_point_value != 0
            then (pivoted.achieved_points * 100 / indicators.indicator_point_value)
    end as percentage_points_achieved,
    pivoted.reporting_year
from pivoted
left join indicators
    on
        pivoted.indicator_code = indicators.indicator_code
        and pivoted.reporting_year = indicators.reporting_year
left join reference_set
    on
        pivoted.indicator_code = reference_set.indicator_code
