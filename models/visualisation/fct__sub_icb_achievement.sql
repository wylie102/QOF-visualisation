select
    sub_icb_ods_code as organisation_code,
    sub_icb_ons_code,
    sub_icb_name as organisation_name,
    indicator_code,
    output_description,
    group_code,
    group_description,
    round(avg(percentage_patients_achieved), 2) as percentage_patients_achieved,
    round(avg(percentage_points_achieved), 2) as percentage_points_achieved,
    round(avg(lat), 2) as lat,
    round(avg(lng), 2) as lng,
    reporting_year
from
    {{ ref("dim__practice_summary") }}
where
    numerator is not null
    and denominator is not null
group by
    all
