select
    region_ods_code as organisation_code,
    region_ons_code,
    region_name as organisation_name,
    indicator_code,
    output_description,
    group_code,
    group_description,
    avg(percentage_patients_achieved) as percentage_patients_achieved,
    avg(percentage_points_achieved) as percentage_points_achieved,
    avg(lat) as lat,
    avg(lng) as lng,
    reporting_year
from
    {{ ref("dim__practice_summary") }}
where
    numerator is not null
    and denominator is not null
group by
    all
