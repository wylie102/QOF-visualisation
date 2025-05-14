select
    practice_code as organisation_code,
    practice_name as organisation_name,
    indicator_code,
    output_description,
    group_code,
    group_description,
    percentage_patients_achieved,
    percentage_points_achieved,
    lat,
    lng,
    reporting_year
from
    {{ ref("dim__practice_summary") }}
where
    numerator is not null
    and denominator is not null
group by
    all
