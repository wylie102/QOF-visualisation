select
    PRACTICE_CODE as "practice_code",
    INDICATOR_CODE as "indicator_code",
    MEASURE as "measure",
    VALUE as "value",
    2023 as reporting_year
from
    {{ source('qof', 'achievement_2022_2023') }}
