select
    PRACTICE_CODE as "practice_code",
    INDICATOR_CODE as "indicator_code",
    MEASURE as "measure",
    VALUE as "value",
    2022 as reporting_year
from
    {{ source('qof', 'achievement_2021_2022') }}
