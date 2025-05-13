select
    PRACTICE_CODE as "practice_code",
    INDICATOR_CODE as "indicator_code",
    MEASURE as "measure",
    VALUE as "value"
from
    {{ source('qof', 'achievement_2020_2021') }}
