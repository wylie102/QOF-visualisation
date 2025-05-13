select
    INDICATOR_CODE as "indicator_code",
    INDICATOR_POINT_VALUE as "indicator_point_value",
    GROUP_CODE as "group_code",
    GROUP_DESCRIPTION as "group_description",
    DOMAIN_CODE as "domain_code",
    DOMAIN_DESCRIPTION as "domain_description",
    PATIENT_LIST_TYPE as "patient_list_type"
from
    {{ source('ind', 'qof_indicators_2021_2022') }}
