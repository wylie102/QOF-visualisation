select
    REGION_ODS_CODE as "region_ods_code",
    REGION_ONS_CODE as "region_ons_code",
    REGION_NAME as "region_name",
    STP_ODS_CODE as "icb_ods_code",
    STP_ONS_CODE as "icb_ons_code",
    STP_NAME as "icb_name",
    CCG_ODS_CODE as "sub_icb_ods_code",
    CCG_ONS_CODE as "sub_icb_ons_code",
    CCG_NAME as "sub_icb_name",
    PCN_ODS_CODE as "pcn_ods_code",
    PCN_NAME as "pcn_name",
    PRACTICE_CODE as "practice_code",
    PRACTICE_NAME as "practice_name",
    2021 as reporting_year
from
    {{ source('nhs', 'structures_2020_2021') }}
