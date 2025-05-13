select
    REGION_ODS_CODE as "region_ods_code",
    REGION_ONS_CODE as "region_ons_code",
    REGION_NAME as "region_name",
    ICB_ODS_CODE as "icb_ods_code",
    ICB_ONS_CODE as "icb_ons_code",
    ICB_NAME as "icb_name",
    SUB_ICB_LOC_ODS_CODE as "sub_icb_ods_code",
    SUB_ICB_LOC_ONS_CODE as "sub_icb_ons_code",
    SUB_ICB_LOC_NAME as "sub_icb_name",
    PCN_ODS_CODE as "pcn_ods_code",
    PCN_NAME as "pcn_name",
    PRACTICE_CODE as "practice_code",
    PRACTICE_NAME as "practice_name"
from
    {{ source('nhs', 'structures_2021_2022') }}
