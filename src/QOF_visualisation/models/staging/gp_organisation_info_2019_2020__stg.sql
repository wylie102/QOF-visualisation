create temp table qof_map_temp as
select
    REGION_ODS_CODE as "reegion_ods_code",
    REGION_ONS_CODE as "region_ons_code",
    REGION_NAME as "region_name",
    STP_ODS_CODE as "stp_ods_code",
    STP_ONS_CODE as "stp_ons_code",
    STP_NAME as "stp_name",
    CCG_ODS_CODE as "ccg_ods_code",
    CCG_ONS_CODE as "ccg_ons_code",
    CCG_NAME as "ccg_name",
    PCN_ODS_CODE as "pcn_ods_code",
    PCN_NAME as "pcn_name",
    PRACTICE_CODE as "practice_code",
    PRACTICE_NAME as "practice_name"
from read_csv('sources/QOF_2019_2020/MAPPING_NHS_GEOGRAPHIES_1920.csv');
