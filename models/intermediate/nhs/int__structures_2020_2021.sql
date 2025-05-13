select
    region_ods_code,
    region_ons_code,
    region_name,
    icb_ods_code,
    icb_ons_code,
    icb_name,
    sub_icb_ods_code,
    sub_icb_ons_code,
    sub_icb_name,
    pcn_ods_code,
    pcn_name,
    practice_code,
    practice_name
from
    {{ ref('stg_nhs__structures_2020_2021') }}
