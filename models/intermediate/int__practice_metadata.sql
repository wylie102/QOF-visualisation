with stg_location_info as (
    select
        practice_code,
        practice_name,
        address_line_1,
        address_line_2,
        address_line_3,
        address_line_4,
        address_line_5,
        postcode,
        open_date,
        closed_date,
        telephone_no,
        short_address,
        long_address,
        lat,
        lng
    from
        {{ ref('stg_gp__practice_location_info') }}
)

select
    nhs_structures.practice_code,
    nhs_structures.practice_name,
    stg_location_info.postcode,
    nhs_structures.pcn_ods_code,
    nhs_structures.pcn_name,
    nhs_structures.sub_icb_ods_code,
    nhs_structures.sub_icb_ons_code,
    nhs_structures.sub_icb_name,
    nhs_structures.icb_ods_code,
    nhs_structures.icb_ons_code,
    nhs_structures.icb_name,
    nhs_structures.region_ods_code,
    nhs_structures.region_ons_code,
    nhs_structures.region_name,
    stg_location_info.open_date,
    stg_location_info.closed_date,
    stg_location_info.telephone_no,
    stg_location_info.short_address,
    stg_location_info.long_address,
    stg_location_info.lat,
    stg_location_info.lng,
    nhs_structures.reporting_year
from
    {{ ref('int_union__nhs_structures') }} as nhs_structures
left join stg_location_info
    on nhs_structures.practice_code = stg_location_info.practice_code
