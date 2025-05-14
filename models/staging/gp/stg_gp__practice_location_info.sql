with stg_location_info as (
    select distinct *
    from {{ source('gp', 'practice_location_info') }}
)

select
    stg_location_info.practice_code,
    stg_location_info.practice_name,
    stg_location_info.address_line_1,
    stg_location_info.address_line_2,
    stg_location_info.address_line_3,
    stg_location_info.address_line_4,
    stg_location_info.address_line_5,
    stg_location_info.postcode,
    cast(strptime(cast(stg_location_info.open_date as varchar), '%Y%m%d') as date) as open_date,
    cast(strptime(cast(stg_location_info.closed_date as varchar), '%Y%m%d') as date) as closed_date,
    stg_location_info.telephone_no,
    stg_location_info.short_address,
    stg_location_info.long_address,
    stg_location_info.lat,
    stg_location_info.long as lng
from stg_location_info
