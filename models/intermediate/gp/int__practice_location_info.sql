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
    long
from
    {{ ref('stg_gp__practice_location_info') }}
