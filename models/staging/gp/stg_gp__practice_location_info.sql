select
    practice_code,
    practice_name,
    address_line_1,
    address_line_2,
    address_line_3,
    address_line_4,
    address_line_5,
    postcode,
    cast(strptime(cast(open_date as varchar), '%Y%m%d') as date) as open_date,
    cast(strptime(cast(closed_date as varchar), '%Y%m%d') as date) as closed_date,
    telephone_no,
    short_address,
    long_address,
    lat,
    long
from
    {{ source('gp', 'practice_location_info') }}
