copy (
    select
        "column00" as practice_code,
        "column01" as practice_name,
        "column04" as address_line_1,
        "column05" as address_line_2,
        "column06" as address_line_3,
        "column07" as address_line_4,
        "column08" as address_line_5,
        "column09" as postcode,
        "column10" as open_date,
        "column11" as closed_date,
        "column17" as telephone_number
    from
        'sources/epraccur/epraccur.csv'
) to 'sources.practice_location_info.parquet' (format parquet);
