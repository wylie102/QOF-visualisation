copy (
    select
        PRACTICE_CODE as practice_code,
        INDICATOR_CODE as indicator_code,
        MEASURE as measure,
        VALUE as value
    from
        read_csv('sources/QOF_2022_2023/ACHIEVEMENT_*.csv')
)

to 'sources/qof_achievement_2022_2023.parquet' (format parquet);
