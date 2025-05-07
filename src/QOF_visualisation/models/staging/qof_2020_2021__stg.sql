copy (
    select
        "PRACTICE_CODE" as practice_code,
        "INDICATOR_CODE" as indicator_code,
        "MEASURE" as measure,
        "VALUE" as value
    from
        read_csv('sources/QOF_2020_2021/ACHIEVEMENT_2021_v2.csv')
)

to 'sources/qof_achievement_2020_2021.parquet' (format parquet);
