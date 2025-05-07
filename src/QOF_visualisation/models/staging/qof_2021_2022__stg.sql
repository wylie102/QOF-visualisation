copy (
    select
        "PRACTICE_CODE" as practice_code,
        "INDICATOR_CODE" as indicator_code,
        "MEASURE" as measure,
        "VALUE" as value
    from
        read_csv('sources/QOF_2021_2022/ACHIEVEMENT_2122.csv')
)

to 'sources/qof_achievement_2021_2022.parquet' (format parquet);
