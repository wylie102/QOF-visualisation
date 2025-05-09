create or replace temp table qof_temp as
select
    PRACTICE_CODE as "practice_code",
    INDICATOR_CODE as "indicator_code",
    MEASURE as "measure",
    VALUE as "value"
from
    read_csv('sources/QOF_2019_2020/ACHIEVEMENT_1920.csv');
