select * from {{ ref('stg_qof__achievement_2019_2020') }}
union all
select * from {{ ref('stg_qof__achievement_2020_2021') }}
union all
select * from {{ ref('stg_qof__achievement_2021_2022') }}
union all
select * from {{ ref('stg_qof__achievement_2022_2023') }}
union all
select * from {{ ref('stg_qof__achievement_2023_2024') }}
