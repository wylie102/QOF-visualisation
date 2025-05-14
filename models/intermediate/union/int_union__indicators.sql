select * from {{ ref('stg_ind__indicators_2019_2020') }}
union all
select * from {{ ref('stg_ind__indicators_2020_2021') }}
union all
select * from {{ ref('stg_ind__indicators_2021_2022') }}
union all
select * from {{ ref('stg_ind__indicators_2022_2023') }}
union all
select * from {{ ref('stg_ind__indicators_2023_2024') }}
