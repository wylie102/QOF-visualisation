select * from {{ ref('stg_nhs__structures_2019_2020') }}
union all
select * from {{ ref('stg_nhs__structures_2020_2021') }}
union all
select * from {{ ref('stg_nhs__structures_2021_2022') }}
union all
select * from {{ ref('stg_nhs__structures_2022_2023') }}
union all
select * from {{ ref('stg_nhs__structures_2023_2024') }}
