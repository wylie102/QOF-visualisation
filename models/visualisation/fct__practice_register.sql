select *
from
    {{ ref('dim__practice_summary') }}
where
    register is not null
