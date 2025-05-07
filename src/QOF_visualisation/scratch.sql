select
    practice_code,
    indicator_code,
    max(case when measure = 'NUMERATOR' then value end) as numerator,
    max(case when measure = 'DENOMINATOR' then value end) as denominator,
    max(case when measure = 'REGISTER' then value end) as register,
    max(case when measure = 'ACHIEVED_POINTS' then value end) as achieved_points
from qof_grouped
group by all
