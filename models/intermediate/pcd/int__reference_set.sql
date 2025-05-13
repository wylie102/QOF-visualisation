select
    service_id,
    ruleset_id,
    output_id,
    output_description,
    type
from
    {{ ref('stg_pcd__reference_set') }}
