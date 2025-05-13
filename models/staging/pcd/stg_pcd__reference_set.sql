select
    Service_ID as "service_id",
    Ruleset_ID as "ruleset_id",
    Output_ID as "output_id",
    Output_Description as "output_description",
    Type as "type"
from
    {{ source('pcd', 'reference_set') }}
