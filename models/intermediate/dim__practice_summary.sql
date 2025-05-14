with practice_metadata as (
    select
        practice_code,
        practice_name,
        postcode,
        pcn_ods_code,
        pcn_name,
        sub_icb_ods_code,
        sub_icb_ons_code,
        sub_icb_name,
        icb_ods_code,
        icb_ons_code,
        icb_name,
        region_ods_code,
        region_ons_code,
        region_name,
        open_date,
        closed_date,
        telephone_no,
        short_address,
        long_address,
        lat,
        lng,
        reporting_year
    from
        {{ ref('int__practice_metadata') }}
)

select
    percent_achieved.practice_code,
    practice_metadata.practice_name,
    practice_metadata.postcode,
    practice_metadata.pcn_ods_code,
    practice_metadata.pcn_name,
    practice_metadata.sub_icb_ods_code,
    practice_metadata.sub_icb_ons_code,
    practice_metadata.sub_icb_name,
    practice_metadata.icb_ods_code,
    practice_metadata.icb_ons_code,
    practice_metadata.icb_name,
    practice_metadata.region_ods_code,
    practice_metadata.region_ons_code,
    practice_metadata.region_name,
    practice_metadata.open_date,
    practice_metadata.closed_date,
    practice_metadata.telephone_no,
    practice_metadata.short_address,
    practice_metadata.long_address,
    practice_metadata.lat,
    practice_metadata.lng,
    percent_achieved.indicator_code,
    percent_achieved.service_id,
    percent_achieved.ruleset_id,
    percent_achieved.output_description,
    percent_achieved.type,
    percent_achieved.numerator,
    percent_achieved.denominator,
    percent_achieved.register,
    percent_achieved.achieved_points,
    percent_achieved.indicator_point_value,
    percent_achieved.group_code,
    percent_achieved.group_description,
    percent_achieved.domain_code,
    percent_achieved.domain_description,
    percent_achieved.patient_list_type,
    percent_achieved.percentage_patients_achieved,
    percent_achieved.percentage_points_achieved,
    percent_achieved.reporting_year
from
    {{ ref('int__percent_achieved') }} as percent_achieved
left join
    practice_metadata
    on
        percent_achieved.practice_code = practice_metadata.practice_code
        and percent_achieved.reporting_year = practice_metadata.reporting_year
