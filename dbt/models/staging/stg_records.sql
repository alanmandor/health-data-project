{{ config(materialized='table') }}
with extracted as (
    select r.*, b.batch_id, b.delivery_sequence, b.source_system as record_source,
        coalesce({{ text_field('_op') }}, 'upsert') as operation,
        {{ text_field('participant_id') }} as participant_id,
        {{ text_field('device_id') }} as device_id,
        {{ text_field('timestamp') }} as timestamp_original,
        try_cast({{ text_field('date') }} as date) as observation_date,
        {{ integer_field('age') }} as age_at_enrollment,
        {{ number_field('height_cm') }} as height_cm,
        {{ text_field('gender') }} as gender,
        {{ text_field('chronotype') }} as chronotype,
        {{ integer_field('max_heart_rate') }} as max_heart_rate,
        {{ text_field('wear_site') }} as wear_site,
        try_cast({{ text_field('calibration_date') }} as date) as calibration_date,
        {{ text_field('firmware_version') }} as firmware_version_reported,
        {{ text_field('device_label') }} as device_label,
        {{ integer_field('steps') }} as steps_reported,
        {{ text_field('_unit') }} as unit_reported,
        {{ integer_field('heart_rate_bpm') }} as heart_rate_bpm,
        {{ text_field('sleep_onset') }} as sleep_onset_original,
        {{ text_field('sleep_end') }} as sleep_end_original,
        {{ number_field('efficiency_pct') }} as efficiency_pct_reported,
        {{ number_field('restlessness') }} as restlessness,
        {{ integer_field('fatigue_score') }} as fatigue_score,
        {{ integer_field('stress_score') }} as stress_score,
        {{ integer_field('readiness_score') }} as readiness_score,
        {{ integer_field('sleep_quality_score') }} as sleep_quality_score
    from {{ source('raw', 'source_records') }} r
    join {{ source('raw', 'batch_records') }} b using (record_id)
), normalized as (
    select *, {{ utc_timestamp('timestamp_original') }} as event_at_utc,
        {{ utc_timestamp('sleep_onset_original') }} as sleep_onset_utc,
        {{ utc_timestamp('sleep_end_original') }} as sleep_end_utc,
        case when unit_reported = 'steps_per_hour' then steps_reported / 60.0
             when unit_reported is null or unit_reported = 'steps' then steps_reported::double end as steps_normalized,
        nullif(regexp_extract(device_label, 'fw([0-9]+[.][0-9]+(?:[.][0-9]+)?)', 1), '') as firmware_version_from_label
    from extracted
)
select *, timezone('{{ var("study_timezone") }}', event_at_utc)::date as local_date,
    coalesce(firmware_version_reported, firmware_version_from_label) as firmware_version,
    case when firmware_version_reported is not null then 'reported'
         when firmware_version_from_label is not null then 'device_label' else 'missing' end as firmware_provenance
from normalized
