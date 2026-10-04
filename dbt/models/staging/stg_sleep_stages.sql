select s.record_id as session_record_id, s.batch_id, s.delivery_sequence, s.record_source,
    s.participant_id, s.wake_date, try_cast(x.key as bigint) as stage_position,
    {{ text_field('stage', 'x.value') }} as stage_type,
    {{ text_field('start_time', 'x.value') }} as start_original,
    {{ text_field('end_time', 'x.value') }} as end_original,
    {{ utc_timestamp(text_field('start_time', 'x.value')) }} as start_at_utc,
    {{ utc_timestamp(text_field('end_time', 'x.value')) }} as end_at_utc,
    {{ number_field('duration_min', 'x.value') }} as duration_min_reported
from {{ ref('stg_sleep_sessions') }} s,
lateral json_each(case when json_type(s.payload_json, '$.stages') = 'ARRAY'
                      then json_extract(s.payload_json, '$.stages') else '[]'::json end) x
