with timeline as (
    select *, lag(end_at_utc) over (partition by batch_id, session_record_id order by stage_position) as preceding_end
    from {{ ref('stg_sleep_stages') }}
)
select s.batch_id, s.record_id,
    count(t.stage_position) as stage_count,
    coalesce(bool_or(t.stage_type not in ('light', 'deep', 'rem', 'awake') or t.stage_type is null
        or t.start_at_utc is null or t.end_at_utc is null or t.end_at_utc < t.start_at_utc
        or t.duration_min_reported is null or t.duration_min_reported < 0), false) as invalid_stage,
    coalesce(bool_or(abs(epoch(t.end_at_utc - t.start_at_utc) / 60.0 - t.duration_min_reported) > 0.000001), false) as duration_discrepancy,
    coalesce(bool_or(t.preceding_end is not null and t.preceding_end <> t.start_at_utc), false) as timeline_discrepancy,
    coalesce(bool_or(t.start_at_utc < s.sleep_onset_utc or t.end_at_utc > s.sleep_end_utc), false)
        or min(t.start_at_utc) <> s.sleep_onset_utc
        or max(t.end_at_utc) filter (where t.stage_type <> 'awake') <> s.sleep_end_utc as boundary_discrepancy,
    sum(t.duration_min_reported) filter (where t.stage_type <> 'awake') as sleep_duration_min_reported,
    to_json(list(struct_pack(stage_type := t.stage_type, start_at := epoch_us(t.start_at_utc),
        end_at := epoch_us(t.end_at_utc), duration := t.duration_min_reported) order by t.stage_position)
        filter (where t.stage_position is not null))::varchar as normalized_stages
from {{ ref('stg_sleep_sessions') }} s
left join timeline t on s.batch_id = t.batch_id and s.record_id = t.session_record_id
group by s.batch_id, s.record_id, s.sleep_onset_utc, s.sleep_end_utc
