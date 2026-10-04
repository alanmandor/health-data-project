select v.session_version_id, t.stage_position, v.sleep_session_hk, t.stage_type,
    t.start_at_utc, t.end_at_utc, t.duration_min_reported,
    t.stage_type='awake' and t.end_at_utc>v.sleep_end_utc as is_post_boundary_awake,
    case when t.stage_type='awake' and t.end_at_utc>v.sleep_end_utc then 'provisional_boundary_warning' end as warning_status
from {{ ref('mart_sleep') }} v join vault.sat_sleep_stage t using(session_version_id)
