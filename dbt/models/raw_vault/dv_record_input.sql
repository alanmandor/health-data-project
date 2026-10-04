with identities as (
    select q.*,
        '{{ env_var("STUDY_CODE", "pmdata_2026_01") }}' as study_code,
        {{ dv_hash('participant', ['record_source', 'participant_id']) }} as participant_hk,
        case when device_id is not null then {{ dv_hash('device', ['record_source', 'device_id']) }} end as device_hk,
        {{ dv_hash('study', ["'" ~ env_var('STUDY_CODE', 'pmdata_2026_01') ~ "'"]) }} as study_hk,
        case
            when business_key_hash is null or operation not in ('upsert', 'delete') then false
            when operation = 'delete' then true
            when source_name = 'participants' then device_id is not null and height_cm is not null
                and gender is not null and chronotype is not null
                and try_cast(age_at_enrollment as smallint) is not null and try_cast(max_heart_rate as smallint) is not null
            when source_name = 'device_metadata' then device_id is not null and calibration_date is not null and wear_site is not null
            when source_name = 'steps' then steps_reported is not null
            when source_name = 'heart_rate' then heart_rate_bpm is not null
            when source_name = 'wellness_survey' then try_cast(fatigue_score as smallint) is not null
                and try_cast(stress_score as smallint) is not null and try_cast(readiness_score as smallint) is not null
                and try_cast(sleep_quality_score as smallint) is not null
            when source_name = 'sleep' then sleep_onset_utc is not null and sleep_end_utc > sleep_onset_utc
                and efficiency_pct_reported is not null and restlessness is not null and stage_count > 0 and not invalid_stage
            else false
        end as structurally_loadable
    from {{ ref('record_quality') }} q
), event_hubs as (
    select *,
        case when source_name = 'sleep' then {{ dv_hash('sleep_session', ['study_hk', 'participant_hk', 'observation_date']) }} end as sleep_session_hk,
        case when source_name = 'wellness_survey' then {{ dv_hash('wellness_response', ['study_hk', 'participant_hk', 'observation_date']) }} end as wellness_response_hk
    from identities
)
select *,
    case when device_hk is not null then {{ dv_hash('participant_device', ['participant_hk', 'device_hk']) }} end as participant_device_hk,
    {{ dv_hash('participant_study', ['participant_hk', 'study_hk']) }} as participant_study_hk,
    case when sleep_session_hk is not null then {{ dv_hash('participant_sleep_session', ['participant_hk', 'sleep_session_hk']) }} end as participant_sleep_hk,
    case when wellness_response_hk is not null then {{ dv_hash('participant_wellness_response', ['participant_hk', 'wellness_response_hk']) }} end as participant_wellness_hk
from event_hubs
