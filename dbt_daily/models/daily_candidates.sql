-- Recompute compact daily candidates; minute facts and Vault history stay unchanged.
with steps as (
    select study_hk, participant_hk, date_sk, count(*) as n, sum(steps_per_minute) as total
    from marts.fact_steps_minute group by all
), heart as (
    select study_hk, participant_hk, date_sk, count(*) as n, sum(heart_rate_bpm) as total
    from marts.fact_heart_rate_minute group by all
), warnings as (
    select h.participant_hk,
        case when q.source_name in ('steps','heart_rate') then q.local_date else q.observation_date end as local_date,
        sum(len(q.warning_codes)) as warning_count
    from business.quality_current q join vault.hub_participant h
        on h.participant_id=q.participant_id and h.business_namespace=q.record_source
    where q.source_name in ('steps','heart_rate','sleep','wellness_survey') group by all
), spine as (
    select g.*, s.study_timezone,
        case when g.local_date>=s.study_start and g.local_date<s.study_end_exclusive then
            date_diff('minute', timezone(s.study_timezone,g.local_date::timestamp),
                timezone(s.study_timezone,(g.local_date+1)::timestamp)) else 0 end as expected_minutes,
        strftime(g.local_date,'%Y%m%d')::integer as date_sk
    from business.participant_date_state g join marts.dim_study s using(study_hk)
)
select g.study_hk, g.participant_hk, g.local_date, g.expected_minutes,
    coalesce(t.n,0)::integer as accepted_steps_minutes,
    coalesce(h.n,0)::integer as accepted_heart_rate_minutes,
    t.total as observed_steps_total, h.total::integer as heart_rate_sum,
    h.total/nullif(h.n,0) as heart_rate_mean,
    s.sleep_onset_utc, s.sleep_end_utc, s.reported_stage_sleep_min, s.reported_stage_deep_min,
    s.deep_sleep_pct_from_reported_stages, s.efficiency_pct_reported, s.restlessness,
    w.fatigue_score, w.stress_score, w.readiness_score, w.sleep_quality_score,
    t.n is not null as has_steps, h.n is not null as has_heart_rate,
    s.session_version_id is not null as has_sleep, w.response_version_id is not null as has_wellness,
    coalesce(a.warning_count,0)::integer as warning_count,
    case when coalesce(a.warning_count,0)>0 then 'accepted_with_warning' else 'accepted' end as quality_status
from spine g left join steps t using(study_hk,participant_hk,date_sk)
left join heart h using(study_hk,participant_hk,date_sk)
left join marts.fact_sleep_session s on s.study_hk=g.study_hk and s.participant_hk=g.participant_hk and s.wake_date=g.local_date
left join marts.fact_wellness_daily w on w.study_hk=g.study_hk and w.participant_hk=g.participant_hk and w.survey_date=g.local_date
left join warnings a on a.participant_hk=g.participant_hk and a.local_date=g.local_date
