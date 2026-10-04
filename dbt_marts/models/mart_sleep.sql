with stages as (
    select session_version_id,
        sum(duration_min_reported) filter(where stage_type='deep') as deep_min
    from vault.sat_sleep_stage group by session_version_id
)
select q.parent_hk as sleep_session_hk, q.study_hk, q.participant_hk, s.study_sk, p.participant_sk,
    strftime(q.analysis_date,'%Y%m%d')::integer as date_sk, q.analysis_date as wake_date,
    q.sleep_onset_utc, q.sleep_end_utc, q.sleep_duration_min_reported as reported_stage_sleep_min,
    coalesce(t.deep_min,0) as reported_stage_deep_min,
    100*coalesce(t.deep_min,0)/nullif(q.sleep_duration_min_reported,0) as deep_sleep_pct_from_reported_stages,
    q.efficiency_pct_reported, q.restlessness, q.version_id as session_version_id,
    q.quality_status, q.rule_version
from {{ ref('mart_records') }} q join marts.dim_study s using(study_hk)
join marts.dim_participant p on p.participant_hk=q.participant_hk and p.is_current
left join stages t on t.session_version_id=q.version_id where q.source_name='sleep'
