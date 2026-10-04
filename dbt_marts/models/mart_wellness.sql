select q.parent_hk as wellness_response_hk, q.study_hk, q.participant_hk, s.study_sk, p.participant_sk,
    strftime(q.analysis_date,'%Y%m%d')::integer as date_sk, q.analysis_date as survey_date,
    q.fatigue_score, q.stress_score, q.readiness_score, q.sleep_quality_score,
    q.version_id as response_version_id, q.quality_status, q.rule_version
from {{ ref('mart_records') }} q join marts.dim_study s using(study_hk)
join marts.dim_participant p on p.participant_hk=q.participant_hk and p.is_current
where q.source_name='wellness_survey'
