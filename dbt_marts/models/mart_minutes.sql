select q.study_hk, q.participant_hk, s.study_sk, p.participant_sk,
    case when g.assignment_quality_status='reported_assignment' then d.device_sk end as device_sk,
    strftime(q.analysis_date,'%Y%m%d')::integer as date_sk, q.event_at_utc,
    q.steps_normalized as steps_per_minute, q.heart_rate_bpm,
    q.version_id as source_version_id, q.quality_status, q.rule_version, q.source_name
from {{ ref('mart_records') }} q
join marts.dim_study s using(study_hk)
join marts.dim_participant p on p.participant_hk=q.participant_hk and p.is_current
left join {{ ref('mart_gold') }} g on g.participant_hk=q.participant_hk
left join vault.hub_device h on h.business_namespace=g.business_namespace and h.device_id=g.device_id
left join marts.dim_device d on d.device_hk=h.device_hk and d.is_current
where q.source_name in ('steps','heart_rate')
