with profiles as (
    select *, count(*) over(partition by record_source, device_id) as device_owners
    from {{ ref('mart_records') }} where source_name='participants'
)
select p.participant_hk, p.participant_id, p.record_source as business_namespace,
    case when p.device_owners=1 then p.device_id end as device_id,
    case when p.device_owners=1 then m.wear_site end as wear_site,
    case when p.device_owners=1 and d.device_sk is not null then m.firmware_version end as firmware_version,
    case when p.device_owners=1 and d.device_sk is not null then m.firmware_provenance end as firmware_provenance,
    case when p.device_owners=1 then m.calibration_date end as calibration_date,
    case when p.device_owners>1 then 'ambiguous_assignment'
         when m.version_id is null then 'pending_metadata'
         when d.device_sk is null then 'ambiguous_metadata'
         else 'reported_assignment' end as assignment_quality_status
from profiles p left join {{ ref('mart_records') }} m
    on m.source_name='device_metadata' and m.participant_hk=p.participant_hk and m.device_id=p.device_id
left join {{ ref('mart_devices') }} d on d.device_hk=m.parent_hk
