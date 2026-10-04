{{ config(materialized='table') }}
with decisions as (
    select c.*,
        case when c.duplicate_conflict or (c.operation = 'upsert' and c.firmware_conflict) then 'conflict'
             when c.invalid_reason_code is not null then 'rejected'
             when c.source_name <> 'participants' and p.participant_id is null then 'pending_reference'
             when c.source_name = 'device_metadata' and c.operation = 'upsert' and c.device_id <> p.device_id then 'pending_reference'
             when c.duplicate_rank > 1 then 'identical_duplicate'
             else 'accepted' end as decision,
        case when c.duplicate_conflict then 'conflicting_duplicate'
             when c.operation = 'upsert' and c.firmware_conflict then 'firmware_label_conflict'
             when c.invalid_reason_code is not null then c.invalid_reason_code
             when c.source_name <> 'participants' and p.participant_id is null then 'unknown_participant'
             when c.source_name = 'device_metadata' and c.operation = 'upsert' and c.device_id <> p.device_id then 'device_assignment_mismatch'
             when c.duplicate_rank > 1 then 'identical_duplicate' end as reason_code
    from {{ ref('quality_candidates') }} c
    left join {{ ref('quality_participants_current') }} p
        on c.record_source = p.record_source and c.participant_id = p.participant_id
)
select *, case when decision = 'accepted' and len(warning_codes) > 0 then 'accepted_with_warning'
               else decision end as quality_status
from decisions
