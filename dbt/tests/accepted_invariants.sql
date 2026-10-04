select batch_id, record_id from {{ ref('record_quality') }}
where decision = 'accepted' and (
    invalid_reason_code is not null or duplicate_conflict or duplicate_rank <> 1
    or business_key_hash is null or operation not in ('upsert', 'delete')
    or (source_name in ('steps', 'heart_rate') and epoch_us(event_at_utc) % 60000000 <> 0)
    or (source_name <> 'participants' and not exists (
        select 1 from {{ ref('quality_participants_current') }} p
        where p.record_source = record_quality.record_source
          and p.participant_id = record_quality.participant_id
    ))
)
