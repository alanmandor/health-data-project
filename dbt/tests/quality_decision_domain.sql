select batch_id, record_id from {{ ref('record_quality') }}
where decision is null or decision not in ('accepted', 'rejected', 'pending_reference', 'conflict', 'identical_duplicate')
