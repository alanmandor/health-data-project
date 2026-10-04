select batch_id, record_id from {{ ref('record_quality') }}
group by batch_id, record_id having count(*) <> 1
