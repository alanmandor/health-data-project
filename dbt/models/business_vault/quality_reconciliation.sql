select batch_id, source_name, decision, count(*) as record_count
from {{ ref('record_quality') }}
group by batch_id, source_name, decision
