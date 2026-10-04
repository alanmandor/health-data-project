with raw_counts as (
    select b.batch_id, r.source_name, count(*) as n
    from {{ source('raw', 'source_records') }} r
    join {{ source('raw', 'batch_records') }} b using (record_id)
    group by b.batch_id, r.source_name
), quality_counts as (
    select batch_id, source_name, sum(record_count) as n
    from {{ ref('quality_reconciliation') }} group by batch_id, source_name
)
select coalesce(r.batch_id, q.batch_id) as batch_id
from raw_counts r full join quality_counts q using (batch_id, source_name)
where r.n is distinct from q.n
