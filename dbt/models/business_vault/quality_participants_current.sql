with eligible as (
    select * from {{ ref('quality_candidates') }}
    where source_name = 'participants' and invalid_reason_code is null
        and not duplicate_conflict and duplicate_rank = 1
    qualify row_number() over (partition by record_source, participant_id order by delivery_sequence desc) = 1
)
select * from eligible where operation = 'upsert'
