select record_id, batch_id from {{ ref('dv_record_input') }}
where decision = 'accepted' and not structurally_loadable
