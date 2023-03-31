with log_events as (
  select
    distinct doc as feedback_doc,
    doc #>> '{meta,user,name}' as username,
    date_trunc('day', (doc #>> '{meta,time}')::timestamptz) as daystamp
  from couchdb_users_meta
  CROSS JOIN LATERAL json_populate_recordset(null::record, (doc->>'log')::json) AS (level text, arguments text)
  where doc #>> '{meta,time}' > '2023-01'
  and arguments like '%Database has a global failure%' 
),

telemetry as (
  select
    doc #>> '{metadata,versions,app}' as core_version,
    doc #>> '{metadata,user}' as username,
    doc #>> '{metadata,deviceId}' as device_id,
    doc #>> '{dbInfo,doc_count}' as doc_count,
    doc #>> '{device,deviceInfo,app,version}' as cht_android,
    doc #>> '{device,deviceInfo,hardware,model}' as model,
    doc #>> '{device,hardwareConcurrency}' as hardwareConcurrency,
    doc #>> '{device,deviceInfo,storage,free}' as free_storage,
    
    make_date(
      (doc #>> '{metadata,year}')::int,
      (doc #>> '{metadata,month}')::int,
      (doc #>> '{metadata,day}')::int
    )::timestamptz as daystamp,
    doc as telemetry_doc
  from couchdb_users_meta
  where doc ->> 'type' = 'telemetry'
)

select
  *
from log_events
left join telemetry
on telemetry.username = log_events.username
  and telemetry.daystamp = log_events.daystamp
;