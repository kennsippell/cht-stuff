with log_events as (
  select
    doc,
    arguments as log_event
  from couchdb_users_meta
  CROSS JOIN LATERAL json_populate_recordset(null::record, (doc->>'log')::json) AS (level text, arguments text)
  where doc #>> '{meta,time}' > '2023-01'
),

logs_with_global_failure as (
  select distinct doc
  from log_events
  where log_event = '["Database has a global failure",{}]'
)

select
   arguments,
   count(*)
from logs_with_global_failure
CROSS JOIN LATERAL json_populate_recordset(null::record, (doc->>'log')::json) AS (level text, arguments text)
where 
  arguments like '%IDBDatabase%'
  or
  arguments like '%indexed_db_went_bad%'
group by 1
order by 2 desc
;

