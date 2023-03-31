with feedback_with_db_fail as (
  select distinct doc as doc
  from couchdb_users_meta
  CROSS JOIN LATERAL json_populate_recordset(null::record, (doc->>'log')::json) AS (level text, arguments text)
  where doc #>> '{meta,time}' > '2022-10'
  and arguments like '%Database has a global failure%' 
),

events_in_db_fail_docs as (
  select *
  from feedback_with_db_fail
  CROSS JOIN LATERAL json_populate_recordset(null::record, (doc->>'log')::json) AS (level text, arguments text)
),

write_db_fail_docs as (
  select doc ->> '_id' as doc_id
  from events_in_db_fail_docs
  where arguments like '%indexed_db_went_bad%TimeoutError%'
)

select
  COUNT(distinct doc) as count_db_fail,
  COUNT(distinct doc) filter (where doc ->> '_id' in (select doc_id from write_db_fail_docs)) as count_write_fail,
  COUNT(distinct doc) filter (where 
    arguments like '%IDBDatabase%' 
    and doc ->> '_id' not in (select doc_id from write_db_fail_docs)
  ) as count_read_fail,
  COUNT(distinct doc)
--  COUNT(distinct doc) filter (where arguments like '%indexed_db_went_bad%TimeoutError%') as count_read_fail
from events_in_db_fail_docs
;

