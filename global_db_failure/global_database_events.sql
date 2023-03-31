select
  doc #>> '{meta,user,name}' as username,
  doc #>> '{meta,time}',
  arguments
from couchdb_users_meta
CROSS JOIN LATERAL json_populate_recordset(null::record, (doc->>'log')::json) AS (level text, arguments text)
where doc ->> 'type' = 'feedback'
  and doc #>> '{meta,time}' > '2023-01'
  and arguments like '%Database has a global failure%'
--group by 1
--order by 2 desc;