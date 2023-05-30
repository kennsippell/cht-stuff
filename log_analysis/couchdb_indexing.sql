update couchdb_log_nonrequest
set
  created = y.created,
  indexing = y.indexing
from (
  select 
    id,
    created,
    sum(
      case
        when raw like '%finished%' then -1
        when raw like '%Starting%' then 1
        else 0
      end
    ) over (order by created asc, id asc rows between unbounded preceding and current row) as indexing
  from (
    select
      id,
      to_timestamp((string_to_array(raw, ' '))[2], 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as created,
      raw
    from couchdb_log_nonrequest
    where raw like '%Index update finished%' or raw like '%Starting index update%'
  ) x
) y
where
  couchdb_log_nonrequest.id = y.id
;

update couchdb_log
set indexing = x.indexing
from (
  select 
    date_trunc('second', created) as created_second,
    min(indexing) as indexing
  from couchdb_log_nonrequest
  group by 1
) x 
where
  couchdb_log.created = x.created_second
;
