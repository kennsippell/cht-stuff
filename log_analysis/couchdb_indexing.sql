select 
  id,
  created,
  sum(
	  case
	  	when raw like '%finished%' then -1
	  	when raw like '%Starting%' then 1
	  	else 0
	  end
  ) over (order by created asc, id asc rows between unbounded preceding and current row) as queued
  
from (
  select
    id,
    to_timestamp((string_to_array(raw, ' '))[2], 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as created,
    raw
  from couchdb_log_nonrequest
  where
    raw like '%2023-04-2%ndex%'
) x
where
  created > '2023-04-26' and created < '2023-04-27'
order by 1
;