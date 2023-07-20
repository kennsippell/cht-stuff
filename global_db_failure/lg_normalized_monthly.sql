
with monthly_cht_effort as (
  select
  --  username,
    date_trunc('month', period_start) as monthyear,
    SUM(count) as cht_effort
  FROM useview_telemetry
  WHERE
    metric in (
      'tasks:load',
      'tasks:refresh',
      'analytics:targets:load',
      'search:contacts:types',
      'search:reports'
    )
  group by 1
  order by 1
),

log_events as (
  select
--    doc #>> '{meta,user,name}' as username,
    date_trunc('month', (doc #>> '{meta,time}')::timestamptz) as monthyear,
    COUNT(*) as global_failure_count
  from couchdb_users_meta
  CROSS JOIN LATERAL json_populate_recordset(null::record, (doc->>'log')::json) AS (level text, arguments text)
  WHERE
--  where doc #>> '{meta,time}' > '2023-01'
    arguments like '%Database has a global failure%'
  group by 1 
)

select
  monthly_cht_effort.monthyear,
  global_failure_count,
  cht_effort,
  global_failure_count / cht_effort as normalized 
from log_events
left join monthly_cht_effort
on monthly_cht_effort.monthyear = log_events.monthyear 
----left join telemetry
----on telemetry.username = log_events.username
----  and telemetry.daystamp = log_events.daystamp
--group by 1
order by 1
;

