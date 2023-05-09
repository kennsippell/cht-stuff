
with monthly_cht_effort as (
  select
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
    date_trunc('month', (doc #>> '{meta,time}')::timestamptz) as monthyear,
    COUNT(*) as global_failure_count
  from couchdb_users_meta
  CROSS JOIN LATERAL json_populate_recordset(null::record, (doc->>'log')::json) AS (level text, arguments text)
  WHERE
    arguments like '%Database has a global failure%'
  group by 1 
)

select
  monthly_cht_effort.monthyear,
  global_failure_count,
  cht_effort,
  global_failure_count / cht_effort as normalized 
from monthly_cht_effort
left join log_events
  on monthly_cht_effort.monthyear = log_events.monthyear 
where monthly_cht_effort.monthyear >= '2022-01-01 00:00:00.000 -0700'
order by 1
;

