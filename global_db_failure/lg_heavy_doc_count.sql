with 
  monthly_cht_effort as (
  select
    username,
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
  and period_start > '2022-10-01'::timestamptz 
  group by 1
),

log_events as (
  select
    doc #>> '{meta,user,name}' as username,
--    date_trunc('month', (doc #>> '{meta,time}')::timestamptz) as monthyear,
    COUNT(*) as global_failure_count
  from couchdb_users_meta
  CROSS JOIN LATERAL json_populate_recordset(null::record, (doc->>'log')::json) AS (level text, arguments text)
  WHERE
    doc #>> '{meta,time}' >= '2022-10'
    and arguments like '%Database has a global failure%'
  group by 1 
), 

latest_user_telemetry as (
  select
    distinct on (doc #>> '{metadata,user}')
    
    make_date(
      (doc #>> '{metadata,year}')::int,
      (doc #>> '{metadata,month}')::int,
      (doc #>> '{metadata,day}')::int
    )::timestamptz as daystamp,
    
    doc #>> '{metadata,versions,app}' as core_version,
    doc #>> '{metadata,user}' as username,
    
    doc #>> '{metadata,deviceId}' as device_id,
    doc #>> '{dbInfo,doc_count}' as doc_count,
    doc #>> '{device,deviceInfo,app,version}' as cht_android,
    doc #>> '{device,userAgent}' as user_agent,
    substring(doc #>> '{device,userAgent}' from 'Chrome\/\d{2,3}') as chrome_version,
    doc #>> '{device,deviceInfo,hardware,model}' as model,
    doc #>> '{device,hardwareConcurrency}' as hardwareConcurrency,
    doc #>> '{device,deviceInfo,storage,free}' as free_storage,

    doc as telemetry_doc
  from couchdb_users_meta
  where 
    doc ->> 'type' = 'telemetry'
    and (doc #>> '{metadata,year}')::int >= 2022
    and (doc #>> '{metadata,month}')::int >= 10
    and doc #>> '{metadata,day}' is not null
  order by 3 desc
),

user_details as (
  select
    monthly_cht_effort.username,
    coalesce(global_failure_count, 0) as global_failure_count,
    cht_effort,
    coalesce(latest_user_telemetry.doc_count::int, 0) / 1000 doc_count_thousands,
    cht_android,
      model,
      chrome_version,
      hardwareConcurrency
  from monthly_cht_effort
  left join log_events
    on monthly_cht_effort.username = log_events.username
  left join latest_user_telemetry
    on monthly_cht_effort.username = latest_user_telemetry.username
  order by 1
)

select 
  chrome_version,
  COUNT(*) as user_count,
  sum(global_failure_count) as global_failure_count,
  SUM(cht_effort) as cht_effort,
  sum(global_failure_count) / SUM(cht_effort) as normalized 
from user_details
group by 1
order by 1
;