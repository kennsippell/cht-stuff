with api_stats AS (
  select
    date_trunc('hour', created) 
      + make_interval(0,0,0,0,0, extract(minute FROM created)::int / 10 * 10) AS ten_min_bucket,
    COUNT(*) FILTER (WHERE exchange = 'REQ') AS request_count,
    COUNT(*) FILTER (WHERE exchange = 'REQ' AND url like '/medic/_changes%timeout=600000%style%heartbeat%since%limit=100') AS changes_request_count,
    COUNT(*) FILTER (WHERE 
      exchange = 'RES'
      and (
        status < 500
        
        -- alb timeout at 60 mins (yields a 504 to user, but has null status in API logs)
        or (status is null and total_time < 3595000)
      )

      -- _changes?timeout=x can timeout with status 200 -- but this does not yield
      and (url not like '%_changes%timeout=600000%' OR total_time < 600000)
    )
      / COUNT(*) FILTER (WHERE exchange = 'RES')::double precision AS estimated_yield,

    COUNT(*) FILTER (WHERE
      exchange = 'RES' 
      AND url_bucket = '/medic/_changes (offline)'
      AND total_time < 600000
    )
      / COALESCE(NULLIF(COUNT(*) FILTER (WHERE exchange = 'RES' AND url_bucket = '/medic/_changes (offline)')::double precision, 0), 1) AS replication_yield,

    percentile_disc(0.5) within group (ORDER BY express_queue ASC) AS express_queue_percentile_50th,
    percentile_disc(0.90) within group (ORDER BY express_queue ASC) AS express_queue_percentile_90th,
    percentile_disc(0.99) within group (ORDER BY express_queue ASC) AS express_queue_percentile_99th,

    percentile_disc(0.5) within group (ORDER BY total_time ASC) AS total_time_percentile_50th,
    percentile_disc(0.90) within group (ORDER BY total_time ASC) AS total_time_percentile_90th,
    percentile_disc(0.99) within group (ORDER BY total_time ASC) AS total_time_percentile_99th,

    percentile_disc(0.5) within group (ORDER BY total_time ASC) FILTER (WHERE url_bucket = '/medic/_changes (offline)') AS replicating_total_time_percentile_50th,
    percentile_disc(0.90) within group (ORDER BY total_time ASC) FILTER (WHERE url_bucket = '/medic/_changes (offline)') AS replicating_total_time_percentile_90th,
    percentile_disc(0.99) within group (ORDER BY total_time ASC) FILTER (WHERE url_bucket = '/medic/_changes (offline)') AS replicating_total_time_percentile_99th
  from medic_api_log
  group by 1
),

couch_stats AS (
  select
    date_trunc('hour', created) 
      + make_interval(0,0,0,0,0, extract(minute FROM created)::int / 10 * 10) AS ten_min_bucket,
    COUNT(*) AS request_count,
    COUNT(*) FILTER (WHERE url_bucket = '/medic/_changes (FILTER)') AS changes_request_count,
    COUNT(*) FILTER(WHERE status < 500)
      / COUNT(*) FILTER(WHERE status is not null)::double precision AS estimated_yield,

    percentile_disc(0.5) within group (ORDER BY response_time ASC) AS duration_percentile_50th,
    percentile_disc(0.90) within group (ORDER BY response_time ASC) AS duration_percentile_90th,
    percentile_disc(0.99) within group (ORDER BY response_time ASC) AS duration_percentile_99th,

    percentile_disc(0.5) within group (ORDER BY response_time ASC) FILTER (WHERE url_bucket = '/medic/_changes (FILTER)') AS changes_duration_percentile_50th,
    percentile_disc(0.90) within group (ORDER BY response_time ASC) FILTER (WHERE url_bucket = '/medic/_changes (FILTER)') AS changes_duration_percentile_90th,
    percentile_disc(0.99) within group (ORDER BY response_time ASC) FILTER (WHERE url_bucket = '/medic/_changes (FILTER)') AS changes_duration_percentile_99th
  from couchdb_log
  WHERE created is not null
  group by 1
),

couch_queue_stats AS (
  select
    date_trunc('hour', created) 
      + make_interval(0,0,0,0,0, extract(minute FROM created)::int / 10 * 10) AS ten_min_bucket,
    percentile_disc(0.5) within group (ORDER BY queued ASC) AS couchdb_queue_percentile_50th,
    percentile_disc(0.90) within group (ORDER BY queued ASC) AS couchdb_queue_percentile_90th,
    percentile_disc(0.99) within group (ORDER BY queued ASC) AS couchdb_queue_percentile_99th
  from couchdb_queue
  where created is not null
  group by 1
)

select 
  coalesce(coalesce(api_stats.ten_min_bucket, couch_stats.ten_min_bucket), couch_queue_stats.ten_min_bucket) AS ten_min_buckets, 
  *
from api_stats
full join couch_stats on api_stats.ten_min_bucket = couch_stats.ten_min_bucket
full join couch_queue_stats on api_stats.ten_min_bucket = couch_queue_stats.ten_min_bucket
;