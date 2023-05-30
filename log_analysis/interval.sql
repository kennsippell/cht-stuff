with api_stats as (
  select
    date_trunc('hour', created) 
      + make_interval(0,0,0,0,0, extract(minute FROM created)::int / 10 * 10) AS ten_min_bucket,
    COUNT(*) FILTER (WHERE exchange = 'REQ') as request_count,
    COUNT(*) FILTER (WHERE exchange = 'REQ' AND url like '/medic/_changes%timeout=600000%style%heartbeat%since%limit=100') as changes_request_count,
    COUNT(*) FILTER (WHERE 
      exchange = 'RES'
      and (
        status < 500
        
        -- alb timeout at 60 mins yields a 504
        or (status is null and (total_time < 3595000 or total_time > 3605000))
      )

      -- _changes?timeout=x can timeout with status 200 -- but this does not yield
      and (url not like '%_changes%timeout=600000%' OR total_time < 600000)
    )
      / COUNT(*) FILTER (WHERE exchange = 'RES')::double precision as estimated_yield_without_alb,

    percentile_disc(0.5) within group (order by express_queue asc) as express_queue_percentile_50th,
    percentile_disc(0.90) within group (order by express_queue asc) as express_queue_percentile_90th,
    percentile_disc(0.99) within group (order by express_queue asc) as express_queue_percentile_99th,

    percentile_disc(0.5) within group (order by total_time asc) as duration_percentile_50th,
    percentile_disc(0.90) within group (order by total_time asc) as duration_percentile_90th,
    percentile_disc(0.99) within group (order by total_time asc) as duration_percentile_99th,

    percentile_disc(0.5) within group (order by total_time asc) filter (WHERE url_bucket = '/medic/_changes (offline)') as changes_duration_percentile_50th,
    percentile_disc(0.90) within group (order by total_time asc) filter (WHERE url_bucket = '/medic/_changes (offline)') as changes_duration_percentile_90th,
    percentile_disc(0.99) within group (order by total_time asc) filter (WHERE url_bucket = '/medic/_changes (offline)') as changes_duration_percentile_99th
  from medic_api_log
  group by 1
),

couch_stats as (
  select
    date_trunc('hour', created) 
      + make_interval(0,0,0,0,0, extract(minute FROM created)::int / 10 * 10) AS ten_min_bucket,
    COUNT(*) as request_count,
    COUNT(*) filter (WHERE url_bucket = '/medic/_changes (filter)') as changes_request_count,
    COUNT(*) FILTER(WHERE status < 500)
      / COUNT(*) filter(WHERE status is not null)::double precision as estimated_yield,

    percentile_disc(0.5) within group (order by response_time asc) as duration_percentile_50th,
    percentile_disc(0.90) within group (order by response_time asc) as duration_percentile_90th,
    percentile_disc(0.99) within group (order by response_time asc) as duration_percentile_99th,

    percentile_disc(0.5) within group (order by response_time asc) filter (WHERE url_bucket = '/medic/_changes (filter)') as changes_duration_percentile_50th,
    percentile_disc(0.90) within group (order by response_time asc) filter (WHERE url_bucket = '/medic/_changes (filter)') as changes_duration_percentile_90th,
    percentile_disc(0.99) within group (order by response_time asc) filter (WHERE url_bucket = '/medic/_changes (filter)') as changes_duration_percentile_99th
  from couchdb_log
  WHERE created is not null
  group by 1
),

couch_queue_stats as (
  select
    date_trunc('hour', created) 
      + make_interval(0,0,0,0,0, extract(minute FROM created)::int / 10 * 10) AS ten_min_bucket,
    percentile_disc(0.5) within group (order by queued asc) as couchdb_queue_percentile_50th,
    percentile_disc(0.90) within group (order by queued asc) as couchdb_queue_percentile_90th,
    percentile_disc(0.99) within group (order by queued asc) as couchdb_queue_percentile_99th
  from couchdb_queue
  where created is not null
  group by 1
)

select 
  coalesce(coalesce(api_stats.ten_min_bucket, couch_stats.ten_min_bucket), couch_queue_stats.ten_min_bucket) as ten_min_buckets, 
  *
from api_stats
full join couch_stats on api_stats.ten_min_bucket = couch_stats.ten_min_bucket
full join couch_queue_stats on api_stats.ten_min_bucket = couch_queue_stats.ten_min_bucket
;