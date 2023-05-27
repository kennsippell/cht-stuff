with api_stats as (
  select
    date_trunc('hour', created) 
      + make_interval(0,0,0,0,0, extract(minute FROM created)::int / 10 * 10) AS ten_min_bucket,
    COUNT(*) FILTER(where exchange = 'REQ') as request_count,
    COUNT(*) filter (where exchange = 'REQ' AND url_bucket = '/medic/_changes w/o longpoll') as changes_request_count,
    COUNT(*) FILTER(where exchange = 'RES' and status < 500)
      / COUNT(*) filter(where exchange = 'RES')::double precision as estimated_yield_without_alb,

    percentile_disc(0.5) within group (order by express_queue asc) as express_queue_percentile_50th,
    percentile_disc(0.90) within group (order by express_queue asc) as express_queue_percentile_90th,
    percentile_disc(0.99) within group (order by express_queue asc) as express_queue_percentile_99th,

    percentile_disc(0.5) within group (order by response_time asc) as duration_percentile_50th,
    percentile_disc(0.90) within group (order by response_time asc) as duration_percentile_90th,
    percentile_disc(0.99) within group (order by response_time asc) as duration_percentile_99th,

    percentile_disc(0.5) within group (order by response_time asc) filter (where url_bucket = '/medic/_changes w/o longpoll') as changes_duration_percentile_50th,
    percentile_disc(0.90) within group (order by response_time asc) filter (where url_bucket = '/medic/_changes w/o longpoll') as changes_duration_percentile_90th,
    percentile_disc(0.99) within group (order by response_time asc) filter (where url_bucket = '/medic/_changes w/o longpoll') as changes_duration_percentile_99th
  from medic_api_log
  group by 1
),

couch_stats as (
  select
    date_trunc('hour', created) 
      + make_interval(0,0,0,0,0, extract(minute FROM created)::int / 10 * 10) AS ten_min_bucket,
    COUNT(*) as request_count,
    COUNT(*) filter (where url_bucket = '/medic/_changes w/ filter') as changes_request_count,
    COUNT(*) FILTER(where status < 500)
      / COUNT(*) filter(where status is not null)::double precision as estimated_yield,

    percentile_disc(0.5) within group (order by response_time asc) as duration_percentile_50th,
    percentile_disc(0.90) within group (order by response_time asc) as duration_percentile_90th,
    percentile_disc(0.99) within group (order by response_time asc) as duration_percentile_99th,

    percentile_disc(0.5) within group (order by response_time asc) filter (where url_bucket = '/medic/_changes w/ filter') as changes_duration_percentile_50th,
    percentile_disc(0.90) within group (order by response_time asc) filter (where url_bucket = '/medic/_changes w/ filter') as changes_duration_percentile_90th,
    percentile_disc(0.99) within group (order by response_time asc) filter (where url_bucket = '/medic/_changes w/ filter') as changes_duration_percentile_99th
  from couchdb_log
  where created is not null
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