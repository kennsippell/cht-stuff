select
  date_trunc('hour', created) 
    + make_interval(0,0,0,0,0, extract(minute FROM created)::int / 10 * 10) AS ten_min_bucket,
  COUNT(*) FILTER(where exchange = 'REQ') as request_count,
  COUNT(*) filter (where exchange = 'REQ' AND url_bucket = '/medic/_changes') as changes_request_count,
  COUNT(*) FILTER(where exchange = 'RES' and status < 500)
    / COUNT(*) filter(where exchange = 'RES')::double precision as estimated_yield_without_alb,

  percentile_cont(0.5) within group (order by express_queue asc) as express_queue_percentile_50th,
  percentile_cont(0.90) within group (order by express_queue asc) as express_queue_percentile_90th,
  percentile_cont(0.99) within group (order by express_queue asc) as express_queue_percentile_99th,

  percentile_cont(0.5) within group (order by response_time asc) as duration_percentile_50th,
  percentile_cont(0.90) within group (order by response_time asc) as duration_percentile_90th,
  percentile_cont(0.99) within group (order by response_time asc) as duration_percentile_99th,

  percentile_cont(0.5) within group (order by response_time asc) filter (where url_bucket = '/medic/_changes') as changes_duration_percentile_50th,
  percentile_cont(0.90) within group (order by response_time asc) filter (where url_bucket = '/medic/_changes') as changes_duration_percentile_90th,
  percentile_cont(0.99) within group (order by response_time asc) filter (where url_bucket = '/medic/_changes') as changes_duration_percentile_99th
from medic_api_log
group by 1
order by 1
;
