select
  date_trunc('hour', created) 
    + make_interval(0,0,0,0,0, extract(minute FROM created)::int / 10 * 10) AS ten_min_bucket,
  COUNT(*) as request_count,
  COUNT(*) filter (where url_bucket LIKE '%_changes%') as changes_request_count,
  COUNT(*) FILTER(where status < 500)
    / COUNT(*) filter(where status is not null)::double precision as estimated_yield,

  percentile_cont(0.5) within group (order by response_time asc) as duration_percentile_50th,
  percentile_cont(0.90) within group (order by response_time asc) as duration_percentile_90th,
  percentile_cont(0.99) within group (order by response_time asc) as duration_percentile_99th,

  percentile_cont(0.5) within group (order by response_time asc) filter (where url_bucket LIKE '%_changes%') as changes_duration_percentile_50th,
  percentile_cont(0.90) within group (order by response_time asc) filter (where url_bucket LIKE '%_changes%') as changes_duration_percentile_90th,
  percentile_cont(0.99) within group (order by response_time asc) filter (where url_bucket LIKE '%_changes%') as changes_duration_percentile_99th
from couchdb_log
where created is not null
group by 1
order by 1
;

select
  date_trunc('hour', created) 
    + make_interval(0,0,0,0,0, extract(minute FROM created)::int / 10 * 10) AS ten_min_bucket,
  percentile_cont(0.5) within group (order by queued asc) as couchdb_queue_percentile_50th,
  percentile_cont(0.90) within group (order by queued asc) as couchdb_queue_percentile_90th,
  percentile_cont(0.99) within group (order by queued asc) as couchdb_queue_percentile_99th
from couchdb_queue
where created is not null
group by 1
order by 1
;
