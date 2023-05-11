select  
  url_bucket,
  count(*) as request_count,
  percentile_disc(0.5) within group (order by response_time asc) as percentile_50th,
  percentile_disc(0.9) within group (order by response_time asc) as percentile_90th,
  percentile_disc(0.99) within group (order by response_time asc) as percentile_99th,
  sum(response_time) as sum_duration
from medic_api_log
group by 1
order by 6 desc
;

select  
  url_bucket,
  count(*) as request_count,
  percentile_disc(0.5) within group (order by response_time asc) as percentile_50th,
  percentile_disc(0.9) within group (order by response_time asc) as percentile_90th,
  percentile_disc(0.99) within group (order by response_time asc) as percentile_99th,
  sum(response_time) as sum_duration
from couchdb_log
group by 1
order by 6 desc
;