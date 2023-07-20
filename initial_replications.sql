WITH dates AS (
  SELECT generate_series(now() - '60 days'::interval, now(), '1 day'::interval)::date AS date
)

SELECT
  dates.date,
  COALESCE(
    COUNT(*) FILTER(WHERE device_id IS NOT NULL)
  , 0) AS count_initial_replications
FROM dates  
LEFT JOIN useview_telemetry_devices ON dates.date = period_start
GROUP BY 1
ORDER BY 1 ASC
;