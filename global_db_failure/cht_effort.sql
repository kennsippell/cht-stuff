WITH telemetry_docs_with_metric_blob AS (
  SELECT
    doc->> '_id' AS id,
    doc #>> '{metadata,user}' as username,
    concat_ws(
      '-'::text, doc #>> '{metadata,year}',
      CASE
          WHEN
            doc #>> '{metadata,day}' IS NULL -- some telemetry documents have version 3.4, but have the modern daily metadata
            AND (
              doc #>> '{metadata,versions,app}' IS NULL or 
              string_to_array("substring"(doc #>> '{metadata,versions,app}', '(\d+.\d+.\d+)'), '.')::integer[] < '{3,8,0}'::integer[]
            )
          THEN ((doc #>> '{metadata,month}')::integer) + 1
          ELSE (doc #>> '{metadata,month}')::integer
      END,
      CASE
          WHEN (doc #>> '{metadata,day}') IS NOT NULL
          THEN doc #>> '{metadata,day}'
          ELSE '1'::text
      END
    )::date AS period_start,
    jsonb_object_keys(doc->'metrics') AS metric,
    doc->'metrics'->jsonb_object_keys(doc->'metrics') AS metric_values
  FROM couchdb_users_meta
  WHERE
    doc ->> 'type' = 'telemetry'
),

telemetry_metrics AS (
  SELECT 
    id,
    period_start,
    username,
    metric,
    min,
    max,
    sum,
    count,
    sumsqr
  FROM telemetry_docs_with_metric_blob
  CROSS JOIN LATERAL jsonb_to_record(metric_values) AS (min decimal, max decimal, sum decimal, count bigint, sumsqr decimal)
)

select
  date_trunc('month', period_start) as yearmonth,
  SUM(count)
from telemetry_metrics
where 
  metric in (
    'tasks:load',
    'tasks:refresh',
    'analytics:targets:load',
    'search:contacts:types',
    'search:reports'
  )
  and period_start > now() - '300 days'::interval
--group by 1, 2
;
