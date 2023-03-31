
CREATE MATERIALIZED VIEW public.useview_telemetry_metrics
TABLESPACE pg_default
AS WITH telemetry_docs_with_metric_blob AS (
         SELECT couchdb_users_meta.doc ->> '_id'::text AS id,
            couchdb_users_meta.doc #>> '{metadata,user}'::text[] AS username,
            concat_ws('-'::text, couchdb_users_meta.doc #>> '{metadata,year}'::text[],
                CASE
                    WHEN (couchdb_users_meta.doc #>> '{metadata,day}'::text[]) IS NULL AND ((couchdb_users_meta.doc #>> '{metadata,versions,app}'::text[]) IS NULL OR string_to_array("substring"(couchdb_users_meta.doc #>> '{metadata,versions,app}'::text[], '(\d+.\d+.\d+)'::text), '.'::text)::integer[] < '{3,8,0}'::integer[]) THEN ((couchdb_users_meta.doc #>> '{metadata,month}'::text[])::integer) + 1
                    ELSE (couchdb_users_meta.doc #>> '{metadata,month}'::text[])::integer
                END,
                CASE
                    WHEN (couchdb_users_meta.doc #>> '{metadata,day}'::text[]) IS NOT NULL THEN couchdb_users_meta.doc #>> '{metadata,day}'::text[]
                    ELSE '1'::text
                END)::date AS period_start,
            jsonb_object_keys(couchdb_users_meta.doc -> 'metrics'::text) AS metric,
            (couchdb_users_meta.doc -> 'metrics'::text) -> jsonb_object_keys(couchdb_users_meta.doc -> 'metrics'::text) AS metric_values
           FROM couchdb_users_meta
          WHERE (couchdb_users_meta.doc ->> 'type'::text) = 'telemetry'::text
        )
 SELECT telemetry_docs_with_metric_blob.id,
    telemetry_docs_with_metric_blob.period_start,
    telemetry_docs_with_metric_blob.username,
    telemetry_docs_with_metric_blob.metric,
    jsonb_to_record.min,
    jsonb_to_record.max,
    jsonb_to_record.sum,
    jsonb_to_record.count,
    jsonb_to_record.sumsqr
  FROM telemetry_docs_with_metric_blob
  CROSS JOIN LATERAL jsonb_to_record(telemetry_docs_with_metric_blob.metric_values) jsonb_to_record(min numeric, max numeric, sum numeric, count bigint, sumsqr numeric)
WITH DATA;
