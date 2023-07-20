SELECT id
FROM (
  select
    id,
     created,
    LAG(created) OVER (ORDER BY id asc) AS prev_created
  FROM medic_api_log
) x
WHERE created < prev_created
;