CREATE TABLE haproxy_log (
  id SERIAL PRIMARY KEY,
  raw text,
  created timestamp,
  status smallint,
  method varchar(6),
  url text,
  url_bucket text,
  request_source varchar(8),
  username varchar(256),
  bytes_read integer,
  response_time integer,
  res_header_0_what_is_this integer,
  user_agent text
);

-- import mapping to raw column. set invalid delimiter, set quote character to nothing, set escape character to nothing

update haproxy_log
set
  created = to_timestamp(
    -- no year in the logged date?
    concat(
      (regexp_matches(x.comma_separated[1]::text, '^([^:]*:[^:][^ ]*).*$'))[1],
      extract(year from now())
    ), 
    'Mon DD HH24:MI:SSYYYY'
  ),
  status = x.comma_separated[2]::smallint,
  method = x.comma_separated[3]::varchar(6),
  url = x.comma_separated[4]::text,
  url_bucket = case
    when x.comma_separated[4] ~ '/_users/org.couchdb.user.*' then '~/_users/org.couchdb.user.*'
    when x.comma_separated[4] ~ '/medic/org.couchdb.user.*' then '~/medic/org.couchdb.user.*'
    when x.comma_separated[4] ~ '/_utils.*' then '~/_utils/.*'
    when x.comma_separated[4] ~ '/medic-purged-role-.*' then '~/medic-purged-role-.*'
    when x.comma_separated[4] ~ '/medic-logs.*' then '~/medic-logs.*'
    when x.comma_separated[4] ~ '/medic-user-.*-meta.*' then '~/medic-user-.*-meta.*'
    when x.comma_separated[4] ~ '/medic/_local/.*' then '~/medic/_local/.*'
    when x.comma_separated[4] ~ '/medic-sentinel/.*-info' then '~/medic-sentinel/.*-info'
    when x.comma_separated[4] ~ '/medic/form.*' then '~/medic/form.*'
    when x.comma_separated[4] ~ '/medic/resources.*' then '~/medic/resources.*'
    else substring(x.comma_separated[4] FROM '^([^\?]*).*')
  end,
  request_source = x.comma_separated[5]::varchar(8),
  username = x.comma_separated[6]::varchar(256),
  bytes_read = x.comma_separated[8]::integer,
  response_time = x.comma_separated[9]::integer,
  res_header_0_what_is_this = case 
    when x.comma_separated[10] = '-' then null
    else x.comma_separated[10]::integer
  end,
  user_agent = case 
    when x.comma_separated[11] = '''-''' then null
    else x.comma_separated[11]::text
  end
from (
  select
    id,
    raw,
    string_to_array(raw, ',') as comma_separated
  from haproxy_log
  where
    raw like 'May %' and
    raw not like '%Proxy % stopped%'
) x
where haproxy_log.id = x.id
;
