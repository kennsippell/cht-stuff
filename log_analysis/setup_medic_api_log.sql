CREATE TABLE medic_api_log (
  id SERIAL PRIMARY KEY,
  raw text,
  created timestamp,
  exchange varchar(3),
  request_id varchar(40),
  ip varchar(16),
  method varchar(6),
  url text,
  url_bucket text,
  status smallint,
  response_time real,
  total_time real,
  express_queue int
);

CREATE INDEX IF NOT EXISTS medic_api_log_idx_exchange ON medic_api_log USING btree(exchange);
CREATE INDEX IF NOT EXISTS medic_api_log_idx_request_id ON medic_api_log USING btree(request_id);
CREATE INDEX IF NOT EXISTS medic_api_log_idx_remote_ip ON medic_api_log USING btree(ip);
CREATE INDEX IF NOT EXISTS medic_api_log_idx_url_bucket ON medic_api_log USING btree(url_bucket);
CREATE INDEX IF NOT EXISTS medic_api_log_idx_status ON medic_api_log USING btree(status);

-- import mapping to raw column

delete from medic_api_log where raw not like '% REQ %' and raw not like '% RES %';

update medic_api_log
set
  created = x.parsed[1]::timestamp,
  exchange = x.parsed[2],
  request_id = x.parsed[3],
  ip = x.parsed[4],
  method = x.parsed[6],
  url = x.parsed[7],
  status = case
    when x.parsed[9] = '-' then null
    else x.parsed[9]::smallint
  end,
  response_time = case
    when x.parsed[11] = '-' then null
    else x.parsed[11]::real
  end,
  url_bucket = case
    when url ~ 'medic-user-.*-meta/_local.*' then 'medic-user-.*-meta/_local.*'
    when url ~ 'medic-user-.*-meta/_revs_diff.*' then 'medic-user-.*-meta/_revs_diff.*'
    when url ~ 'medic-user-.*-meta/_changes.*' then 'medic-user-.*-meta/_changes.*'
    when url ~ 'medic-user-.*-meta' then 'medic-user-.*-meta'
    when url ~ '/medic/_changes\?.*longpoll.*' then '/medic/_changes (longpoll)'
    when url ~ '/medic/_changes\?.*timeout=600000.*style.*heartbeat.*since.*limit=100' then '/medic/_changes (offline)'
    when url ~ '/medic/_changes.*' then '/medic/_changes (other)'
    when url ~ '/medic-purged-role-.*' then '/medic-purged-role-.*'
    when url ~ '/medic/[a-zA-Z0-9\-]{36}' then '/medic/[a-zA-Z0-9\-]{36}'
    when url ~ '/medic/_local/.*' then '/medic/_local/.*'
    when url ~ 'medic/.*/content' then 'medic/.*/content'
    when url ~ 'medic/form%3.*' then 'medic/form%3.*'
    when url ~ 'medic/resources/.*' then 'medic/resources/.*'
    else substring(url FROM '^([^\?]*).*')
  end,
  express_queue = x.express_queue
from (
  select
    id,
    regexp_matches(raw, '\[([^\]]*)\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*)( [^ ]*)?(?: ([^ ]*) ([^ ]*) ([^ ]*) ms)?$') as parsed,
    sum(
      case
        when exchange = 'REQ' then 1
        else -1
      end
    ) over (order by id asc rows between unbounded preceding and current row) as express_queue
  from medic_api_log
) x
where medic_api_log.id = x.id
;

-- response_time is time for response headers to be written. for many requests, this is an okay approximation.
-- but for requests liked _changes, it is a totally wrong. we really want morgan's total-time metric
-- 
-- to approximate total-time, use the time between the REQ and RES log events. unfortunately, the precision of
-- these events is in seconds.
update medic_api_log
set
  total_time = case
    -- use log events if there is no response ('-')
    when x.response_time is null then x.delta_in_ms
    -- use log events if it off by more than the 1sec precision
    when x.delta_in_ms > x.response_time + 1000 then x.delta_in_ms
    else x.response_time
  end
from (
  select
    max(id) as response_id,
    max(response_time) as response_time,
    extract (epoch from (max(created) - min(created))) * 1000 as delta_in_ms
  from medic_api_log
  group by request_id
) x
where medic_api_log.id = x.response_id
;