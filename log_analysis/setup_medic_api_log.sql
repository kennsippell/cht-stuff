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
  express_queue int
);

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
	  regexp_matches(raw, '\[([^\]]*)\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ms)?$') as parsed,
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