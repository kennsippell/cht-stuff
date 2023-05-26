
CREATE TABLE couchdb_log (
  id SERIAL PRIMARY KEY,
  raw text,
  created timestamp,
  node varchar(18),
  request_id varchar(10),
  hostname varchar(100),
  remote_addr varchar(18),
  username varchar(32),
  "method" varchar(6),
  url text,
  url_bucket text,
  status integer,
  response_time integer
);

CREATE TABLE couchdb_log_nonrequest (
  id integer,
  raw text
);

CREATE INDEX IF NOT EXISTS couchdb_log_idx_created ON couchdb_log USING btree(created);
CREATE INDEX IF NOT EXISTS couchdb_log_idx_request_id ON couchdb_log USING btree(request_id);
CREATE INDEX IF NOT EXISTS couchdb_log_idx_username ON couchdb_log USING btree(username);
CREATE INDEX IF NOT EXISTS couchdb_log_idx_url_bucket ON couchdb_log USING btree(url_bucket);
CREATE INDEX IF NOT EXISTS couchdb_log_idx_status ON couchdb_log USING btree(status);

-- import data mapped to raw

insert into couchdb_log_nonrequest(id, raw)
select id, raw from couchdb_log
where
  (string_to_array(raw, ' '))[9] not in ('GET', 'POST', 'PUT', 'DELETE', 'HEAD')
  or (string_to_array(raw, ' '))[10] is null
;

delete from couchdb_log 
where
  (string_to_array(raw, ' '))[9] not in ('GET', 'POST', 'PUT', 'DELETE', 'HEAD')
  or (string_to_array(raw, ' '))[10] is null
;

update couchdb_log
set
  created = to_timestamp(x.comma_separated[2], 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  node = x.comma_separated[3],
  request_id = x.comma_separated[5],
  hostname = x.comma_separated[6],
  remote_addr = x.comma_separated[7],
  username = x.comma_separated[8],
  method = x.comma_separated[9],
  url = x.comma_separated[10],
  status = x.comma_separated[11]::integer,
  response_time = x.comma_separated[13]::integer,
  url_bucket = case
    when url ~ '\/medic\/[a-zA-Z0-9\-]{36}\/content' then '\/medic\/[a-zA-Z0-9\-]{36}\/content'
    when url ~ '\/medic\/[a-zA-Z0-9\-]{36}' then '/medic/[a-zA-Z0-9\-]{36}'
    when url ~ '/medic/_local/.*' then '/medic/_local/.*'
    when url ~ '/medic/_changes\?.*filter=.*' then '/medic/_changes w/ filter'
	  when url ~ '/medic/_changes.*' then '/medic/_changes w/o filter'
    when url ~ '/_users/.*' then '/_users/.*'
    when url ~ 'medic-user-.*-meta.*' then 'medic-user-.*-meta.*'
    when url ~ '/medic-users-meta/_local/.*' then '/medic-users-meta/_local/.*'
    when url ~ '/medic/org.couchdb.user.*' then '/medic/org.couchdb.user.*'
    when url ~ '/medic-logs/.*' then '/medic-logs/.*'
    when url ~ '/medic-sentinel/.*' then '/medic-sentinel/.*'
    when url ~ '/medic-purged-role-.*/_changes' then '/medic-purged-role-.*/_changes'
    when url ~ '/medic-purged-role-.*' then '/medic-purged-role-.*'
    when url ~ 'medic/form%3.*' then 'medic/form%3.*'
    when url ~ 'medic/resources/.*' then 'medic/resources/.*'
    else substring(url FROM '^([^\?]*).*')
  end
from (
  select
    id,
    raw,
    string_to_array(raw, ' ') as comma_separated
  from couchdb_log
) x
where
  couchdb_log.id = x.id
;