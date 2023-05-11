DROP TABLE couchdb_queue;
CREATE TABLE couchdb_queue (
  id SERIAL PRIMARY KEY,
  couchdb_id integer,
  created timestamp,
  exchange varchar(3),
  queued integer,
  constraint fk_couchdb_id
    foreign key(couchdb_id) references couchdb_log(id)
);

insert into couchdb_queue (couchdb_id, created, exchange)
  select 
    id as couchdb_id,
    created - make_interval(0, 0, 0, 0, 0, 0, response_time / 1000::double precision) as created,
    'REQ' as exchange
  from couchdb_log
  where created is not null and response_time is not null

  union all 

  select 
    id,
    created,
    'RES'
  from couchdb_log
  where created is not null and response_time is not null
;

update couchdb_queue
set
  queued = x.queued
from (
  select
    id,
    sum(
      case
        when exchange = 'REQ' then 1
        else -1
      end
    ) over (order by created asc, couchdb_id asc rows between unbounded preceding and current row) as queued
  from couchdb_queue
) x
where couchdb_queue.id = x.id
;
