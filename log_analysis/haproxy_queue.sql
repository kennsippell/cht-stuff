DROP TABLE haproxy_queue;
CREATE TABLE haproxy_queue (
  id SERIAL PRIMARY KEY,
  haproxy_id integer,
  created timestamp,
  exchange varchar(3),
  queued integer,
  constraint fk_haproxy_id
    foreign key(haproxy_id) references haproxy(id)
);

insert into haproxy_queue (haproxy_id, created, exchange)
  select 
    id as haproxy_id,
    created - make_interval(0, 0, 0, 0, 0, 0, response_time / 1000::double precision) as created,
    'REQ' as exchange
  from haproxy
  where created is not null and response_time is not null

  union all 

  select 
    id,
    created,
    'RES'
  from haproxy
  where created is not null and response_time is not null
;

update haproxy_queue
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
    ) over (order by created asc, haproxy_id asc rows between unbounded preceding and current row) as queued
  from haproxy_queue
) x
where haproxy_queue.id = x.id
;
