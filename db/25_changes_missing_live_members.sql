-- Insert unknown members in table
WITH features as (
  select osmid, version from :features_table
  union
  select osmid, version from :features_table_update
), unknown as (
  -- Finding osmid of referenced members that are not known in features
  select m.memberid as osmid
  from :members_table m 
  left join features f ON f.osmid=m.memberid
  where f.version is null

  union

  -- Fiding osmid of known live features that are not known as features
  select l.osm_id as osmid
  from :live_table l
  left join features u ON u.osmid= l.osm_id
  where u.version is null
), missing as (
  select distinct u.osmid as osmid
  from unknown u
)

select 
  case when count(distinct case when osmid like 'node/%' then osmid end) > 0 then concat('node(id:', array_to_string(array_agg(distinct case when osmid like 'node%' then replace(osmid, 'node/','') end), ','),');') end as nodes,
  case when count(distinct case when osmid like 'way/%' then osmid end) > 0 then concat('way(id:', array_to_string(array_agg(distinct case when osmid like 'way%' then replace(osmid, 'way/','') end), ','),');') end as ways,
  case when count(distinct case when osmid like 'relation/%' then osmid end) > 0 then concat('relation(id:', array_to_string(array_agg(distinct case when osmid like 'relation%' then replace(osmid, 'relation/','') end), ','),');') end as relation
from missing;
