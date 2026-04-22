-- Insert unknown members in table
WITH features as (
  select osmid, version from :features_table
  union
  select osmid, version from :features_table_update
), missing as (
  -- Fiding osmid of known live features that are not known as features
  select distinct l.osm_id as osmid
  from :live_table l
  left join features u ON u.osmid= l.osm_id
  where u.version is null
)

select 
  case when count(distinct case when osmid like 'node/%' then osmid end) > 0 then concat('node(id:', array_to_string(array_agg(distinct case when osmid like 'node%' then replace(osmid, 'node/','') end), ','),');') end as nodes,
  case when count(distinct case when osmid like 'way/%' then osmid end) > 0 then concat('way(id:', array_to_string(array_agg(distinct case when osmid like 'way%' then replace(osmid, 'way/','') end), ','),');') end as ways,
  case when count(distinct case when osmid like 'relation/%' then osmid end) > 0 then concat('relation(id:', array_to_string(array_agg(distinct case when osmid like 'relation%' then replace(osmid, 'relation/','') end), ','),');') end as relation
from missing;
