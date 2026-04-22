-- Insert unknown members in table
WITH features as (
  select osmid, version from :features_table
  union
  select osmid, version from :features_table_update
), missing as (
  -- Finding osmid of referenced members that are not known in features
  select distinct m.memberid as osmid
  from :members_table m 
  left join features f ON f.osmid=m.memberid
  where f.version is null
)

select 
  case when count(distinct case when osmid like 'node/%' then osmid end) > 0 then concat('node(id:', array_to_string(array_agg(distinct case when osmid like 'node%' then replace(osmid, 'node/','') end), ','),');') end as nodes,
  case when count(distinct case when osmid like 'way/%' then osmid end) > 0 then concat('way(id:', array_to_string(array_agg(distinct case when osmid like 'way%' then replace(osmid, 'way/','') end), ','),');') end as ways,
  case when count(distinct case when osmid like 'relation/%' then osmid end) > 0 then concat('relation(id:', array_to_string(array_agg(distinct case when osmid like 'relation%' then replace(osmid, 'relation/','') end), ','),');') end as relation
from missing;
