-- first release limitation: filters can only be applied to 1st level attributes (no properties of properties, ...)

/*
The temporal evolution of an NGSI-LD Property shall be represented as an Array of JSON-LD objects, 
each one representing an instance of the Property (as mandated by clause 4.5.2) at a particular point in time. 

If a Property is static (i.e. it has not changed over time) then it shall be represented by an Array with a single instance. 
*/

\pset pager 0
-- \set ECHO queries

\echo id only
\echo /temporal/entities/urn:ngsi-ld:Vehicle:B9211
with r as (
  select te.id, te.type, te.createdat, te.modifiedat, coalesce(teai.attributeid, '') as attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
  from temporalentity te
  left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
  where -- basic query
        te.id = 'urn:ngsi-ld:Vehicle:B130' 
  group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid
  order by te.id, teai.attributeid
)
select id, tedata || case when attrdata <> '{"": [null]}'::jsonb then attrdata else tedata end as data from (
  select  id,
          ('{"@id":"' || id || '"}')::jsonb || 
          ('{"@type":["' || type || '"]}')::jsonb || 
          ('{"https://uri.etsi.org/ngsi-ld/createdAt":[ { "@type": "https://uri.etsi.org/ngsi-ld/DateTime", "@value": "' || createdat || '" }]}')::jsonb || 
          ('{"https://uri.etsi.org/ngsi-ld/modifiedAt":[ { "@type": "https://uri.etsi.org/ngsi-ld/DateTime", "@value": "' || modifiedat || '" }]}')::jsonb as tedata, 
          jsonb_object_agg(attributeid, attributedata) as attrdata
  from r
  group by id, type, createdat, modifiedat
  order by modifiedat desc
) as m;

\echo type + temporal 
\echo ?type=Vehicle&timerel=after&time=2018-08-01T00:00:00Z&timeproperty=observedAt
with r as (
  select te.id, te.type, te.createdat, te.modifiedat, coalesce(teai.attributeid, '') as attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
  from temporalentity te
  left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
  where -- basic query
        te.type = 'http://example.org/vehicle/Vehicle' and 
        -- temporal query
        ( (teai.static = true and teai.observedat is null) or  -- temporal filters do not apply to static attributes
          teai.observedat >= '2018-08-01T00:00:00Z')
  group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid
  order by te.id, teai.attributeid
)
select tedata || case when attrdata <> '{"": [null]}'::jsonb then attrdata else tedata end as data from (
  select  ('{"@id":"' || id || '"}')::jsonb || 
          ('{"@type":["' || type || '"]}')::jsonb || 
          ('{"https://uri.etsi.org/ngsi-ld/createdAt":[ { "@type": "https://uri.etsi.org/ngsi-ld/DateTime", "@value": "' || createdat || '" }]}')::jsonb || 
          ('{"https://uri.etsi.org/ngsi-ld/modifiedAt":[ { "@type": "https://uri.etsi.org/ngsi-ld/DateTime", "@value": "' || modifiedat || '" }]}')::jsonb as tedata, 
          jsonb_object_agg(attributeid, attributedata) as attrdata
  from r
  group by id, type, createdat, modifiedat
  order by modifiedat desc
) as m;

\echo  type + temporal before
\echo  ?type=Vehicle&timerel=before&time=2018-08-01T00:00:00Z&timeproperty=observedAt
select te.id, te.type, te.createdat, te.modifiedat, teai.attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
from temporalentity te
left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
where
-- basic query
te.type = 'http://example.org/vehicle/Vehicle' and 
-- temporal query
((teai.static = true and teai.observedat is null)  or 
 teai.observedat <= '2018-08-01T00:00:00Z')
group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid;


\echo  type + temporal after
\echo  ?type=Vehicle&timerel=after&time=2018-08-01T00:00:00Z&timeproperty=observedAt
select te.id, te.type, te.createdat, te.modifiedat, teai.attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
from temporalentity te
left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
where
-- basic query
te.type = 'http://example.org/vehicle/Vehicle' and 
-- temporal query
((teai.static = true and teai.observedat is null)  or 
 teai.observedat >= '2018-08-01T00:00:00Z')
group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid;

\echo  type + temporal between
\echo  ?type=Vehicle&timerel=between&time=2019-08-01T00:00:00Z&endtime==2014-08-01T00:00:00Z&timeproperty=observedAt
select te.id, te.type, te.createdat, te.modifiedat, teai.attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
from temporalentity te
left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
where
-- basic query
te.type = 'http://example.org/vehicle/Vehicle' and 
-- temporal query
((teai.static = true and teai.observedat is null)  or 
 teai.observedat between '2014-08-01T00:00:00Z' and '2019-08-01T00:00:00Z')
group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid;


\echo  attrs + temporal
\echo  ?attrs=speed,brandName&timerel=after&time=2018-08-01T00:00:00Z&timeproperty=observedAt
select te.id, te.type, te.createdat, te.modifiedat, teai.attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
from temporalentity te
left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
where
-- attrs query
teai.attributeid in ('http://example.org/vehicle/speed', 'http://example.org/vehicle/brandName') and
-- temporal query
((teai.static = true and teai.observedat is null)  or 
 teai.observedat >= '2018-08-01T00:00:00Z')
group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid;

\echo  type + attrs + temporal
\echo  ?type=Vehicle&attrs=speed,brandName&timerel=after&time=2018-08-01T00:00:00Z&timeproperty=observedAt
select te.id, te.type, te.createdat, te.modifiedat, teai.attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
from temporalentity te
left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
where -- basic query 
      te.type = 'http://example.org/vehicle/Vehicle' and 
      -- attrs query
      teai.attributeid in ('http://example.org/vehicle/speed', 'http://example.org/vehicle/brandName') and
      -- temporal query
      ((teai.static = true and teai.observedat is null)  or 
       teai.observedat >= '2018-08-01T00:00:00Z')
group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid;

\echo  example from appendix C.5.5.2:
\echo  type + attrs + temporal + advanced query
\echo  ?type=Vehicle&q=brandName!=Mercedes&attrs=speed,brandName&timerel=between&time=2018-08-01:12:00:00Z&endTime=2018-08-01:13:00:00Z
select te.id, te.type, te.createdat, te.modifiedat, teai.attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
from temporalentity te
left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
where -- basic query (id, type, idPattern)
      te.type = 'http://example.org/vehicle/Vehicle' and 
      -- attrs query
      teai.attributeid in ('http://example.org/vehicle/speed', 'http://example.org/vehicle/brandName') and
      -- temporal query
      ((teai.static = true and teai.observedat is null)  or -- temporal filters do not apply to static attributes
      teai.observedat >= '2018-08-01T00:00:00Z') and
      -- advanced query
      (teai.attributeid != 'http://example.org/vehicle/brandName' or 
        (teai.attributeid = 'http://example.org/vehicle/brandName' and
         teai.value <> '"Mercedes"'))
group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid;

\echo type + temporal + advanced query
\echo ?type=Vehicle&q=brandName!=Mercedes&timerel=after&time=2018-08-01T00:00:00Z&timeproperty=observedAt
select te.id, te.type, te.createdat, te.modifiedat, teai.attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
from temporalentity te
left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
where -- basic query 
      te.type = 'http://example.org/vehicle/Vehicle' and 
      -- temporal query
      ((teai.static = true and teai.observedat is null) or 
        teai.observedat >= '2018-08-01T00:00:00Z') and 
      -- advanced query
      (teai.attributeid != 'http://example.org/vehicle/brandName' or 
        (teai.attributeid = 'http://example.org/vehicle/brandName' and
        teai.value <> '"Mercedes"'))
group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid;


-- If geo-query is present, from S2, select those Entities whose GeoProperty instances meet the geospatial restrictions
-- imposed by the geo-query (as mandated by clause 4.10); those geospatial restrictions shall be checked against the GeoProperty 
-- instances that are within the interval defined by the temporal query. Let S3 be this new subset.
\echo type + temporal + geoquery
\echo ?type=Vehicle&timerel=after&time=2018-08-01T00:00:00Z&geoproperty=location&georel=near;maxDistance==360&geometry=Point&coordinates=%5B-8.5%2C43.2%5D
with r as (
  select te.id, te.type, te.createdat, te.modifiedat, teai.attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
  from temporalentity te
  left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
  where -- basic query
        te.type = 'http://example.org/vehicle/Vehicle' and 
        -- temporal query
        ((teai.static = true and teai.observedat is null) or 
         teai.observedat >= '2018-08-01T00:00:00Z')
  group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid
)
select  ('{"@id":"' || id || '"}')::jsonb || 
        ('{"@type":["' || type || '"]}')::jsonb || 
        jsonb_object_agg(attributeid, attributedata) as data
from r
--geoquery
where exists (
  select 1 
  from temporalentityattrinstance 
  where temporalentity_id = r.id and 
        attributeid = 'https://uri.etsi.org/ngsi-ld/location' and 
        attributetype = 'https://uri.etsi.org/ngsi-ld/GeoProperty' and
        ((static = true and observedat is null) or 
         observedat >= '2018-08-01T00:00:00Z') and 
        ST_DWithin(  
          geovalue::geography,
          ST_GeomFromGeoJSON( '{
            "type": "Point",
            "coordinates": [
              -8.5,43.2
            ]
          }')::geography,
          360
        )
)
group by id, type;

\echo type + temporal + custom timeproperty
\echo ?type=Vehicle&timerel=after&time=2019-08-01T00:00:00Z&timeproperty=testedAt
with r as (
  select te.id, te.type, te.createdat, te.modifiedat, teai.attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
  from temporalentity te
  left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
  where -- basic query
        te.type = 'http://example.org/vehicle/Vehicle' and 
        -- temporal query
        ((teai.static = true and data?'http://example.org/vehicle/testedAt' = false) or 
         (
          data#>>'{http://example.org/vehicle/testedAt,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@type}' = 'https://uri.etsi.org/ngsi-ld/DateTime' and 
          (data#>>'{http://example.org/vehicle/testedAt,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::timestamp >= '2019-08-01T00:00:00Z'::timestamp
         )
        )
  group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid
)
select  ('{"@id":"' || id || '"}')::jsonb || 
        ('{"@type":["' || type || '"]}')::jsonb || 
        jsonb_object_agg(attributeid, attributedata) as data
from r
group by id, type;

\echo type + temporal + geoquery + custom timeproperty
\echo ?type=Vehicle&timerel=after&time=2019-08-01T00:00:00Z&timeproperty=testedAt&geoproperty=location&georel=near;maxDistance==360&geometry=Point&coordinates=%5B-8.5%2C43.2%5D
with r as (
  select te.id, te.type, te.createdat, te.modifiedat, teai.attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
  from temporalentity te
  left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
  where -- basic query
        te.type = 'http://example.org/vehicle/Vehicle' and 
        -- temporal query
        ((teai.static = true and data?'http://example.org/vehicle/testedAt' = false) or 
         (
          data#>>'{http://example.org/vehicle/testedAt,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@type}' = 'https://uri.etsi.org/ngsi-ld/DateTime' and 
          (data#>>'{http://example.org/vehicle/testedAt,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::timestamp >= '2019-08-01T00:00:00Z'::timestamp
         )
        )
  group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid
)
select  ('{"@id":"' || id || '"}')::jsonb || 
        ('{"@type":["' || type || '"]}')::jsonb || 
        jsonb_object_agg(attributeid, attributedata) as data
from r
--geoquery
where exists (
  select 1
  from temporalentityattrinstance 
  where temporalentity_id = r.id and 
        attributeid = 'https://uri.etsi.org/ngsi-ld/location' and 
        attributetype = 'https://uri.etsi.org/ngsi-ld/GeoProperty' and
        ((static = true and data?'http://example.org/vehicle/testedAt' = false) or          
         (
          data#>>'{http://example.org/vehicle/testedAt,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@type}' = 'https://uri.etsi.org/ngsi-ld/DateTime' and 
          (data#>>'{http://example.org/vehicle/testedAt,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::timestamp >= '2019-08-01T00:00:00Z'::timestamp
         )                  
        ) and 
        ST_DWithin(  
          geovalue::geography,
          ST_GeomFromGeoJSON( '{
            "type": "Point",
            "coordinates": [
              -8.5,43.2
            ]
          }')::geography,
          360
        )
)
group by id, type;

-- advanced query using jsonb operator
\echo type + temporal + advanced query
\echo ?type=Vehicle&timerel=after&time=2000-08-01T00:00:00Z&q=speed>=100
with r as (
  select te.id, te.type, te.createdat, te.modifiedat, teai.attributeid, jsonb_agg(teai.data order by teai.modifiedat desc) as attributedata
  from temporalentity te
  left join temporalentityattrinstance teai on (teai.temporalentity_id = te.id)
  where -- basic query
        te.type = 'http://example.org/vehicle/Vehicle' and 
        -- temporal query
        ( (teai.static = true and teai.observedat is null) or  -- temporal filters do not apply to static attributes
          teai.observedat >= '2000-08-01T00:00:00Z') and
        -- advanced query
        (teai.attributeid = 'http://example.org/vehicle/speed' and
         teai.data@>'{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}' and 
         teai.data#>'{https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' >= '100'::jsonb)
  group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid
)
select  ('{"@id":"' || id || '"}')::jsonb || 
        ('{"@type":["' || type || '"]}')::jsonb || 
        jsonb_object_agg(attributeid, attributedata) as data
from r
group by id, type;
