\echo old version... not in expanded form. exiting...
\q

\pset pager 0

\echo Query entity urn:ngsi-ld:Vehicle:A4567 
select jsonb_pretty(data)
  from entity 
where id = 'urn:ngsi-ld:Vehicle:A4567';

\echo again, using jsonb filter
select data, data->'id'
  from entity 
 where data @> '{"id": "urn:ngsi-ld:Vehicle:A4567"}'; -- fastest way, use indexes
-- other ways to do the same filter, but "@>" is the only that use GIN indexes
-- where data->'id' = '"urn:ngsi-ld:Vehicle:A4567"';
-- where data->>'id' = 'urn:ngsi-ld:Vehicle:A4567';

\echo Query NGSI-LD pre-defined members
select id, type, createdAt, modifiedAt, observedAt, location, ST_AsText(location) as location_text
  from entity 
limit 3;

\echo Retrieving all entities whose the property availableSpotNumber is greater or equals (>=) to 100
select data, (data#>>'{availableSpotNumber,value}')::numeric as spot_numeric_value, data#>'{availableSpotNumber,value}' as spot_jsonb_value
  from entity 
  where data#>'{availableSpotNumber,value}' >= '100'::jsonb;
-- other ways of doing the same filter, check which one is faster
-- where (data#>>'{availableSpotNumber,value}')::numeric >= 100;	
-- where data->'availableSpotNumber'->'value' >= '100'::jsonb;

\echo Test Geolocation
\echo First geofence test... get every entity within the radius of 50 meters from point (0,0)
select data,
  ST_AsText( location )  as location_geovalue_text
  from entity
  WHERE    
    ST_DWithin( location,
      ST_GeomFromText('POINT(0 0)', 0),
      50
    );

\echo Second geofence test... get every entity within the radius of 2 meters from point (0,0)
select data,
  ST_AsText( location )  as location_geovalue_text
  from entity
  WHERE    
    ST_DWithin( location,
      ST_GeomFromText('POINT(0 0)', 0),
      2
    );


\echo Test geolocation reading data from JSONB, using property name "anotherLocation"
select data,
  ST_AsText( ST_GeomFromGeoJSON( data#>'{anotherLocation}'#>>'{value}') ) as geovalue_text
  from entity
  WHERE    
    data#>>'{anotherLocation,type}' = 'GeoProperty' and 
    ST_DWithin(  ST_GeomFromGeoJSON( data#>>'{anotherLocation,value}'),
      ST_GeomFromText('POINT(0 0)', 0),
      50
    );


\echo Test multi-values
\echo Query tirePressure
select id, data->'tirePressure' 
from entity
  where id = 'urn:ngsi-ld:Vehicle:A9999';

\echo Complex querying multi-values
select e.id, e.data->'tirePressure'->0->'value' as tirepressure_0, t.value->'id' as tire_id, t.value->'value' as tire_value
from entity e
    cross join jsonb_array_elements(data->'tirePressure') t
  where id = 'urn:ngsi-ld:Vehicle:A9999' and 
        t.value->'value' > '6'::jsonb ;


\echo Test historical data
delete from entity_history;

\echo Update tire1 pressure from 5 to 9
update entity set data = '{  
  "id":"urn:ngsi-ld:Vehicle:A9999",
  "type":"Vehicle",
  "brandName":{
    "type":"Property",
    "value":"Mercedes"
  },
  "tirePressure": [{
                    "id": "tire1",
                    "type":"Property", 
                    "value": 9
                  }, 
                  {
                    "id": "tire2",
                    "type":"Property", 
                    "value": 7
                  }],
  "isParked":{
    "type":"Relationship",
    "object":"urn:ngsi-ld:OffStreetParking:Downtown1",
    "observedAt":"2017-07-29T12:00:04",
    "providedBy":{
      "type":"Relationship",
      "object":"urn:ngsi-ld:Person:Bob"
    }
  },
  "createdAt":{
    "type":"Property",
    "value":"2018-06-21T12:00:04"
  },
  "modifiedAt":{
    "type":"Property",
    "value":"2018-06-21T12:00:04"
  },
  "observedAt":{
    "type":"Property",
    "value":"2018-06-20T11:59:54"
  }
}'
where id = 'urn:ngsi-ld:Vehicle:A9999';

\echo Query tirePressure again
select id, data->'tirePressure' 
from entity
  where id = 'urn:ngsi-ld:Vehicle:A9999';

\echo Query history
select id, modifiedat, data->'tirePressure', sys_period from entity_history
  where id = 'urn:ngsi-ld:Vehicle:A9999' ;

