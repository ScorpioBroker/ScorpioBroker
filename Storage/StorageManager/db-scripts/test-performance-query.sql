\pset pager 0
\timing

\echo Query entity urn:ngsi-ld:Vehicle:AP12345
select jsonb_pretty(data)
  from entity 
where id = 'urn:ngsi-ld:Vehicle:AP12345';

\echo Query entity urn:ngsi-ld:Vehicle:AP99997 using a JSONB filter operator
--explain analyze
select data
  from entity 
  where data @> '{"@id": "urn:ngsi-ld:Vehicle:AP99997"}';  -- 5ms. Bitmap Index Scan on i_entity_data  (cost=0.00..51.73 rows=498 width=0) (actual time=0.117..0.117 rows=1 loops=1)
-- many ways to do the same filter. to check which one is faster (and uses GIN indexes)
-- where data->'@id' = '"urn:ngsi-ld:Vehicle:AP99997"';  -- 1096ms. Parallel Seq Scan on entity  (cost=0.00..124240.91 rows=1038 width=766) (actual time=1060.945..1078.399 rows=0 loops=3)
-- where data->>'@id' = 'urn:ngsi-ld:Vehicle:AP999997'; -- 388ms (already cached). Parallel Seq Scan on entity  (cost=0.00..124240.91 rows=1038 width=766) (actual time=377.601..381.259 rows=0 loops=3)
-- where data#>>'{@id}' = 'urn:ngsi-ld:Vehicle:AP999997'; -- 384ms (already cached). Parallel Seq Scan on entity  (cost=0.00..124240.91 rows=1038 width=766) (actual time=359.061..377.245 rows=0 loops=3)

\echo Query entity using a JSONB filter operator in a property of property
explain analyze
select data
  from entity 
  where data@>'{"http://example.org/common/isParked": [ {"http://example.org/common/providedBy": [ { "https://uri.etsi.org/ngsi-ld/hasObject": [ {"@id": "urn:ngsi-ld:Person:Bob234555"}] }] } ] }';  -- 1ms. bitmap heap scan + bitmap index scan
-- many ways to do the same filter. to check which one is faster (and uses GIN indexes)
-- where data#>'{http://example.org/common/isParked,0,http://example.org/common/providedBy,0,https://uri.etsi.org/ngsi-ld/hasObject,0,"@id"}' = '"urn:ngsi-ld:Person:Bob573426"';  -- 2160ms. Parallel Seq Scan on entity  (cost=0.00..128163.63 rows=1057 width=740) (actual time=2152.319..2152.319 rows=0 loops=3)
-- where data->'http://example.org/common/isParked'->0->'http://example.org/common/providedBy'->0->'https://uri.etsi.org/ngsi-ld/hasObject'->0->'@id' = '"urn:ngsi-ld:Person:Bob573426"'; -- 2161ms. Parallel Seq Scan on entity  (cost=0.00..131335.26 rows=1057 width=740) (actual time=2151.097..2151.097 rows=0 loops=3)

\echo Query NGSI-LD pre-defined members
select id, type, createdAt, modifiedAt, location, ST_AsText(location) as location_text
  from entity 
limit 1;

\echo Retrieving all entities whose the speed is greater or equals (>=) to 400
--explain analyze
select data, (data#>>'{http://example.org/vehicle/speed,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::numeric as speed_numeric_value, data#>'{speed,value}' as speed_jsonb_value
  from entity 
  where data@>'{"http://example.org/vehicle/speed":[{"@type":["https://uri.etsi.org/ngsi-ld/Property"]}]}' and 
    data#>'{http://example.org/vehicle/speed,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' >= '400'::jsonb; -- 2700ms. Seq Scan on entity  (cost=0.00..133886.39 rows=161571 width=841) (actual time=666.328..969.293 rows=3 loops=1) /  Filter: ((data #> '{speed,value}'::text[]) >= '400'::jsonb)
-- other ways of doing the same filter, check which one is faster
-- where (data#>>'{http://example.org/vehicle/speed,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::numeric >= 400; -- 2991ms. Seq Scan on entity  (cost=0.00..136309.95 rows=161571 width=841) (actual time=780.223..1162.166 rows=3 loops=1)/   Filter: (((data #>> '{speed,value}'::text[]))::numeric >= '400'::numeric)
-- where data->'http://example.org/vehicle/speed'->0->'https://uri.etsi.org/ngsi-ld/hasValue'->0->'@value' >= '400'::jsonb; -- 2623. Seq Scan on entity ......
--  where data @> '{"http://example.org/vehicle/speed": {}}' and data#>'{http://example.org/vehicle/speed,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' >= '400'::jsonb; -- 2696ms. bitmap heap sacn + filter + bitmap heap scan
-- where data ? 'http://example.org/vehicle/speed' and data#>'{http://example.org/vehicle/speed,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' >= '400'::jsonb; -- 4674 ms. bitmap heap + filter + bitmap heap scan

\echo Test using subquery
--explain analyze 
select data, (data#>>'{http://example.org/vehicle/speed,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::numeric as speed_numeric_value, data#>'{speed,value}' as speed_jsonb_value
  from (select data from entity where data ? 'http://example.org/vehicle/speed') as e 
  where data#>'{http://example.org/vehicle/speed,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' >= '400'::jsonb;
    -- 4520ms. ...

\echo Retrieving all entities whose the speed equals to 505
--explain analyze
select data, (data#>>'{http://example.org/vehicle/speed,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::numeric as speed_numeric_value, 
             data#>'{http://example.org/vehicle/speed,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as speed_jsonb_value
  from entity 
--  where data#>'{http://example.org/vehicle/speed,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' = '505'::jsonb; -- 1615ms. parallel seq scan
  where data @> '{"http://example.org/vehicle/speed": [ {"https://uri.etsi.org/ngsi-ld/hasValue": [ { "@value": 505 }]}]}'; -- 0.6ms. bitmap heap scan + Bitmap Index Scan on i_entity_data
  

\echo Test Geolocation
\echo First geofence test... get every entity within the radius of 50 meters from point (0,0)
select id, 
  ST_AsText( location )  as location_geovalue_text
  from entity
  WHERE    
    ST_DWithin( location::geography,
      ST_GeomFromText('POINT(0 0)', 0)::geography,
      50
    ) 
    limit 5;
-- 0.5ms

\echo Testing also the jsonb type (this is needed even for the "location" field, because of null values)
select id, 
  ST_AsText( location )  as location_geovalue_text
  from entity
  WHERE    
    data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' and    
    ST_DWithin( location::geography,
      ST_GeomFromText('POINT(0 0)', 0)::geography,
      50
    ) 
    limit 5;
-- 238ms


\echo Second geofence test... get every entity within the radius of 1 meters from point (0,0)
select id, -- data,
  ST_AsText( location )  as location_geovalue_text
  from entity
  WHERE    
    ST_DWithin( location::geography,
      ST_GeomFromText('POINT(0 0)', 0)::geography,
      1
    )
    limit 5;
-- 0.8ms

\echo Test geolocation reading data from JSONB column
select id, -- data,
  ST_AsText( ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' )) as geovalue_text
  from entity
  WHERE    
    data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' and 
    ST_DWithin(  ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::geography,
      ST_GeomFromText('POINT(0 0)', 0)::geography,
      50
    )
    limit 5;
-- 1243ms



\echo Update speed to 1500 from entity urn:ngsi-ld:Vehicle:AP12347
update entity set data = 
'{
  "@id": "urn:ngsi-ld:Vehicle:AP12347",
  "@type": [
    "http://example.org/vehicle/Vehicle"
  ],
  "http://example.org/vehicle/speed": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": 1500
        }
      ]
    }
  ],
  "http://example.org/common/isParked": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Relationship"
      ],
      "http://example.org/common/providedBy": [
        {
          "@type": [
            "https://uri.etsi.org/ngsi-ld/Relationship"
          ],
          "https://uri.etsi.org/ngsi-ld/hasObject": [
            {
              "@id": "urn:ngsi-ld:Person:Bob10111374"
            }
          ]
        }
      ],
      "https://uri.etsi.org/ngsi-ld/hasObject": [
        {
          "@id": "urn:ngsi-ld:OffStreetParking:Downtown543224"
        }
      ],
      "https://uri.etsi.org/ngsi-ld/observedAt": [
        {
          "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
          "@value": "2017-07-29T12:00:04"
        }
      ]
    }
  ],
  "http://example.org/vehicle/brandName": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "Mercedes"
        }
      ]
    }
  ],
  "https://uri.etsi.org/ngsi-ld/location": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/GeoProperty"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
          {
            "@value":
              "{ \"type\":\"Point\", \"coordinates\":[ -2786.6, 1748.9 ] }"
          }
        ]
    }
  ],
  "https://uri.etsi.org/ngsi-ld/createdAt": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "2018-10-04T14:31:20"
        }
      ]
    }
  ],
  "https://uri.etsi.org/ngsi-ld/modifiedAt": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "2018-10-04T16:31:20"
        }
      ]
    }
  ],
  "https://uri.etsi.org/ngsi-ld/observedAt": [
    {
      "@type": [
        "https://uri.etsi.org/ngsi-ld/Property"
      ],
      "https://uri.etsi.org/ngsi-ld/hasValue": [
        {
          "@value": "2018-06-20T11:59:54"
        }
      ]
    }
  ]
}
'
where id = 'urn:ngsi-ld:Vehicle:AP12347';
-- 8ms

\echo Query again
select id, data->'http://example.org/vehicle/speed' 
from entity
  where id = 'urn:ngsi-ld:Vehicle:AP12347';

