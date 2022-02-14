\pset pager 0
-- \set ECHO queries

-- Query area:
-- { "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }

\echo case 1: near min -> expand + not within - distance: 8km
select id
  from csource
  where    
    id like 'urn:ngsi-ld:TestFedReg:%' AND
    NOT ST_Within(  
      location,
      ST_Buffer(
        ST_SetSRID(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }'), 4326)::geography,
        8000
      )::geometry 
    );

\echo case 2: near max -> expand + intersects - distance: 8km
select id
  from csource
  where    
    id like 'urn:ngsi-ld:TestFedReg:%' AND
    ST_Intersects(  
      location,
      ST_Buffer(
        ST_SetSRID(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }'), 4326)::geography,
        8000
      )::geometry 
    );

\echo case 3: within -> intersects
select id
  from csource
  where    
    id like 'urn:ngsi-ld:TestFedReg:%' AND
    ST_Intersects(  
      location,
      ST_SetSRID(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }'),4326)
    );

\echo case 4: contains -> contains
select id
  from csource
  where    
    id like 'urn:ngsi-ld:TestFedReg:%' AND
    ST_Contains(  
      location,
      ST_SetSRID(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }'),4326)
    );

\echo case 5: overlaps -> overlaps OR contains
select id
  from csource
  where    
    id like 'urn:ngsi-ld:TestFedReg:%' AND
    ( 
      ST_Overlaps(  
        location,
        ST_SetSRID(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }'), 4326)
      ) OR 
      ST_Contains(  
        location,
        ST_SetSRID(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }'), 4326)
      )
    );


\echo case 6: intersects -> intersects
select id
  from csource
  where    
    id like 'urn:ngsi-ld:TestFedReg:%' AND
    ST_Intersects(  
      location,
      ST_SetSRID(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }'), 4326)
    );

\echo case 7: equals -> contains
select id
  from csource
  where    
    id like 'urn:ngsi-ld:TestFedReg:%' AND
    ST_Contains(  
      location,
      ST_SetSRID(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }'), 4326)
    );

\echo case 8: disjoint -> not within
select id
  from csource
  where    
    id like 'urn:ngsi-ld:TestFedReg:%' AND
    NOT ST_Within(  
      location,
      ST_SetSRID(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }'), 4326)
    );    


-- tests srid and buffer
select
  ST_AsGeoJSON(
    ST_Buffer(
      ST_SetSRID(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }'), 4326)::geography,
      8000
    )
  ),
  st_srid(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }')) as srid_default,
  st_srid(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }')::geography) as srid_geography,
  st_srid(
    ST_Buffer(
      ST_SetSRID(ST_GeomFromGeoJSON( '{ "type": "Polygon", "coordinates": [ [ [ 8.400421142578125, 49.32333182991094 ], [ 8.812408447265625, 49.32333182991094 ], [ 8.812408447265625, 49.49489061140408 ], [ 8.400421142578125, 49.49489061140408 ], [ 8.400421142578125, 49.32333182991094 ] ] ] }'), 4326)::geography,
      8000
    )
  ) as srid_buffer_geography
  ;
