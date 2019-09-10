\pset pager 0
-- \set ECHO queries

/*
andOp = %x3B                       ; ;
equal = %x3D %x3D                  ; == 
georel = nearRel / withinRel / containsRel / overlapsRel / intersectsRel / equalsRel / disjointRel
nearRel = nearOp andOp distance equal PositiveNumber   ; near;max(min)Distance==x (in meters)
distance = "maxDistance" / "minDistance"
nearOp = "near"
withinRel = "within"
containsRel = "contains"
intersectsRel = "intersects"
equalsRel = "equals"
disjointRel = "disjoint"
overlapsRel = "overlaps"

PositiveNumber shall be a non-zero positive number as mandated by the JSON Specification. Thus, it shall follow the ABNF Grammar, production rule named Number, section 6 of [6], excluding the minus' symbol and excluding the number 0.

---
Reference geometries shall be specified by:
- A geometry type (parameter name geometry) as defined by the GeoJSON specification ([8], section 1.4), except GeometryCollection.

[8] 1.4:
    the term "geometry type" refers to seven
    case-sensitive strings: "Point", "MultiPoint", "LineString",
    "MultiLineString", "Polygon", "MultiPolygon", and
    "GeometryCollection".


- A coordinates (parameter name coordinates) element which shall represent the coordinates of the reference geometry as mandated by [8], section 3.1.1.

[8] 3.1.1
A position is the fundamental geometry construct.  The "coordinates" member of a Geometry object is composed of either:
   -  one position in the case of a Point geometry,
   -  an array of positions in the case of a LineString or MultiPoint
      geometry,
   -  an array of LineString or linear ring (see Section 3.1.6)
      coordinates in the case of a Polygon or MultiLineString geometry,
      or
   -  an array of Polygon coordinates in the case of a MultiPolygon
      geometry.
A position is an array of numbers.  There MUST be two or more
   elements.  The first two elements are longitude and latitude, or
   easting and northing, precisely in that order and using decimal
   numbers.

- near statement
    ... with distance (in meters) ...

*/

-- testing all GeoJSON geometry types in PostGis
\x
select id,
       ST_AsText( ST_GeomFromGeoJSON( data#>>'{http://example.org/pointExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') ) as point,
       ST_AsText( ST_GeomFromGeoJSON( data#>>'{http://example.org/lineStringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') ) as lineString,
       ST_AsText( ST_GeomFromGeoJSON( data#>>'{http://example.org/polygonExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') ) as polygonExample,
       ST_AsText( ST_GeomFromGeoJSON( data#>>'{http://example.org/polygonWithHolesExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') ) as polygonWithHolesExample,
       ST_AsText( ST_GeomFromGeoJSON( data#>>'{http://example.org/multiPointsExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') ) as multiPointsExample,
       ST_AsText( ST_GeomFromGeoJSON( data#>>'{http://example.org/multiLineStringExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') ) as multiLineStringExample,
       ST_AsText( ST_GeomFromGeoJSON( data#>>'{http://example.org/multiPolygonExample,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}') ) as multiPolygonExample
from entity
where id = 'urn:ngsi-ld:Test:GeometryTypes';
\x


\echo Distance between NEC Labs Heidelberg and AldiBergheim
with aux as (select location from entity where id = 'urn:ngsi-ld:Test:NecLabsHeidelberg' limit 1)
SELECT st_distance(aux.location::geography, entity.location::geography)
  FROM entity, aux
  where id = 'urn:ngsi-ld:Test:AldiBergheim' ;


\echo case 1
\echo georel = nearRel
\echo distance = "maxDistance"
\echo get every entity near NEC Labs Heidelberg, with distance up to 360 meters
\echo    NEC Labs Heidelberg coordinates: 8.684783577919006, 49.406131991436396
\echo geoproperty=location&georel=near;maxDistance==360&geometry=Point&coordinates=%5B8.684783577919006%2C49.406131991436396%5D
select id,
       data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as attr,
       ST_AsText( ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' ) ) as geovalue_text
  from entity
  where    
    data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' and
    ST_DWithin(  
      ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::geography,
      ST_GeomFromGeoJSON( '{
            "type": "Point",
            "coordinates": [
              8.684783577919006, 49.406131991436396
            ]
          }')::geography,
      360
    );


\echo as "location" is the field, a fastest version can be written as
select id,
       location as attr,
       ST_AsText( location ) as geovalue_text
  from entity
  where    
    ST_DWithin(  
      location::geography,
      ST_GeomFromGeoJSON( '{
            "type": "Point",
            "coordinates": [
              8.684783577919006, 49.406131991436396
            ]
          }')::geography,
      360
    );



\echo case 1.1
\echo Polygon geometry
\echo get every entity near NEC building (polygon), with distance up to 360 meters
\echo    NEC Labs Heidelberg building polygon coordinates: 
\echo [[[8.684628009796143,49.406062179606515],[8.685507774353027,49.4062262372493],[8.68545413017273,49.40634491690448],[8.684579730033875,49.40617736907259],[8.684628009796143,49.406062179606515]]]
\echo geoproperty=location&georel=near;maxDistance==50&geometry=Polygon&coordinates=%5B%5B%5B8.684628009796143%2C49.406062179606515%5D%2C%5B8.685507774353027%2C49.4062262372493%5D%2C%5B8.68545413017273%2C49.40634491690448%5D%2C%5B8.684579730033875%2C49.40617736907259%5D%2C%5B8.684628009796143%2C49.406062179606515%5D%5D%5D
select id,
       data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as attr,
       ST_AsText( ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' ) ) as geovalue_text
  from entity
  where    
    data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' and
    ST_DWithin(  
      ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' )::geography,
      ST_GeomFromGeoJSON( '{
            "type": "Polygon",
            "coordinates": [[[8.684628009796143,49.406062179606515],[8.685507774353027,49.4062262372493],[8.68545413017273,49.40634491690448],[8.684579730033875,49.40617736907259],[8.684628009796143,49.406062179606515]]]
          }')::geography,
      360
    );


\echo case 2
\echo georel = nearRel
\echo get every entity near NEC Labs Heidelberg, with distance over 400 meters
\echo distance = "minDistance"
\echo geoproperty=location&georel=near;minDistance==400&geometry=Point&coordinates=%5B8.684783577919006%2C49.406131991436396%5D
select id,
       data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as attr
  from entity
  where    
    data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' and
    NOT ST_DWithin(  
      ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' )::geography,
      ST_GeomFromGeoJSON( '{
            "type": "Point",
            "coordinates": [
              8.684783577919006, 49.406131991436396
            ]
          }')::geography,
      400
    );


\echo case 3
\echo georel = withinRel
\echo get every entity within NEC Labs building (polygon) in Heidelberg
\echo geoproperty=location&georel=within&geometry=Polygon&coordinates=%5B%5B%5B8.684628009796143%2C49.406062179606515%5D%2C%5B8.685507774353027%2C49.4062262372493%5D%2C%5B8.68545413017273%2C49.40634491690448%5D%2C%5B8.684579730033875%2C49.40617736907259%5D%2C%5B8.684628009796143%2C49.406062179606515%5D%5D%5D
select id,
       data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as attr
  from entity
  where    
    data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' and
    ST_Within(        
      ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}'),
      ST_GeomFromGeoJSON( '{
            "type": "Polygon",
            "coordinates": [[[8.684628009796143,49.406062179606515],[8.685507774353027,49.4062262372493],[8.68545413017273,49.40634491690448],[8.684579730033875,49.40617736907259],[8.684628009796143,49.406062179606515]]]
          }')
    );

\echo case 3.1
\echo georel = withinRel
\echo get every entity within Heidelberg or Karlsruhe ( MultiPolygon )
\echo geoproperty=location&georel=within&geometry=MultiPolygon&coordinates=%5B%5B%5B%5B8.686752319335938%2C49.359122687528746%5D%2C%5B8.742027282714844%2C49.3642654834877%5D%2C%5B8.767433166503904%2C49.398462568451485%5D%2C%5B8.768119812011719%2C49.42750021620163%5D%2C%5B8.74305725097656%2C49.44781634951542%5D%2C%5B8.669242858886719%2C49.43754770762113%5D%2C%5B8.63525390625%2C49.41968407776289%5D%2C%5B8.637657165527344%2C49.3995797187007%5D%2C%5B8.663749694824219%2C49.36851347448498%5D%2C%5B8.686752319335938%2C49.359122687528746%5D%5D%5D%2C%5B%5B%5B8.364715576171875%2C48.96939999849952%5D%2C%5B8.47320556640625%2C48.982019588328214%5D%2C%5B8.485565185546875%2C49.017157315497165%5D%2C%5B8.411407470703125%2C49.05677012268616%5D%2C%5B8.33587646484375%2C49.031565622700356%5D%2C%5B8.320770263671875%2C48.98562459864604%5D%2C%5B8.364715576171875%2C48.96939999849952%5D%5D%5D%5D
select id,
       data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as attr
  from entity
  where    
    data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' and
    ST_Within(        
      ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}'),
      ST_GeomFromGeoJSON( '{
            "type": "MultiPolygon",
            "coordinates": [[[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]],[[[8.364715576171875,48.96939999849952],[8.47320556640625,48.982019588328214],[8.485565185546875,49.017157315497165],[8.411407470703125,49.05677012268616],[8.33587646484375,49.031565622700356],[8.320770263671875,48.98562459864604],[8.364715576171875,48.96939999849952]]]]
          }')
    );

\echo case 4
\echo georel = containsRel
\echo get every entity that contains NEC Labs Heidelberg (point)
\echo geoproperty=location&georel=contains&geometry=Point&coordinates=%5B8.684783577919006%2C49.406131991436396%5D
select id,
       data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as attr
  from entity
  where    
    data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' and
    ST_Contains(      
      ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}'),
      ST_GeomFromGeoJSON( '{
            "type": "Point",
            "coordinates": [8.684783577919006,49.406131991436396]
          }')
    );

\echo case 5
\echo georel = overlapsRel
\echo get every entity that overlaps with NEC Labs building (polygon) in Heidelberg
\echo geoproperty=location&georel=overlaps&geometry=Polygon&coordinates=%5B%5B%5B8.684628009796143%2C49.406062179606515%5D%2C%5B8.685507774353027%2C49.4062262372493%5D%2C%5B8.68545413017273%2C49.40634491690448%5D%2C%5B8.684579730033875%2C49.40617736907259%5D%2C%5B8.684628009796143%2C49.406062179606515%5D%5D%5D
select id,
       data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as attr
  from entity
  where    
    data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' and
    ST_Overlaps(      
      ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}'),
      ST_GeomFromGeoJSON( '{
            "type": "Polygon",
            "coordinates": [[[8.684628009796143,49.406062179606515],[8.685507774353027,49.4062262372493],[8.68545413017273,49.40634491690448],[8.684579730033875,49.40617736907259],[8.684628009796143,49.406062179606515]]]
          }')
    );

\echo case 6
\echo georel = intersectsRel
\echo get every entity that intersects with NEC Labs building (polygon) in Heidelberg
-- ST_Intersects(g1, g2 ) --> Not (ST_Disjoint(g1, g2 ))
\echo geoproperty=location&georel=overlaps&geometry=Polygon&coordinates=%5B%5B%5B8.684628009796143%2C49.406062179606515%5D%2C%5B8.685507774353027%2C49.4062262372493%5D%2C%5B8.68545413017273%2C49.40634491690448%5D%2C%5B8.684579730033875%2C49.40617736907259%5D%2C%5B8.684628009796143%2C49.406062179606515%5D%5D%5D
select id,
       data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as attr
  from entity
  where    
    data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' and
    ST_Intersects(      
      ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}'),
      ST_GeomFromGeoJSON( '{
            "type": "Polygon",
            "coordinates": [[[8.684628009796143,49.406062179606515],[8.685507774353027,49.4062262372493],[8.68545413017273,49.40634491690448],[8.684579730033875,49.40617736907259],[8.684628009796143,49.406062179606515]]]
          }')
    );

\echo case 7
\echo georel = equalsRel
\echo get every entity equal to NEC Labs Heidelberg point
\echo geoproperty=location&georel=equals&geometry=Point&coordinates=%5B8.684783577919006%2C49.406131991436396%5D
select id,
       data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as attr
  from entity
  where    
    data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' and
    ST_Equals(      
      ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}'),
      ST_GeomFromGeoJSON( '{
            "type": "Point",
            "coordinates": [8.684783577919006,49.406131991436396]
          }')
    );

\echo case 8
\echo georel = disjointRel
\echo get entities that do not share any space together with NEC Labs Heidelberg building 
\echo geoproperty=location&georel=disjoint&geometry=Polygon&coordinates=%5B%5B%5B8.684628009796143%2C49.406062179606515%5D%2C%5B8.685507774353027%2C49.4062262372493%5D%2C%5B8.68545413017273%2C49.40634491690448%5D%2C%5B8.684579730033875%2C49.40617736907259%5D%2C%5B8.684628009796143%2C49.406062179606515%5D%5D%5D
select id,
       data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' as attr
  from entity
  where    
    data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' and
    ST_Disjoint(      
      ST_GeomFromGeoJSON( data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}'),
      ST_GeomFromGeoJSON( '{
            "type": "Polygon",
            "coordinates": [[[8.684628009796143,49.406062179606515],[8.685507774353027,49.4062262372493],[8.68545413017273,49.40634491690448],[8.684579730033875,49.40617736907259],[8.684628009796143,49.406062179606515]]]
          }')
    );

