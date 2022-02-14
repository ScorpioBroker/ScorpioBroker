\pset pager 0
\a
\t
\pset recordsep ' , '

\echo This SQL script generates a FeatureCollection to graphically visualize all csources geometries (based on "location" attribute)
\echo Just copy the json below and paste at http://geojson.io
\echo ------------------------
\echo '{  "type": "FeatureCollection",  "features": ['
select '{"type": "Feature",
      "properties": {},
      "geometry": ' || coalesce((data#>>'{https://uri.etsi.org/ngsi-ld/location,0,@value}')::text, 'null') || '}' as g
    from csource 
    where data ? 'https://uri.etsi.org/ngsi-ld/location';

\echo '] }'