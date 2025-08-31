DROP TABLE entitymap;

CREATE TABLE entitymap (
    map_id text NOT NULL,
    pos BIGINT NOT NULL,
    query_checksum text NOT NULL,
    entity_id text NOT NULL,
    remote_query text,
    csourceid text NOT NULL,
    last_access timestamp without time zone NOT NULL,
    expires_at timestamp without time zone NOT NULL
);

CREATE INDEX i_entitymap_id ON entitymap USING hash (map_id text_pattern_ops);

CREATE INDEX ON entitymap USING btree (expires_at ASC NULLS LAST)
WITH (deduplicate_items = True);
DROP INDEX "I_entity_types";
CREATE INDEX "I_entity_types" ON public.entity USING gin (e_types array_ops);
REINDEX TABLE entity;

CREATE OR REPLACE FUNCTION getgeojson(ldjson jsonb)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
COST 10
AS $BODY$
  SELECT CASE
    WHEN ldjson IS NULL THEN NULL
    ELSE jsonb_build_object(
      'type', substring(ldjson#>>'{@type,0}' FROM 32),
      'coordinates', getcoordinates(ldjson#>'{https://purl.org/geojson/vocab#coordinates,0,@list}')
    )
  END;
$BODY$;


CREATE OR REPLACE FUNCTION getcoordinates(IN coordinatelist jsonb)
    RETURNS jsonb
    LANGUAGE 'sql'
    IMMUTABLE
    PARALLEL SAFE
    COST 5
AS $BODY$
SELECT jsonb_agg(
  CASE 
    WHEN elem ? '@list' THEN getcoordinates(elem->'@list')
    ELSE elem->'@value'
  END
)
FROM jsonb_array_elements(coordinatelist) AS elem;
$BODY$;


CREATE OR REPLACE FUNCTION public.entity_extract_jsonb_fields()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
COST 50
AS $$
BEGIN
  -- Only proceed if ENTITY has changed or is newly set
  IF TG_OP = 'INSERT' OR
     (TG_OP = 'UPDATE' AND (
        OLD.ENTITY IS DISTINCT FROM NEW.ENTITY
     )) THEN

    -- Extract timestamps
    NEW.createdat := (NEW.ENTITY#>>'{https://uri.etsi.org/ngsi-ld/createdAt,0,@value}')::timestamp;
    NEW.modifiedat := (NEW.ENTITY#>>'{https://uri.etsi.org/ngsi-ld/modifiedAt,0,@value}')::timestamp;

    -- Extract location if it's a GeoProperty
    IF NEW.ENTITY @> '{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }' THEN
      NEW.location := ST_SetSRID(
        ST_GeomFromGeoJSON(
          getGeoJson(NEW.ENTITY#>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0}')
        ),
        4326
      );
    ELSE
      NEW.location := NULL;
    END IF;

    -- Extract scopes if present
    NEW.scopes := CASE
      WHEN NEW.ENTITY ? 'https://uri.etsi.org/ngsi-ld/scope'
      THEN getScopes(NEW.ENTITY#>'{https://uri.etsi.org/ngsi-ld/scope}')
      ELSE NULL
    END;
  END IF;

  RETURN NEW;
END;
$$;


CREATE INDEX i_teai_covering_observedat_index
ON temporalentityattrinstance (temporalentity_id, attributeid, observedat DESC NULLS LAST)
INCLUDE (data, geovalue, createdat, modifiedat, deletedat);

CREATE INDEX i_teai_covering_createdat_index
ON temporalentityattrinstance (temporalentity_id, attributeid, createdat DESC NULLS LAST)
INCLUDE (data, geovalue, observedat, modifiedat, deletedat);

CREATE INDEX i_teai_covering_modifiedat_index
ON temporalentityattrinstance (temporalentity_id, attributeid, modifiedat DESC NULLS LAST)
INCLUDE (data, geovalue, createdat, observedat, deletedat);