CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS Entity (
  id TEXT NOT NULL,
  type TEXT,
  data JSONB NOT NULL,
  context JSONB,
  createdAt TIMESTAMP,
  modifiedAt TIMESTAMP,
  location GEOMETRY(Geometry, 4326), -- 4326 (WGS84) is the Coordinate System defined in GeoJson spec: https://tools.ietf.org/html/rfc7946#section-4
  observationSpace GEOMETRY(Geometry, 4326),
  operationSpace GEOMETRY(Geometry, 4326),
  PRIMARY KEY (id))
;

-- trigger to automatically extract pre-defined ngsi-ld members and store them in regular fields (for query performance)
CREATE OR REPLACE FUNCTION entity_extract_jsonb_fields() RETURNS trigger AS $_$
    BEGIN
        -- is any validation needed?
        NEW.type = NEW.data#>>'{@type,0}';
        NEW.createdat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/createdAt,0,@value}')::TIMESTAMP;
        NEW.modifiedat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/modifiedAt,0,@value}')::TIMESTAMP;

        IF (NEW.data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
            NEW.location = ST_SetSRID(ST_GeomFromGeoJSON( NEW.data#>>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' ), 4326);
        ELSE 
            NEW.location = NULL;
        END IF;
        IF (NEW.data@>'{"https://uri.etsi.org/ngsi-ld/observationSpace": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] }] }') THEN
            NEW.observationSpace = ST_SetSRID( ST_GeomFromGeoJSON( NEW.data#>>'{https://uri.etsi.org/ngsi-ld/observationSpace,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' ), 4326);
        ELSE 
            NEW.observationSpace = NULL;
        END IF;
        IF (NEW.data@>'{"https://uri.etsi.org/ngsi-ld/operationSpace": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
            NEW.operationSpace = ST_SetSRID( ST_GeomFromGeoJSON( NEW.data#>>'{https://uri.etsi.org/ngsi-ld/operationSpace,0,https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' ), 4326);
        ELSE 
            NEW.operationSpace = NULL;
        END IF;

        RETURN NEW;
    END;
$_$ LANGUAGE plpgsql;

CREATE TRIGGER entity_extract_jsonb_fields BEFORE INSERT OR UPDATE ON entity
    FOR EACH ROW EXECUTE PROCEDURE entity_extract_jsonb_fields();

-- create indexes for performance
CREATE INDEX i_entity_type ON entity (type);
CREATE INDEX i_entity_createdat ON entity (createdat);
CREATE INDEX i_entity_modifiedat ON entity (modifiedat);
CREATE INDEX i_entity_location ON entity USING GIST (location);
CREATE INDEX i_entity_observationspace ON entity USING GIST (observationspace);
CREATE INDEX i_entity_operationspace ON entity USING GIST (operationspace);

-- to check if this index will be used by the database optimizer, or if it should be applied only for for certain keys
-- check https://www.postgresql.org/docs/current/static/datatype-json.html
CREATE INDEX i_entity_data ON entity USING GIN (data); 
