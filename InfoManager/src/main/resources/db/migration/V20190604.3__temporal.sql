BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS temporalentity (
  id TEXT NOT NULL,
  type TEXT,
  createdAt TIMESTAMP,
  modifiedAt TIMESTAMP,
  PRIMARY KEY (id))
;

CREATE TABLE IF NOT EXISTS temporalentityattrinstance (
  internalid BIGSERIAL,
  temporalentity_id TEXT NOT NULL REFERENCES temporalentity(id) ON DELETE CASCADE ON UPDATE CASCADE,
  attributeid TEXT NOT NULL,
  instanceid TEXT,
  attributetype TEXT,
  value TEXT, -- object (relationship) is also stored here
  geovalue GEOMETRY,
  createdat TIMESTAMP,
  modifiedat TIMESTAMP,
  observedat TIMESTAMP,
  data JSONB NOT NULL,
  static BOOL NOT NULL,
  PRIMARY KEY (internalid))
;
CREATE UNIQUE INDEX i_temporalentityattrinstance_entityid_attributeid_instanceid ON temporalentityattrinstance (temporalentity_id, attributeid, instanceid);

-- trigger to automatically extract pre-defined ngsi-ld members and store them in regular fields (for query performance)
CREATE OR REPLACE FUNCTION temporalentityattrinstance_extract_jsonb_fields() RETURNS trigger AS $_$
    DECLARE 
        f_internalid temporalentityattrinstance.internalid%TYPE;
    BEGIN
        IF TG_OP = 'INSERT' OR NEW.data <> OLD.data THEN -- do not reprocess if it is just an update on other column (e.g. static)
            NEW.attributetype = NEW.data#>>'{@type,0}';

            NEW.instanceid = NEW.data#>>'{https://uri.etsi.org/ngsi-ld/instanceId,0,@id}';
            
            NEW.createdat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/createdAt,0,@value}')::TIMESTAMP;
            NEW.modifiedat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/modifiedAt,0,@value}')::TIMESTAMP;
            NEW.observedat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/observedAt,0,@value}')::TIMESTAMP;

            IF NEW.attributeid IN ('https://uri.etsi.org/ngsi-ld/createdAt', 'https://uri.etsi.org/ngsi-ld/modifiedAt', 'https://uri.etsi.org/ngsi-ld/observedAt') THEN
                NEW.value = NEW.data#>'{@value}';
            ELSE 
                IF (NEW.data?'https://uri.etsi.org/ngsi-ld/hasValue') THEN
                    NEW.value = NEW.data#>'{https://uri.etsi.org/ngsi-ld/hasValue,0,@value}';  -- TODO: confirm if #> or #>>
                ELSIF (NEW.data?'https://uri.etsi.org/ngsi-ld/hasObject') THEN
                    NEW.value = NEW.data#>'{https://uri.etsi.org/ngsi-ld/hasObject,0,@id}';
                ELSE
                    NEW.value = NULL;
                END IF;
            END IF;

            IF NEW.attributetype = 'https://uri.etsi.org/ngsi-ld/GeoProperty' THEN
                NEW.geovalue = ST_SetSRID(ST_GeomFromGeoJSON( NEW.data#>>'{https://uri.etsi.org/ngsi-ld/hasValue,0,@value}' ), 4326);
            ELSE 
                NEW.geovalue = NULL;
            END IF;
        END IF;

        IF TG_OP = 'INSERT' THEN
            select into f_internalid internalid from temporalentityattrinstance 
                where temporalentity_id = NEW.temporalentity_id and attributeid = NEW.attributeid limit 1;
            IF FOUND THEN
                NEW.static = FALSE;
                UPDATE temporalentityattrinstance SET static = false 
                    WHERE internalid = f_internalid and static = true;
            ELSE 
                NEW.static = TRUE;
            END IF;
        END IF;

        RETURN NEW;
    END;
$_$ LANGUAGE plpgsql;

CREATE TRIGGER temporalentityattrinstance_extract_jsonb_fields BEFORE INSERT OR UPDATE ON temporalentityattrinstance
    FOR EACH ROW EXECUTE PROCEDURE temporalentityattrinstance_extract_jsonb_fields();

CREATE OR REPLACE FUNCTION temporalentityattrinstance_update_static() RETURNS trigger AS $_$
    DECLARE 
        f_internalid temporalentityattrinstance.internalid%TYPE;
        f_count integer;
    BEGIN
        select into f_internalid, f_count min(internalid), count(1) from temporalentityattrinstance 
            where temporalentity_id = OLD.temporalentity_id AND attributeid = OLD.attributeid;
        IF (f_count = 1) THEN
            UPDATE temporalentityattrinstance SET static = true WHERE internalid = f_internalid;
        END IF;
        RETURN OLD;
    END;
$_$ LANGUAGE plpgsql;

CREATE TRIGGER temporalentityattrinstance_update_static AFTER DELETE ON temporalentityattrinstance
    FOR EACH ROW EXECUTE PROCEDURE temporalentityattrinstance_update_static();

-- create indexes for performance

CREATE INDEX i_temporalentity_type ON temporalentity (type);

CREATE INDEX i_temporalentityattrinstance_data ON temporalentityattrinstance USING GIN (data); 

COMMIT;