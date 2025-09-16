CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS csource (
  id TEXT NOT NULL,
  data JSONB NOT NULL,
  type TEXT,
  name TEXT,
  description TEXT,  
  timestamp_start TIMESTAMP,
  timestamp_end TIMESTAMP,  
  location GEOMETRY(Geometry, 4326), -- 4326 (WGS84) is the Coordinate System defined in GeoJson spec: https://tools.ietf.org/html/rfc7946#section-4
  expires TIMESTAMP,
  endpoint TEXT,
  internal boolean default false,
  has_registrationinfo_with_attrs_only BOOL NOT NULL DEFAULT FALSE,
  has_registrationinfo_with_entityinfo_only BOOL NOT NULL DEFAULT FALSE,
  PRIMARY KEY (id))
;

-- create indexes for performance
CREATE INDEX i_csource_data ON csource USING GIN (data); 
CREATE INDEX i_csource_name ON csource (name);
CREATE INDEX i_csource_timestamp_start ON csource (timestamp_start);
CREATE INDEX i_csource_timestamp_end ON csource (timestamp_end);
CREATE INDEX i_csource_location ON csource USING GIST (location);
CREATE INDEX i_csource_expires ON csource (expires);
CREATE INDEX i_csource_endpoint ON csource (endpoint);
CREATE INDEX i_csource_internal ON csource (internal);

CREATE TABLE IF NOT EXISTS csourceinformation (
  id BIGSERIAL,
  csource_id TEXT NOT NULL REFERENCES csource(id) ON DELETE CASCADE ON UPDATE CASCADE,
  group_id BIGINT,
  entity_id TEXT,
  entity_idpattern TEXT,
  entity_type TEXT,
  property_id TEXT,
  relationship_id TEXT,
  PRIMARY KEY (id))
;
CREATE SEQUENCE csourceinformation_group_id_seq OWNED BY csourceinformation.group_id; -- used by csource trigger
-- create indexes for performance
CREATE INDEX i_csourceinformation_csource_id ON csourceinformation (csource_id);
CREATE INDEX i_csourceinformation_entity_type_id_idpattern ON csourceinformation (entity_type, entity_id, entity_idpattern); 
CREATE INDEX i_csourceinformation_entity_type_id ON csourceinformation (entity_type, entity_id); 
CREATE INDEX i_csourceinformation_entity_type_idpattern ON csourceinformation (entity_type, entity_idpattern); 
CREATE INDEX i_csourceinformation_property_id ON csourceinformation (property_id); 
CREATE INDEX i_csourceinformation_relationship_id ON csourceinformation (relationship_id);
CREATE INDEX i_csourceinformation_group_property_relationship ON csourceinformation (group_id, property_id, relationship_id);

-- trigger to automatically extract pre-defined ngsi-ld members and store them in regular fields (for query performance)
CREATE OR REPLACE FUNCTION csource_extract_jsonb_fields() RETURNS trigger AS $_$
DECLARE
    l_rec RECORD;
    l_entityinfo_count INTEGER;
    l_attributeinfo_count INTEGER;
BEGIN
    NEW.type = NEW.data#>>'{@type,0}';
    NEW.name = NEW.data#>>'{https://uri.etsi.org/ngsi-ld/name,0,@value}';
    NEW.description = NEW.data#>>'{https://uri.etsi.org/ngsi-ld/description,0,@value}';
    NEW.timestamp_start = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/timestamp,0,https://uri.etsi.org/ngsi-ld/start,0,@value}')::TIMESTAMP;
    NEW.timestamp_end = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/timestamp,0,https://uri.etsi.org/ngsi-ld/end,0,@value}')::TIMESTAMP;
    NEW.location = ST_SetSRID(ST_GeomFromGeoJSON( NEW.data#>>'{https://uri.etsi.org/ngsi-ld/location,0,@value}' ), 4326);
    NEW.expires = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/expires,0,@value}')::TIMESTAMP;
    NEW.endpoint = NEW.data#>>'{https://uri.etsi.org/ngsi-ld/endpoint,0,@value}';
    NEW.internal = COALESCE((NEW.data#>>'{https://uri.etsi.org/ngsi-ld/internal,0,@value}')::BOOLEAN, FALSE);

    NEW.has_registrationinfo_with_attrs_only = false;
    NEW.has_registrationinfo_with_entityinfo_only = false;

    FOR l_rec IN SELECT value FROM jsonb_array_elements(NEW.data#>'{https://uri.etsi.org/ngsi-ld/information}')
    LOOP        
        SELECT INTO l_entityinfo_count count(*) FROM jsonb_array_elements( l_rec.value#>'{https://uri.etsi.org/ngsi-ld/entities}' );
        SELECT INTO l_attributeinfo_count count(*) FROM jsonb_array_elements( l_rec.value#>'{https://uri.etsi.org/ngsi-ld/properties}' );
        SELECT INTO l_attributeinfo_count count(*)+l_attributeinfo_count FROM jsonb_array_elements( l_rec.value#>'{https://uri.etsi.org/ngsi-ld/relationships}' );

        IF (NEW.has_registrationinfo_with_attrs_only = false) THEN
            NEW.has_registrationinfo_with_attrs_only = (l_entityinfo_count = 0 AND l_attributeinfo_count > 0);
        END IF;

        IF (NEW.has_registrationinfo_with_entityinfo_only = false) THEN
            NEW.has_registrationinfo_with_entityinfo_only = (l_entityinfo_count > 0 AND l_attributeinfo_count = 0);
        END IF;
    END LOOP;
    
    RETURN NEW;
END;
$_$ LANGUAGE plpgsql;

CREATE TRIGGER csource_extract_jsonb_fields BEFORE INSERT OR UPDATE ON csource
    FOR EACH ROW EXECUTE PROCEDURE csource_extract_jsonb_fields();

-- trigger to automatically extract pre-defined ngsi-ld members and store them in information table
CREATE OR REPLACE FUNCTION csource_extract_jsonb_fields_to_information_table() RETURNS trigger AS $_$
DECLARE
    l_rec RECORD;
    l_group_id csourceinformation.group_id%TYPE;
BEGIN    
    IF TG_OP = 'UPDATE' THEN
        DELETE FROM csourceinformation where csource_id = NEW.id;
    END IF;
    
    FOR l_rec IN SELECT value FROM jsonb_array_elements(NEW.data#>'{https://uri.etsi.org/ngsi-ld/information}')
    LOOP        
        -- RAISE NOTICE '%', rec.value;
        SELECT nextval('csourceinformation_group_id_seq') INTO l_group_id;

        -- id takes precedence over idPattern. so, only store idPattern if id is empty. this makes queries much easier/faster.
        INSERT INTO csourceinformation (csource_id, group_id, entity_id, entity_type, entity_idpattern) 
            SELECT NEW.id, 
                   l_group_id,
                   value#>>'{@id}', 
                   value#>>'{@type,0}', 
                   CASE WHEN value#>>'{@id}' IS NULL THEN value#>>'{https://uri.etsi.org/ngsi-ld/idPattern,0,@value}' END
                FROM jsonb_array_elements( l_rec.value#>'{https://uri.etsi.org/ngsi-ld/entities}');

        INSERT INTO csourceinformation (csource_id, group_id, property_id) 
            SELECT NEW.id, 
                   l_group_id,
                   value#>>'{@id}' 
                FROM jsonb_array_elements( l_rec.value#>'{https://uri.etsi.org/ngsi-ld/properties}');
        
        INSERT INTO csourceinformation (csource_id, group_id, relationship_id) 
            SELECT NEW.id, 
                   l_group_id,
                   value#>>'{@id}'
            FROM jsonb_array_elements( l_rec.value#>'{https://uri.etsi.org/ngsi-ld/relationships}');
        
    END LOOP;
    RETURN NEW;
END;
$_$ LANGUAGE plpgsql;

CREATE TRIGGER csource_extract_jsonb_fields_to_information_table AFTER INSERT OR UPDATE ON csource
    FOR EACH ROW EXECUTE PROCEDURE csource_extract_jsonb_fields_to_information_table();