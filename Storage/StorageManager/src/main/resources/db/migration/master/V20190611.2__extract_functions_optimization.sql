CREATE OR REPLACE FUNCTION entity_extract_jsonb_fields() RETURNS trigger AS $_$
    BEGIN
        IF TG_OP = 'INSERT' OR NEW.data <> OLD.data THEN -- do not reprocess if it is just an update on another column
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
        END IF;
        
        RETURN NEW;
    END;
$_$ LANGUAGE plpgsql;




-- trigger to automatically extract pre-defined ngsi-ld members and store them in regular fields (for query performance)
CREATE OR REPLACE FUNCTION csource_extract_jsonb_fields() RETURNS trigger AS $_$
DECLARE
    l_rec RECORD;
    l_entityinfo_count INTEGER;
    l_attributeinfo_count INTEGER;
BEGIN
    IF TG_OP = 'INSERT' OR NEW.data <> OLD.data THEN -- do not reprocess if it is just an update on another column
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
    END IF;

    RETURN NEW;
END;
$_$ LANGUAGE plpgsql;



-- trigger to automatically extract pre-defined ngsi-ld members and store them in information table
CREATE OR REPLACE FUNCTION csource_extract_jsonb_fields_to_information_table() RETURNS trigger AS $_$
DECLARE
    l_rec RECORD;
    l_group_id csourceinformation.group_id%TYPE;
BEGIN    

    IF TG_OP = 'INSERT' OR NEW.data <> OLD.data THEN -- do not reprocess if it is just an update on another column

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

    END IF;

    RETURN NEW;
END;
$_$ LANGUAGE plpgsql;