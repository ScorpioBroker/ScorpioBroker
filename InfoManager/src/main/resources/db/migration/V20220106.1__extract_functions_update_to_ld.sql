CREATE OR REPLACE FUNCTION getCoordinates (coordinateList jsonb)
RETURNS jsonb AS $coordinates$
declare
	coordinates jsonb := '[]';
	i jsonb;
BEGIN
     FOR i IN SELECT jsonb_array_elements FROM jsonb_array_elements(coordinateList)
		LOOP
		   IF i ? '@list' THEN
		     SELECT jsonb_insert(coordinates, '{-1}', getCoordinates(i#>'{@list}')) into coordinates;
		   ELSE
			 SELECT jsonb_insert(coordinates,'{-1}',  (i#>'{@value}')) into coordinates;
		   END IF;
		END LOOP;
   RETURN coordinates;
END;
$coordinates$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getGeoJson (ldjson jsonb)
RETURNS jsonb AS $geojson$
declare
	geojson jsonb;
BEGIN
  SELECT json_build_object('type', substring(ldjson#>>'{@type,0}' from 32),'coordinates',getCoordinates(ldjson#>'{https://purl.org/geojson/vocab#coordinates,0,@list}')) into geojson;
  RETURN geojson;
END;
$geojson$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION entity_extract_jsonb_fields() RETURNS trigger AS $_$
    BEGIN
	    -- do not reprocess if it is just an update on another column
        IF (TG_OP = 'INSERT' AND NEW.data IS NOT NULL) OR 
        	(TG_OP = 'UPDATE' AND OLD.data IS NULL AND NEW.data IS NOT NULL) OR 
            (TG_OP = 'UPDATE' AND OLD.data IS NOT NULL AND NEW.data IS NULL) OR 
			(TG_OP = 'UPDATE' AND OLD.data IS NOT NULL AND NEW.data IS NOT NULL AND OLD.data <> NEW.data) THEN 
          -- is any validation needed?
          NEW.type = NEW.data#>>'{@type,0}';
          NEW.createdat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/createdAt,0,@value}')::TIMESTAMP;
          NEW.modifiedat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/modifiedAt,0,@value}')::TIMESTAMP;

          IF (NEW.data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
              NEW.location = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson( NEW.data#>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0}') ), 4326);
          ELSE 
              NEW.location = NULL;
          END IF;
          IF (NEW.data@>'{"https://uri.etsi.org/ngsi-ld/observationSpace": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] }] }') THEN
              NEW.observationSpace = ST_SetSRID( ST_GeomFromGeoJSON( getGeoJson( NEW.data#>'{https://uri.etsi.org/ngsi-ld/observationSpace,0,https://uri.etsi.org/ngsi-ld/hasValue,0}') ), 4326);
          ELSE 
              NEW.observationSpace = NULL;
          END IF;
          IF (NEW.data@>'{"https://uri.etsi.org/ngsi-ld/operationSpace": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
              NEW.operationSpace = ST_SetSRID( ST_GeomFromGeoJSON( getGeoJson(NEW.data#>'{https://uri.etsi.org/ngsi-ld/operationSpace,0,https://uri.etsi.org/ngsi-ld/hasValue,0}') ), 4326);
          ELSE 
              NEW.operationSpace = NULL;
          END IF;
        END IF;
        
        RETURN NEW;
    END;
$_$ LANGUAGE plpgsql;

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
                NEW.geovalue = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(NEW.data#>'{https://uri.etsi.org/ngsi-ld/hasValue,0}') ), 4326);
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



-- trigger to automatically extract pre-defined ngsi-ld members and store them in regular fields (for query performance)
CREATE OR REPLACE FUNCTION csource_extract_jsonb_fields() RETURNS trigger AS $_$
DECLARE
    l_rec RECORD;
    l_entityinfo_count INTEGER;
    l_attributeinfo_count INTEGER;
BEGIN
    IF (TG_OP = 'INSERT' AND NEW.data IS NOT NULL) OR 
        (TG_OP = 'UPDATE' AND OLD.data IS NULL AND NEW.data IS NOT NULL) OR 
        (TG_OP = 'UPDATE' AND OLD.data IS NOT NULL AND NEW.data IS NULL) OR 
        (TG_OP = 'UPDATE' AND OLD.data IS NOT NULL AND NEW.data IS NOT NULL AND OLD.data <> NEW.data) THEN 
      NEW.type = NEW.data#>>'{@type,0}';
      NEW.tenant_id = NEW.data#>>'{https://uri.etsi.org/ngsi-ld/default-context/tenant,0,@value}';
      NEW.name = NEW.data#>>'{https://uri.etsi.org/ngsi-ld/name,0,@value}';
      NEW.description = NEW.data#>>'{https://uri.etsi.org/ngsi-ld/description,0,@value}';
      NEW.timestamp_start = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/timestamp,0,https://uri.etsi.org/ngsi-ld/start,0,@value}')::TIMESTAMP;
      NEW.timestamp_end = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/timestamp,0,https://uri.etsi.org/ngsi-ld/end,0,@value}')::TIMESTAMP;
      IF NEW.data ? 'https://uri.etsi.org/ngsi-ld/location' THEN
        IF (NEW.data@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
          NEW.location = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson( NEW.data#>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0}') ), 4326);
        ELSE
          NEW.location = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(NEW.data#>'{https://uri.etsi.org/ngsi-ld/location,0}') ), 4326);
        END IF;
      ELSE
        NEW.location = null;
      END IF;
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
