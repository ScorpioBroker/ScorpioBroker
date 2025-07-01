ALTER TABLE public.entity
    ADD COLUMN scopes text[] COLLATE pg_catalog."default";
CREATE OR REPLACE FUNCTION getScopes (scopeList jsonb)
RETURNS text[] AS $scopes$
declare
	scopes text[];
	i jsonb;
BEGIN
     FOR i IN SELECT jsonb_array_elements FROM jsonb_array_elements(scopeList)
		LOOP
		   SELECT array_append(scopes,'{-1}',  (i#>'{@value}')) into scopes;
		END LOOP;
   RETURN scopes;
END;
$scopes$ LANGUAGE plpgsql;
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
          IF (NEW.data ? 'https://uri.etsi.org/ngsi-ld/default-context/scope') THEN
              NEW.scopes = getScopes(NEW.data#>'{https://uri.etsi.org/ngsi-ld/default-context/scope}');
          ELSE 
              NEW.scopes = NULL;
          END IF;
        END IF;
        
        RETURN NEW;
    END;
$_$ LANGUAGE plpgsql;
