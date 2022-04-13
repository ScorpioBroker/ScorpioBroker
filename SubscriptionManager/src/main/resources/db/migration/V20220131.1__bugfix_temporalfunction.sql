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

            IF NEW.data#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/GeoProperty' THEN
                NEW.geovalue = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(NEW.data#>'{https://uri.etsi.org/ngsi-ld/hasValue,0}') ), 4326);
            ELSE 
                NEW.geovalue = NULL;
            END IF;
            IF (NEW.data ? 'https://uri.etsi.org/ngsi-ld/default-context/scope') THEN
        		UPDATE temporalentity SET scopes = getScopes(NEW.data#>'{https://uri.etsi.org/ngsi-ld/default-context/scope}') WHERE id = NEW.temporalentity_id;
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
