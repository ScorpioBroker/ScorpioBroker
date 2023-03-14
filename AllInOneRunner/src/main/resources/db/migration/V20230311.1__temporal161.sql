ALTER TABLE PUBLIC.temporalentity ADD COLUMN E_TYPES TEXT[];
CREATE INDEX "I_temporalentity_types"
    ON public.entity USING gin
    (e_types array_ops);
UPDATE temporalentity SET E_TYPES=array_append(E_TYPES,TYPE);
ALTER TABLE PUBLIC.temporalentity DROP COLUMN type;
ALTER TABLE PUBLIC.temporalentity ADD COLUMN DELETEDAT TIMESTAMP WITHOUT TIME ZONE;
ALTER TABLE PUBLIC.temporalentityattrinstance ADD COLUMN DELETEDAT TIMESTAMP WITHOUT TIME ZONE;
ALTER TABLE PUBLIC.temporalentityattrinstance DROP COLUMN static;
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

        RETURN NEW;
    END;
$_$ LANGUAGE plpgsql;