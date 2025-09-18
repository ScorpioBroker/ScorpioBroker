CREATE OR REPLACE FUNCTION public.temporalentityattrinstance_extract_jsonb_fields()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
    DECLARE 
        f_internalid temporalentityattrinstance.internalid%TYPE;
    BEGIN
        IF TG_OP = 'INSERT' OR NEW.data <> OLD.data THEN -- do not reprocess if it is just an update on other column (e.g. static)
            NEW.instanceid = NEW.data#>>'{https://uri.etsi.org/ngsi-ld/instanceId,0,@id}';
            NEW.createdat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/createdAt,0,@value}')::TIMESTAMP;
            NEW.modifiedat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/modifiedAt,0,@value}')::TIMESTAMP;
            NEW.observedat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/observedAt,0,@value}')::TIMESTAMP;
            IF NEW.data#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/GeoProperty' THEN
                NEW.geovalue = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(NEW.data#>'{https://uri.etsi.org/ngsi-ld/hasValue,0}') ), 4326);
            ELSE 
                NEW.geovalue = NULL;
            END IF;
			IF NEW.location is NULL THEN
				SELECT teai.location INTO NEW.location FROM temporalentityattrinstance teai WHERE teai.internalid = new.internalid and COALESCE(teai.modifiedat, teai.observedat) <= COALESCE(NEW.modifiedat, NEW.observedat) ORDER BY COALESCE(teai.modifiedat, teai.observedat) LIMIT 1;
			END IF;
        END IF;

        RETURN NEW;
    END;
$function$