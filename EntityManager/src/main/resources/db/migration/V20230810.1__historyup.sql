ALTER TABLE IF EXISTS public.temporalentityattrinstance
    ADD COLUMN IF NOT EXISTS location geometry;
CREATE INDEX IF NOT EXISTS i_temporalentityattrinstance_location
    ON public.temporalentityattrinstance USING gist
    (location)
    WITH (buffering=auto)
;
CREATE INDEX IF NOT EXISTS i_temporalentityattrinstance_entityid
    ON public.temporalentityattrinstance USING hash
    (temporalentity_id)
;
with x as (SELECT distinct temporalentity_id as eid, geovalue, modifiedat as mat, observedat as oat, COALESCE(modifiedat, observedat) FROM temporalentityattrinstance WHERE geovalue is not null ORDER BY COALESCE(modifiedat, observedat)) UPDATE temporalentityattrinstance SET location = (SELECT x.geovalue FROM x WHERE eid = temporalentity_id and COALESCE(x.mat, x.oat) <= COALESCE(modifiedat, observedat) ORDER BY COALESCE(modifiedat, observedat) DESC limit 1) WHERE location is not null;

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
				SELECT teai.location INTO NEW.location FROM temporalentityattrinstance teai WHERE COALESCE(teai.modifiedat, teai.observedat) <= COALESCE(NEW.modifiedat, NEW.observedat) ORDER BY COALESCE(teai.modifiedat, teai.observedat) LIMIT 1;
			END IF;
        END IF;

        RETURN NEW;
    END;
$function$

