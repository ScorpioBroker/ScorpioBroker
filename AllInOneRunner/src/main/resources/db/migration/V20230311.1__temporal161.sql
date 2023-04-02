ALTER TABLE PUBLIC.temporalentity ADD COLUMN E_TYPES TEXT[];
ALTER TABLE PUBLIC.temporalentityattrinstance DROP COLUMN VALUE;
ALTER TABLE PUBLIC.temporalentityattrinstance DROP COLUMN attributetype;
CREATE INDEX "I_temporalentity_types"
    ON public.temporalentity USING gin
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
            NEW.instanceid = NEW.data#>>'{https://uri.etsi.org/ngsi-ld/instanceId,0,@id}';
            NEW.createdat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/createdAt,0,@value}')::TIMESTAMP;
            NEW.modifiedat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/modifiedAt,0,@value}')::TIMESTAMP;
            NEW.observedat = (NEW.data#>>'{https://uri.etsi.org/ngsi-ld/observedAt,0,@value}')::TIMESTAMP;
            IF NEW.data#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/GeoProperty' THEN
                NEW.geovalue = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(NEW.data#>'{https://uri.etsi.org/ngsi-ld/hasValue,0}') ), 4326);
            ELSE 
                NEW.geovalue = NULL;
            END IF;
        END IF;

        RETURN NEW;
    END;
$_$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getScopeEntry (scopeList text[])
RETURNS jsonb AS $scopes$
declare
	scopes jsonb;
	i text;
BEGIN
	scopes := '[]'::jsonb;
    FOREACH i IN ARRAY scopeList LOOP
		scopes = scopes || jsonb_build_object('@value', i);
	END LOOP;
	RETURN scopes;
END;
$scopes$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getScopes (scopeList jsonb)
RETURNS text[] AS $scopes$
declare
	scopes text[];
	i jsonb;
BEGIN
	if scopeList is null THEN
		RETURN null;
	END IF;
	FOR i IN SELECT jsonb_array_elements FROM jsonb_array_elements(scopeList) LOOP
		SELECT array_append(scopes, (i#>>'{@value}')::text) into scopes;
	END LOOP;
	RETURN scopes;
END;
$scopes$ LANGUAGE plpgsql;

CREATE INDEX i_temporalentityattrinstance_attribname
    ON public.temporalentityattrinstance USING hash
    (attributeid text_ops);
CREATE INDEX i_temporalentity_location ON public.temporalentityattrinstance USING GIST (geovalue);