CREATE OR REPLACE FUNCTION public.ngsild_deleteattrib(IN entity jsonb,IN attribname text,IN deletedatasetid text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
    
AS $BODY$
declare
	tmp jsonb;
	datasetId text;
	originalEntry jsonb;
BEGIN
	tmp := '[]'::jsonb;
	FOR originalEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITY->attribName) LOOP
		datasetId := originalEntry #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}';
		IF NOT ((deleteDatasetId is null and datasetId is null)or (deleteDatasetId is not null and datasetId is not null and deleteDatasetId = datasetId)) THEN
			tmp := tmp || originalEntry;
		END IF;
	END LOOP;
	IF jsonb_array_length(tmp) > 0 THEN
		ENTITY := jsonb_set(Entity,Array['https://uri.etsi.org/ngsi-ld/modifiedAt','0'],jsonb_build_object('@type', 'https://uri.etsi.org/ngsi-ld/DateTime','@value', to_char(timezone('utc', now()), 'YYYY-MM-DD"T"HH24:MI:SS') || 'Z'));
		ENTITY := jsonb_set(ENTITY,ARRAY[attribname]::text[], tmp);
	ELSE
		ENTITY := jsonb_set(Entity,Array['https://uri.etsi.org/ngsi-ld/modifiedAt','0'],jsonb_build_object('@type', 'https://uri.etsi.org/ngsi-ld/DateTime','@value', to_char(timezone('utc', now()), 'YYYY-MM-DD"T"HH24:MI:SS') || 'Z'));
		ENTITY := ENTITY - attribName;
	END IF;
	return ENTITY;
END;
$BODY$;