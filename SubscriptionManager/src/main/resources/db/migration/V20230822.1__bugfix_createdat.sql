CREATE OR REPLACE FUNCTION NGSILD_PARTIALUPDATE(ENTITY jsonb, attribName text, attribValues jsonb) RETURNS jsonb AS $ENTITYPU$
declare
	tmp jsonb;
	datasetId text;
	insertDatasetId text;
	originalEntry jsonb;
	insertEntry jsonb;
	inUpdate boolean;
BEGIN
	tmp := '[]'::jsonb;
	FOR originalEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITY->attribName) LOOP
		inUpdate := False;
		datasetId := originalEntry #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}';
		FOR insertEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(attribValues) LOOP
			insertDatasetId := insertEntry #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}';
			IF (insertDatasetId is null and datasetId is null)or (insertDatasetId is not null and datasetId is not null and insertDatasetId = datasetId) THEN
				inUpdate = true;
				EXIT;
			END IF;
		END LOOP;
		IF NOT inUpdate THEN
			tmp := tmp || originalEntry;
		END IF;
	END LOOP;
	tmp := tmp || attribValues;
	IF not attribValues ? 'https://uri.etsi.org/ngsi-ld/modifiedAt' THEN
	Entity := jsonb_set(Entity,Array['https://uri.etsi.org/ngsi-ld/modifiedAt','0'],jsonb_build_object('@type', 'https://uri.etsi.org/ngsi-ld/DateTime','@value', to_char(timezone('utc', now()), 'YYYY-MM-DD"T"HH24:MI:SS') || 'Z'));
	tmp := jsonb_set(tmp,Array['0','https://uri.etsi.org/ngsi-ld/modifiedAt'], Entity->'https://uri.etsi.org/ngsi-ld/modifiedAt',true);
	END IF;
	RETURN jsonb_set(Entity,Array[attribName,'0'], (Entity->attribName->0) || (tmp->0),true);
END;
$ENTITYPU$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION NGSILD_DELETEATTRIB(ENTITY jsonb, attribName text, deleteDatasetId text) RETURNS jsonb AS $ENTITYPD$
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
	Entity := jsonb_set(Entity,Array['https://uri.etsi.org/ngsi-ld/modifiedAt','0'],jsonb_build_object('@type', 'https://uri.etsi.org/ngsi-ld/DateTime','@value', to_char(timezone('utc', now()), 'YYYY-MM-DD"T"HH24:MI:SS') || 'Z'));
		RETURN jsonb_set(ENTITY,'{attribName}', tmp);
	ELSE
	Entity := jsonb_set(Entity,Array['https://uri.etsi.org/ngsi-ld/modifiedAt','0'],jsonb_build_object('@type', 'https://uri.etsi.org/ngsi-ld/DateTime','@value', to_char(timezone('utc', now()), 'YYYY-MM-DD"T"HH24:MI:SS') || 'Z'));
		RETURN ENTITY - attribName;
	END IF;
END;
$ENTITYPD$ LANGUAGE PLPGSQL;