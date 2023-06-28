CREATE OR REPLACE FUNCTION NGSILD_UPSERTBATCH(ENTITIES jsonb) RETURNS jsonb AS $ENTITYOUSR$
declare
	resultObj jsonb;
	resultEntry jsonb;
	newentity jsonb;
	updated boolean;
BEGIN
	resultObj := '{"success": [], "failure": []}'::jsonb;
	FOR newentity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
		updated := FALSE;
		BEGIN
			IF newentity ? '@type' THEN
				INSERT INTO ENTITY(ID,E_TYPES, ENTITY) VALUES (newentity->>'@id',  ARRAY(SELECT jsonb_array_elements_text(newentity->'@type')), newentity);
			ELSE
				UPDATE ENTITY SET ENTITY = ENTITY.ENTITY || newentity WHERE id = newentity->>'@id';
				updated := TRUE;
			END IF;
			resultObj['success'] = resultObj['success'] || jsonb_build_object((newentity->>'@id'), updated);
		EXCEPTION WHEN unique_violation THEN
			UPDATE ENTITY SET E_TYPES = ARRAY(SELECT DISTINCT UNNEST(e_types || ARRAY(SELECT jsonb_array_elements_text(newentity->'@type')))), ENTITY = ENTITY.entity || newentity WHERE ID=newentity->>'@id';
			updated := TRUE;
			resultObj['success'] = resultObj['success'] || jsonb_build_object((newentity->>'@id'), updated);
		WHEN OTHERS THEN
			resultObj['failure'] = resultObj['failure'] || jsonb_object_agg(newentity->>'@id', SQLSTATE);
		END;
	END LOOP;
	RETURN resultObj;
END;
$ENTITYOUSR$ LANGUAGE PLPGSQL;