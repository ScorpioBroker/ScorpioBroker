CREATE OR REPLACE FUNCTION NGSILD_UPSERTBATCH(ENTITIES jsonb, DO_REPLACE boolean ) RETURNS jsonb AS $ENTITYOUSR$
declare
	resultObj jsonb;
	resultEntry jsonb;
	newentity jsonb;
	prev_entity jsonb;
	updated_entity jsonb;
	updated boolean;
BEGIN
	resultObj := '{"success": [], "failure": []}'::jsonb;
	FOR newentity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
		prev_entity := NULL;
		updated := FALSE;
		BEGIN
			IF newentity ? '@type' THEN
				INSERT INTO ENTITY(ID,E_TYPES, ENTITY) VALUES (newentity->>'@id',  ARRAY(SELECT jsonb_array_elements_text(newentity->'@type')), newentity) RETURNING ENTITY.ENTITY INTO updated_entity;
			ELSEIF DO_REPLACE THEN
				SELECT ENTITY FROM ENTITY WHERE ID=newentity->>'@id' INTO prev_entity;
				UPDATE ENTITY SET ENTITY = newentity ||jsonb_build_object('@type',(ENTITY->'@type')) WHERE id = newentity->>'@id' RETURNING ENTITY.ENTITY INTO updated_entity;
				updated := TRUE;
			ELSE
				SELECT ENTITY FROM ENTITY WHERE ID=newentity->>'@id' INTO prev_entity;
				UPDATE ENTITY SET ENTITY = ENTITY.ENTITY || newentity WHERE id = newentity->>'@id' RETURNING ENTITY.ENTITY INTO updated_entity;
				updated := TRUE;
			END IF;
			resultObj['success'] = resultObj['success'] || jsonb_build_object('id', (newentity->>'@id'), 'updated', updated, 'old', prev_entity, 'new', updated_entity);
		EXCEPTION WHEN unique_violation THEN
			SELECT ENTITY FROM ENTITY WHERE ID=newentity->>'@id' INTO prev_entity;
			IF DO_REPLACE THEN
				UPDATE ENTITY SET E_TYPES = ARRAY(SELECT DISTINCT UNNEST(ARRAY(SELECT jsonb_array_elements_text(newentity->'@type')))), ENTITY = newentity WHERE ID=newentity->>'@id' RETURNING ENTITY.entity INTO updated_entity;
			ELSE
				UPDATE ENTITY SET E_TYPES = ARRAY(SELECT DISTINCT UNNEST(e_types || ARRAY(SELECT jsonb_array_elements_text(newentity->'@type')))), ENTITY = ENTITY.entity || newentity || jsonb_set(newentity, '{@type}', array_to_json(ARRAY(SELECT DISTINCT UNNEST(e_types || ARRAY(SELECT jsonb_array_elements_text(newentity->'@type'))))) ::jsonb)
				WHERE ID=newentity->>'@id' RETURNING ENTITY.entity INTO updated_entity;
			END IF;

			updated := TRUE;
			resultObj['success'] = resultObj['success'] || jsonb_build_object('id', (newentity->>'@id'), 'updated', updated, 'old', prev_entity, 'new', updated_entity);
		WHEN OTHERS THEN
			resultObj['failure'] = resultObj['failure'] || jsonb_build_object(newentity->>'@id', SQLSTATE);
		END;
	END LOOP;
	RETURN resultObj;
END;
$ENTITYOUSR$ LANGUAGE PLPGSQL;