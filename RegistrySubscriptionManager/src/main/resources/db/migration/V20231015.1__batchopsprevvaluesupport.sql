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
				UPDATE ENTITY SET E_TYPES = ARRAY(SELECT DISTINCT UNNEST(e_types || ARRAY(SELECT jsonb_array_elements_text(newentity->'@type')))), ENTITY = ENTITY.entity || newentity WHERE ID=newentity->>'@id' RETURNING ENTITY.entity INTO updated_entity;
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


CREATE OR REPLACE FUNCTION NGSILD_DELETEBATCH(ENTITY_IDS jsonb) RETURNS jsonb AS $ENTITYODR$
declare
	resultObj jsonb;
	prev_entity jsonb;
	entityId text;
BEGIN
	resultObj := '{"success": [], "failure": []}'::jsonb;
	FOR entityId IN SELECT jsonb_array_elements_text FROM jsonb_array_elements_text(ENTITY_IDS) LOOP
		BEGIN
			DELETE FROM ENTITY WHERE ID = entityId RETURNING ENTITY.ENTITY INTO prev_entity;
			if NOT FOUND THEN
			    resultObj['failure'] = resultObj['failure'] || jsonb_build_object(entityId, 'Not Found');
            else
				resultObj['success'] = resultObj['success'] || jsonb_build_object('id', entityId, 'old', prev_entity);
			End IF;
		EXCEPTION WHEN OTHERS THEN
			resultObj['failure'] = resultObj['failure'] || jsonb_build_object(entityId, SQLSTATE);
		END;
	END LOOP;
	RETURN resultObj;
END;
$ENTITYODR$ LANGUAGE PLPGSQL PARALLEL SAFE;

CREATE OR REPLACE FUNCTION NGSILD_APPENDBATCH(ENTITIES jsonb) RETURNS jsonb AS $ENTITYOAR$
declare
	resultObj jsonb;
	resultEntry jsonb;
	newentity jsonb;
	prev_entity jsonb;
	updated_entity jsonb;
BEGIN
	resultObj := '{"success": [], "failure": []}'::jsonb;
	FOR newentity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
		prev_entity := NULL;
		BEGIN
			SELECT ENTITY FROM ENTITY WHERE ID=newentity->>'@id' INTO prev_entity;
			IF newentity ? '@type' THEN
				UPDATE ENTITY SET E_TYPES = ARRAY(SELECT jsonb_array_elements_text(newentity->'@type')), ENTITY = ENTITY.ENTITY || newentity WHERE id = newentity->>'@id' RETURNING ENTITY.ENTITY INTO updated_entity;
			ELSE
				UPDATE ENTITY SET ENTITY = ENTITY.ENTITY || newentity WHERE id = newentity->>'@id' RETURNING ENTITY.ENTITY INTO updated_entity;
			END IF;
			if NOT FOUND THEN resultObj['failure'] = resultObj['failure'] || jsonb_build_object(newentity->>'@id', 'Not Found');
			else resultObj['success'] = resultObj['success'] || jsonb_build_object('id', newentity->'@id', 'old', prev_entity, 'new', updated_entity)::jsonb;
			END IF;
		EXCEPTION WHEN OTHERS THEN
			resultObj['failure'] = resultObj['failure'] || jsonb_build_object(newentity->>'@id', SQLSTATE);
		END;
	END LOOP;
	RETURN resultObj;
END;
$ENTITYOAR$ LANGUAGE PLPGSQL;
