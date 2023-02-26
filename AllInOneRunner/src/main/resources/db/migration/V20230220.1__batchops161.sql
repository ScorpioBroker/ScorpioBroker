CREATE OR REPLACE FUNCTION NGSILD_CREATEBATCH(ENTITIES jsonb) RETURNS jsonb AS $ENTITYOCR$
declare
	resultObj jsonb;
	entity jsonb;
BEGIN
	resultObj := '{"success": [], "failure": []}'::jsonb;
	FOR entity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
		BEGIN
			INSERT INTO ENTITY(ID,E_TYPES, ENTITY) VALUES (entity->>'@id',  ARRAY(SELECT json_array_elements(entity->'@type')), entity);
			resultObject['success'] = resultObject['success'] || entity->>'@id'
		EXCEPTION WHEN OTHERS THEN
			resultObject['failure'] = resultObject['failure'] || jsonb_obj_agg(entity->>'@id', SQLSTATE);
		END;
	END LOOP;
	RETURN resultObj;
END;
$ENTITYOCR$ LANGUAGE PLPGSQL PARALLEL SAFE;

CREATE OR REPLACE FUNCTION NGSILD_DELETEBATCH(ENTITY_IDS jsonb) RETURNS jsonb AS $ENTITYODR$
declare
	resultObj jsonb;
	entityId text;
BEGIN
	resultObj := '{"success": [], "failure": []}'::jsonb;
	FOR entityId IN SELECT jsonb_array_elements_text FROM jsonb_array_elements_text(ENTITY_IDS) LOOP
		BEGIN
			DELETE FROM ENTITY WHERE ID = entityId;
			resultObject['success'] = resultObject['success'] || entityId
		EXCEPTION WHEN OTHERS THEN
			resultObject['failure'] = resultObject['failure'] || jsonb_obj_agg(entityId, SQLSTATE);
		END;
	END LOOP;
	RETURN resultObj;
END;
$ENTITYODR$ LANGUAGE PLPGSQL PARALLEL SAFE;

CREATE OR REPLACE FUNCTION NGSILD_APPENDBATCH(ENTITIES jsonb) RETURNS jsonb AS $ENTITYOAR$
declare
	resultObj jsonb;
	resultEntry jsonb;
	entity jsonb;
BEGIN
	resultObj := '{"success": [], "failure": []}'::jsonb;
	FOR entity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
		BEGIN
			IF entity ? '@type' THEN
				UPDATE ENTITY SET ENTITY.ENTITY->'@type' = ENTITY.ENTITY->'@type' || entity->'@type', ENTITY.E_TYPES = ARRAY(SELECT json_array_elements(ENTITY.ENTITY->'@type')), ENTITY.ENTITY = ENTITY.ENTITY || (entity - '@type') WHERE id = entity->>'@id';
			ELSE
				UPDATE ENTITY SET ENTITY.ENTITY = ENTITY.ENTITY || entity WHERE id = entity->>'@id';
			END IF;
			resultObject['success'] = resultObject['success'] || entity->>'@id'
		EXCEPTION WHEN OTHERS THEN
			resultObject['failure'] = resultObject['failure'] || jsonb_obj_agg(entity->>'@id', SQLSTATE);
		END;
	END LOOP;
	RETURN resultObj;
END;
$ENTITYOAR$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION NGSILD_UPSERTBATCH(ENTITIES jsonb) RETURNS jsonb AS $ENTITYOUSR$
declare
	resultObj jsonb;
	resultEntry jsonb;
	entity jsonb;
BEGIN
	resultObj := '{"success": [], "failure": []}'::jsonb;
	FOR entity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
		BEGIN
			IF entity ? '@type' THEN
				INSERT INTO ENTITY(ID,E_TYPES, ENTITY) VALUES (entity->>'@id',  ARRAY(SELECT json_array_elements(entity->'@type')), entity) ON CONFLICT DO UPDATE SET ENTITY.ENTITY->'@type' = ENTITY.ENTITY->'@type' || entity->'@type', ENTITY.E_TYPES = ARRAY(SELECT json_array_elements(ENTITY.ENTITY->'@type')), ENTITY.ENTITY = ENTITY.ENTITY || (entity - '@type');
			ELSE
				UPDATE ENTITY SET ENTITY.ENTITY = ENTITY.ENTITY || entity WHERE id = entity->>'@id';
			END IF;
			resultObject['success'] = resultObject['success'] || entity->>'@id'
		EXCEPTION WHEN OTHERS THEN
			resultObject['failure'] = resultObject['failure'] || jsonb_obj_agg(entity->>'@id', SQLSTATE);
		END;
	END LOOP;
	RETURN resultObj;
END;
$ENTITYOUSR$ LANGUAGE PLPGSQL;
