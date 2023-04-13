CREATE OR REPLACE FUNCTION NGSILD_CREATEBATCH(ENTITIES jsonb) RETURNS jsonb AS $ENTITYOCR$
declare
	resultObj jsonb;
	entity jsonb;
BEGIN
	resultObj := '{"success": [], "failure": []}'::jsonb;
	FOR entity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
		BEGIN
			INSERT INTO ENTITY(ID,E_TYPES, ENTITY) VALUES (entity->>'@id',  ARRAY(SELECT jsonb_array_elements(entity->'@type')), entity);
			RAISE NOTICE 'result obj before %', resultObj;
			resultObj['success'] = resultObj['success'] || (entity->'@id')::jsonb;
			RAISE NOTICE 'result obj after %', resultObj;
		EXCEPTION 
		WHEN OTHERS THEN
			RAISE NOTICE '%, %', SQLSTATE, SQLERRM;
			resultObj['failure'] = resultObj['failure'] || jsonb_object_agg(entity->>'@id', SQLSTATE);
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
			if NOT FOUND THEN
			    resultObj['failure'] = resultObj['failure'] || jsonb_object_agg(entityId, 'Not Found');
            else
			resultObj['success'] = resultObj['success'] || jsonb_agg(entityId);
			End IF;
		EXCEPTION WHEN OTHERS THEN
			resultObj['failure'] = resultObj['failure'] || jsonb_object_agg(entityId, SQLSTATE);
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
BEGIN
	resultObj := '{"success": [], "failure": []}'::jsonb;
	FOR newentity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
		BEGIN
			IF newentity ? '@type' THEN
				UPDATE ENTITY SET E_TYPES = ARRAY(SELECT jsonb_array_elements(newentity->'@type')), ENTITY = ENTITY.ENTITY || newentity WHERE id = newentity->>'@id';
			ELSE
				UPDATE ENTITY SET ENTITY = ENTITY.ENTITY || newentity WHERE id = newentity->>'@id';
			END IF;
			if NOT FOUND THEN resultObj['failure'] = resultObj['failure'] || jsonb_object_agg(newentity->>'@id', 'Not Found');
			else resultObj['success'] = resultObj['success'] || (newentity->'@id')::jsonb;
			END IF;
		EXCEPTION WHEN OTHERS THEN
			resultObj['failure'] = resultObj['failure'] || jsonb_object_agg(newentity->>'@id', SQLSTATE);
		END;
	END LOOP;
	RETURN resultObj;
END;
$ENTITYOAR$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION NGSILD_UPSERTBATCH(ENTITIES jsonb) RETURNS jsonb AS $ENTITYOUSR$
declare
	resultObj jsonb;
	resultEntry jsonb;
	newentity jsonb;
BEGIN
	resultObj := '{"success": [], "failure": []}'::jsonb;
	FOR newentity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
		BEGIN
			IF newentity ? '@type' THEN
				INSERT INTO ENTITY(ID,E_TYPES, ENTITY) VALUES (newentity->>'@id',  ARRAY(SELECT jsonb_array_elements(newentity->'@type')), newentity) ON CONFLICT(ID) DO UPDATE SET E_TYPES = ARRAY(SELECT jsonb_array_elements(newentity->'@type')), ENTITY = ENTITY.entity || newentity;
			ELSE
				UPDATE ENTITY SET ENTITY = ENTITY.ENTITY || newentity WHERE id = newentity->>'@id';
			END IF;
			resultObj['success'] = resultObj['success'] || (newentity->'@id')::jsonb;
		EXCEPTION WHEN OTHERS THEN
			resultObj['failure'] = resultObj['failure'] || jsonb_object_agg(newentity->>'@id', SQLSTATE);
		END;
	END LOOP;
	RETURN resultObj;
END;
$ENTITYOUSR$ LANGUAGE PLPGSQL;
