
DROP TABLE IF EXISTS public.entitymap;
DROP TABLE IF EXISTS public.entitymap_management;
DROP FUNCTION IF EXISTS ngsild_appendbatch(jsonb);
DROP FUNCTION IF EXISTS ngsild_upsertbatch(jsonb);

CREATE OR REPLACE FUNCTION public.ngsild_deletebatch(IN entity_ids jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL UNSAFE
    COST 100
    
AS $BODY$
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
				resultObj = jsonb_set(resultObj, '{failure}', resultObj -> 'failure' || jsonb_build_object(entityId, 'Not Found'));
            else
				resultObj = jsonb_set(resultObj, '{success}', resultObj -> 'success' || jsonb_build_object('id', entityId, 'old', prev_entity));
			End IF;
		EXCEPTION WHEN OTHERS THEN
			resultObj = jsonb_set(resultObj, '{failure}', resultObj -> 'failure' || jsonb_build_object(entityId, SQLSTATE));
		END;
	END LOOP;
	RETURN resultObj;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.ngsild_createbatch(IN entities jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL UNSAFE
    COST 100
    
AS $BODY$
declare
	resultObj jsonb;
	entity jsonb;
BEGIN
	resultObj := '{"success": [], "failure": []}'::jsonb;
	FOR entity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
		BEGIN
			INSERT INTO ENTITY(ID,E_TYPES, ENTITY) VALUES (entity->>'@id',  ARRAY(SELECT jsonb_array_elements_text(entity->'@type')), entity);
			RAISE NOTICE 'result obj before %', resultObj;
			resultObj = jsonb_set(resultObj, '{success}', resultObj -> 'success' || (entity->'@id')::jsonb);
			RAISE NOTICE 'result obj after %', resultObj;
		EXCEPTION 
		WHEN OTHERS THEN
			RAISE NOTICE '%, %', SQLSTATE, SQLERRM;
			resultObj = jsonb_set(resultObj, '{failure}', resultObj -> 'failure' || jsonb_object_agg(entity->>'@id', SQLSTATE));
		END;
	END LOOP;
	RETURN resultObj;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.ngsild_appendbatch(IN entities jsonb,IN nooverwrite boolean)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL UNSAFE
    COST 100
    
AS $BODY$
DECLARE
    resultObj jsonb;
    resultEntry jsonb;
    newentity jsonb;
    prev_entity jsonb;
    updated_entity jsonb;
    not_overwriting boolean;
    to_update jsonb;
    to_append jsonb;
BEGIN
    resultObj := '{"success": [], "failure": []}'::jsonb;

    FOR newentity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
        prev_entity := NULL;

        BEGIN
            SELECT ENTITY FROM ENTITY WHERE ID = newentity->>'@id' INTO prev_entity;

            SELECT
                jsonb_object_agg(key, Array[(value->0) || jsonb_build_object('https://uri.etsi.org/ngsi-ld/createdAt', prev_entity->key->0->'https://uri.etsi.org/ngsi-ld/createdAt')])::jsonb
                || jsonb_build_object('https://uri.etsi.org/ngsi-ld/modifiedAt', newentity->'https://uri.etsi.org/ngsi-ld/modifiedAt')
            FROM jsonb_each((newentity - '@id' - '@type'))
            WHERE key IN (SELECT jsonb_object_keys(prev_entity))
            INTO to_update;

            IF NOOVERWRITE THEN
                SELECT jsonb_object_agg(key, value)::jsonb
                FROM jsonb_each(newentity)
                WHERE key NOT IN (SELECT jsonb_object_keys(prev_entity))
                INTO to_append;

                IF to_append IS NOT NULL THEN
                    UPDATE ENTITY
                    SET ENTITY = ENTITY || to_append || jsonb_build_object('https://uri.etsi.org/ngsi-ld/modifiedAt', newentity->'https://uri.etsi.org/ngsi-ld/modifiedAt')
                    WHERE ID = newentity->>'@id'
                    RETURNING ENTITY.ENTITY INTO updated_entity;
                ELSE
                    not_overwriting := true;
                END IF;
            ELSIF newentity ? '@type' THEN
                UPDATE ENTITY
                SET E_TYPES = ARRAY(SELECT jsonb_array_elements_text(newentity->'@type')),
                    ENTITY = ENTITY.ENTITY || to_update || CASE WHEN to_append IS NOT NULL THEN to_append ELSE '{}' END
                WHERE ID = newentity->>'@id'
                RETURNING ENTITY.ENTITY INTO updated_entity;
            ELSE
                UPDATE ENTITY
                SET ENTITY = ENTITY.ENTITY || to_update || CASE WHEN to_append IS NOT NULL THEN to_append ELSE '{}' END
                WHERE ID = newentity->>'@id'
                RETURNING ENTITY.ENTITY INTO updated_entity;
            END IF;

            IF not_overwriting THEN
				resultObj = jsonb_set(resultObj, '{failure}', resultObj -> 'failure' || jsonb_build_object(newentity->>'@id', 'Not Overwriting'));
            ELSIF NOT FOUND THEN
				resultObj = jsonb_set(resultObj, '{failure}', resultObj -> 'failure' || jsonb_build_object(newentity->>'@id', 'Not Found'));
            ELSE
				resultObj = jsonb_set(resultObj, '{success}', resultObj -> 'success' || jsonb_build_object('id', newentity->'@id', 'old', prev_entity, 'new', updated_entity)::jsonb);
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE '%', SQLERRM;
				resultObj = jsonb_set(resultObj, '{failure}', resultObj -> 'failure' || jsonb_build_object(newentity->>'@id', SQLSTATE));
        END;
    END LOOP;

    RETURN resultObj;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.ngsild_upsertbatch(IN entities jsonb,IN do_replace boolean)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL UNSAFE
    COST 100
    
AS $BODY$
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
			resultObj = jsonb_set(resultObj, '{success}', resultObj -> 'success' || jsonb_build_object('id', (newentity->>'@id'), 'updated', updated, 'old', prev_entity, 'new', updated_entity));
		EXCEPTION WHEN unique_violation THEN
			SELECT ENTITY FROM ENTITY WHERE ID=newentity->>'@id' INTO prev_entity;
			IF DO_REPLACE THEN
				UPDATE ENTITY SET E_TYPES = ARRAY(SELECT DISTINCT UNNEST(ARRAY(SELECT jsonb_array_elements_text(newentity->'@type')))), ENTITY = newentity WHERE ID=newentity->>'@id' RETURNING ENTITY.entity INTO updated_entity;
			ELSE
				UPDATE ENTITY SET E_TYPES = ARRAY(SELECT DISTINCT UNNEST(e_types || ARRAY(SELECT jsonb_array_elements_text(newentity->'@type')))), ENTITY = ENTITY.entity || newentity || jsonb_set(newentity, '{@type}', array_to_json(ARRAY(SELECT DISTINCT UNNEST(e_types || ARRAY(SELECT jsonb_array_elements_text(newentity->'@type'))))) ::jsonb)
				WHERE ID=newentity->>'@id' RETURNING ENTITY.entity INTO updated_entity;
			END IF;

			updated := TRUE;
			resultObj = jsonb_set(resultObj, '{success}', resultObj -> 'success' || jsonb_build_object('id', (newentity->>'@id'), 'updated', updated, 'old', prev_entity, 'new', updated_entity));
		WHEN OTHERS THEN
			resultObj = jsonb_set(resultObj, '{failure}', resultObj -> 'failure' || jsonb_build_object(newentity->>'@id', SQLSTATE));
		END;
	END LOOP;
	RETURN resultObj;
END;
$BODY$;

CREATE TABLE public.entitymap
(
		id text,
		expires_at timestamp without time zone,
		last_access timestamp without time zone,
		entity_map jsonb,
		followup_select text,
    PRIMARY KEY (id)
);

CREATE OR REPLACE FUNCTION public.getmode(IN modetext text)
    RETURNS smallint
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
    
AS $BODY$
declare
	registry_mode smallint;
BEGIN
	IF (modeText = 'auxiliary') THEN
		registry_mode = 0;
	ELSIF (modeText = 'inclusive') THEN
	    registry_mode = 1;
	ELSIF (modeText = 'redirect') THEN
		registry_mode = 2;
	ELSIF (modeText = 'exclusive') THEN
		registry_mode = 3;
	ELSE
		registry_mode = 1;
	END IF;
	RETURN registry_mode;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.updateMapIfNeeded(IN ids text[], ientityMap jsonb, entityMapToken text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    COST 100
    
AS $BODY$
DECLARE
	entityMapEntry jsonb;
	
BEGIN
	if array_length(ids, 1) = 0 or ids is null then
		return ientityMap;
	else
		entityMapEntry := ientityMap -> 'entityMap';
		SELECT jsonb_agg(entry) INTO entityMapEntry FROM jsonb_array_elements(entityMapEntry) as entry, jsonb_object_keys(entry) as id WHERE NOT(id = ANY(ids));
		ientityMap := jsonb_set(ientityMap, '{entityMap}', entityMapEntry);
		UPDATE ENTITYMAP SET LAST_ACCESS = NOW(), entity_map = ientityMap WHERE id=entityMapToken;
		return ientityMap;
	end if;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.getEntityMapAndEntities(IN entityMapToken text, ids text[], ilimit int, ioffset int)
    RETURNS TABLE(id text, entity jsonb, parent boolean, e_types text[], entity_map jsonb)
    LANGUAGE 'plpgsql'
    VOLATILE
    COST 100
    
AS $BODY$
DECLARE
	entitymap jsonb;
	regempty boolean;
	noRootLevelRegEntry boolean;
	queryText text;
BEGIN
	if ids is null or array_length(ids, 1) = 0 then
		UPDATE ENTITYMAP SET LAST_ACCESS = NOW() WHERE ENTITYMAP.id=entityMapToken RETURNING ENTITYMAP.ENTITY_MAP INTO entitymap;
		if entitymap is null then
			RAISE EXCEPTION 'Nonexistent ID --> %', entityMapToken USING ERRCODE = 'S0001';
		end if;
		regempty := entitymap -> 'regEmptyOrNoRegEntryAndNoLinkedQuery';
		noRootLevelRegEntry := entitymap -> 'noRootLevelRegEntryAndLinkedQuery';
		
		if regempty or noRootLevelRegEntry then
			return query execute ('WITH a as (SELECT entityIdEntry.key as id, val.ordinality as ordinality FROM JSONB_ARRAY_ELEMENTS($1) WITH ORDINALITY as val, jsonb_each(val.value) as entityIdEntry where val.ORDINALITY > $2), ' 
					|| (entitymap ->> 'selectPart') || (entitymap ->> 'wherePart') || ' limit $3), X as (SELECT D0.ID as id, max(D0.ordinality) as maxOrdinality FROM D0 GROUP BY D0.ID), C as (SELECT updateMapIfNeeded(ids.aggIds, $4, $5) as entity_map FROM (SELECT ARRAY_AGG(a.id) as aggIds FROM a LEFT JOIN X ON a.id = X.ID WHERE X.ID IS NULL AND a.ordinality <= X.maxOrdinality) as ids)' 
					|| (entitymap ->> 'finalselect')) using (entitymap->'entityMap'), ioffset, ilimit, entitymap, entityMapToken;
		else
			return query execute ('WITH a as (SELECT entityIdEntry.key as id, val.ordinality as ordinality FROM JSONB_ARRAY_ELEMENTS($1) WITH ORDINALITY as val, jsonb_each(val.value) as entityIdEntry where val.ORDINALITY between $2 and ($2 + $3) and entityIdEntry.value ? ''@none''), C as (SELECT $4 as entity_map), ' || (entitymap ->> 'selectPart') || (entitymap ->> 'wherePart')  || ')' ||(entitymap ->> 'finalselect')) using entitymap->'entityMap', ioffset, ilimit, entitymap;
		end if;
	else
		if regempty or noRootLevelRegEntry then
			return query execute ((entitymap ->> 'selectPart') || ' id=any($1) AND ' || (entitymap ->> 'wherePart')  || ')' || (entitymap ->> 'finalselect')) using ids;
		else
			return query execute ((entitymap ->> 'selectPart') || ' id=any($1) AND ' || (entitymap ->> 'wherePart')  || ')' || (entitymap ->> 'finalselect')) using ids;
		end if;
	end if;
END;
$BODY$;

ALTER TABLE IF EXISTS public.csourceinformation DROP COLUMN IF EXISTS entitymap;

ALTER TABLE IF EXISTS public.csourceinformation DROP COLUMN IF EXISTS cancompress;

ALTER TABLE IF EXISTS public.csourceinformation
    ADD COLUMN queryEntityMap boolean;

ALTER TABLE IF EXISTS public.csourceinformation
    ADD COLUMN createEntityMap boolean;
	
ALTER TABLE IF EXISTS public.csourceinformation
    ADD COLUMN updateEntityMap boolean;
	
ALTER TABLE IF EXISTS public.csourceinformation
    ADD COLUMN deleteEntityMap boolean;
	
ALTER TABLE IF EXISTS public.csourceinformation
    ADD COLUMN retrieveEntityMap boolean;

UPDATE public.csourceinformation SET queryEntityMap = false,createEntityMap = false, updateEntityMap = false, deleteEntityMap = false,retrieveEntityMap = false;

CREATE OR REPLACE FUNCTION public.getoperations(IN operationjson jsonb)
    RETURNS boolean[]
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
    
AS $BODY$
declare
	operations boolean[];
	operationEntry jsonb;
BEGIN
	operations = array[false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false]::boolean[];
	FOR operationEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(OPERATIONJSON) LOOP
		CASE operationEntry#>>'{@value}'
		    WHEN 'federationOps' THEN
               operations[21] = true;
               operations[22] = true;
               operations[26] = true;
               operations[27] = true;
               operations[28] = true;
               operations[29] = true;
               operations[30] = true;
               operations[31] = true;
               operations[32] = true;
               operations[33] = true;
               operations[34] = true;
               operations[35] = true;
               operations[36] = true;
			   operations[37] = true;
			   operations[38] = true;
			   operations[39] = true;
			   operations[40] = true;
			   operations[41] = true;
		    WHEN 'updateOps' THEN
        		operations[2] = true;
        		operations[4] = true;
        		operations[18] = true;
        		operations[19] = true;
        	WHEN 'retrieveOps' THEN
                operations[21] = true;
                operations[22] = true;
            WHEN 'redirectionOps' THEN
                operations[1] = true;
                operations[2] = true;
                operations[3] = true;
                operations[4] = true;
                operations[5] = true;
                operations[6] = true;
                operations[17] = true;
                operations[18] = true;
                operations[21] = true;
                operations[22] = true;
                operations[26] = true;
                operations[27] = true;
                operations[28] = true;
                operations[29] = true;
                operations[30] = true;
                operations[31] = true;
			WHEN 'createEntity' THEN
				operations[1] = true;
			WHEN 'updateEntity' THEN
				operations[2] = true;
			WHEN 'appendAttrs' THEN
				operations[3] = true;
			WHEN 'updateAttrs' THEN
				operations[4] = true;
			WHEN 'deleteAttrs' THEN
				operations[5] = true;
			WHEN 'deleteEntity' THEN
				operations[6] = true;
			WHEN 'createBatch' THEN
				operations[7] = true;
			WHEN 'upsertBatch' THEN
				operations[8] = true;
			WHEN 'updateBatch' THEN
				operations[9] = true;
			WHEN 'deleteBatch' THEN
				operations[10] = true;
			WHEN 'upsertTemporal' THEN
				operations[11] = true;
			WHEN 'appendAttrsTemporal' THEN
				operations[12] = true;
			WHEN 'deleteAttrsTemporal' THEN
				operations[13] = true;
			WHEN 'updateAttrsTemporal' THEN
				operations[14] = true;
			WHEN 'deleteAttrInstanceTemporal' THEN
				operations[15] = true;
			WHEN 'deleteTemporal' THEN
				operations[16] = true;
			WHEN 'mergeEntity' THEN
				operations[17] = true;
			WHEN 'replaceEntity' THEN
				operations[18] = true;
			WHEN 'replaceAttrs' THEN
				operations[19] = true;
			WHEN 'mergeBatch' THEN
				operations[20] = true;
			WHEN 'retrieveEntity' THEN
				operations[21] = true;
			WHEN 'queryEntity' THEN
				operations[22] = true;
			WHEN 'queryBatch' THEN
				operations[23] = true;
			WHEN 'retrieveTemporal' THEN
				operations[24] = true;
			WHEN 'queryTemporal' THEN
				operations[25] = true;
			WHEN 'retrieveEntityTypes' THEN
				operations[26] = true;
			WHEN 'retrieveEntityTypeDetails' THEN
				operations[27] = true;
			WHEN 'retrieveEntityTypeInfo' THEN
				operations[28] = true;
			WHEN 'retrieveAttrTypes' THEN
				operations[29] = true;
			WHEN 'retrieveAttrTypeDetails' THEN
				operations[30] = true;
			WHEN 'retrieveAttrTypeInfo' THEN
				operations[31] = true;
			WHEN 'createSubscription' THEN
				operations[32] = true;
			WHEN 'updateSubscription' THEN
				operations[33] = true;
			WHEN 'retrieveSubscription' THEN
				operations[34] = true;
			WHEN 'querySubscription' THEN
				operations[35] = true;
			WHEN 'deleteSubscription' THEN
				operations[36] = true;
			WHEN 'queryEntityMap' THEN
				operations[37] = true;
			WHEN 'createEntityMap' THEN
				operations[38] = true;
			WHEN 'updateEntityMap' THEN
				operations[39] = true;
			WHEN 'deleteEntityMap' THEN
				operations[40] = true;
			WHEN 'retrieveEntityMap' THEN
				operations[41] = true;
		END CASE;
	END LOOP;
	RETURN operations;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.csourceinformation_extract_jsonb_fields()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    VOLATILE
    COST 100
AS $BODY$
DECLARE
    infoEntry jsonb;
	entitiesEntry jsonb;
    entityType text;
	entityId text;
	entityIdPattern text;
	attribsAdded boolean;
	location GEOMETRY(Geometry, 4326);
	scopes text[];
	tenant text;
	regMode smallint;
	operations boolean[];
	endpoint text;
	expires timestamp without time zone;
	headers jsonb;	
	internalId bigint;
	attribName text;
	errorFound boolean;
BEGIN
    IF (TG_OP = 'INSERT' AND NEW.REG IS NOT NULL) OR
        (TG_OP = 'UPDATE' AND OLD.REG IS NULL AND NEW.REG IS NOT NULL) OR
        (TG_OP = 'UPDATE' AND OLD.REG IS NOT NULL AND NEW.REG IS NULL) OR
        (TG_OP = 'UPDATE' AND OLD.REG IS NOT NULL AND NEW.REG IS NOT NULL AND OLD.REG <> NEW.REG) THEN
		errorFound := false;		
		internalId = NEW.id;
		endpoint = NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/endpoint,0,@value}';
		IF NEW.REG ? 'https://uri.etsi.org/ngsi-ld/location' THEN
			IF (NEW.REG@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
				location = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson( NEW.REG#>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0}')::text ), 4326);
			ELSE
				location = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/location,0}')::text ), 4326);
			END IF;
		ELSE
			location = null;
		END IF;
		
		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/scope') THEN
			scopes = getScopes(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/scope}');
		ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/scope') THEN
			scopes = getScopes(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/default-context/scope}');
		ELSE
			scopes = NULL;
		END IF;
		
		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/tenant') THEN
			tenant = NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/tenant,0,@value}';
		ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/tenant') THEN
			tenant = NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/default-context/tenant,0,@value}';
		ELSE
			tenant = NULL;
		END IF;
		
		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/operations') THEN
			operations = getOperations(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/operations}');
		ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/operations') THEN
			operations = getOperations(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/default-context/operations}');
		ELSE
			operations = array[false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true]::boolean[];
		END IF;

		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/contextSourceInfo') THEN
			headers = NEW.REG#>'{https://uri.etsi.org/ngsi-ld/contextSourceInfo}';
		ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/contextSourceInfo') THEN
			headers = NEW.REG#>'{https://uri.etsi.org/ngsi-ld/default-context/contextSourceInfo}';
		ELSE
			headers = NULL;
		END IF;

		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/mode') THEN
			regMode = getMode(NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/mode,0,@value}');
		ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/mode') THEN
			regMode = getMode(NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/default-context/mode,0,@value}');
		ELSE
			regMode = 1;
		END IF;
		
		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/expires') THEN
			expires = (NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/expires,0,@value}')::TIMESTAMP;
		ELSE
			expires = NULL;
		END IF;
		BEGIN
			IF TG_OP = 'UPDATE' THEN
				DELETE FROM csourceinformation where cs_id = NEW.id;
			END IF;
			FOR infoEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/information}') LOOP
				IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/entities' THEN
					FOR entitiesEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/entities}') LOOP
						FOR entityType IN SELECT jsonb_array_elements_text FROM jsonb_array_elements_text(entitiesEntry#>'{@type}') LOOP
							entityId := NULL;
							entityIdPattern := NULL;
							attribsAdded := false;
							IF entitiesEntry ? '@id' THEN
								entityId = entitiesEntry#>>'{@id}';
							END IF;
							IF entitiesEntry ? 'https://uri.etsi.org/ngsi-ld/idPattern' THEN
								entityIdPattern = entitiesEntry#>>'{https://uri.etsi.org/ngsi-ld/idPattern,0,@value}';
							END IF;
							IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/propertyNames' THEN
								attribsAdded = true;
								FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/propertyNames}') LOOP
									IF regMode > 1 THEN
										IF entityId IS NOT NULL THEN 
											SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE id = entityId AND entityType = ANY(e_types) AND ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' != 'https://uri.etsi.org/ngsi-ld/Relationship');
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and id % conflicts with existing entry', attribName, entityId USING ERRCODE='23514';
											END IF;
										ELSIF entityIdPattern IS NOT NULL THEN
											SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE id ~ entityIdPattern AND entityType = ANY(e_types) AND ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' != 'https://uri.etsi.org/ngsi-ld/Relationship');
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and idpattern % conflicts with existing entry', attribName, entityIdPattern USING ERRCODE='23514';
											END IF;
										ELSE
											SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE entityType = ANY(e_types) AND ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' != 'https://uri.etsi.org/ngsi-ld/Relationship');
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and type % conflicts with existing entry', attribName, entityType USING ERRCODE='23514';
											END IF;
										END IF;
									END IF;
									INSERT INTO csourceinformation (cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap) VALUES (internalId, NEW.C_ID, entityId, entityIdPattern, entityType, NULL, attribName, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38],operations[39],operations[40],operations[41]);
								END LOOP;
							END IF;
							IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/relationshipNames' THEN
								attribsAdded = true;
								FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/relationshipNames}') LOOP
									IF regMode > 1 THEN
										IF entityId IS NOT NULL THEN 
											SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE id = entityId AND entityType = ANY(e_types) AND ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship');
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and id % conflicts with existing entry', attribName, entityId USING ERRCODE='23514';
											END IF;
										ELSIF entityIdPattern IS NOT NULL THEN
											SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE id ~ entityIdPattern AND entityType = ANY(e_types) AND ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship');
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and idpattern % conflicts with existing entry', attribName, entityIdPattern USING ERRCODE='23514';
											END IF;
										ELSE
											SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE entityType = ANY(e_types) AND ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship');
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and type % conflicts with existing entry', attribName, entityType USING ERRCODE='23514';
											END IF;
										END IF;
									END IF;
									INSERT INTO csourceinformation (cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap) VALUES (internalId, NEW.C_ID, entityId, entityIdPattern, entityType, attribName, NULL, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38],operations[39],operations[40],operations[41]);
									
								END LOOP;
							END IF;
							IF NOT attribsAdded THEN
								IF regMode > 1 THEN
									IF entityId IS NOT NULL THEN 
										SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE id = entityId AND entityType = ANY(e_types);
										IF errorFound THEN
											RAISE EXCEPTION 'Registration with entityId % conflicts with existing entity', entityId USING ERRCODE='23514';
										END IF;
									ELSIF entityIdPattern IS NOT NULL THEN
										SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE id ~ entityIdPattern AND entityType = ANY(e_types);
										IF errorFound THEN
											RAISE EXCEPTION 'Registration with idPattern % and type % conflicts with existing entity', entityIdPattern, entityType USING ERRCODE='23514';
										END IF;
									ELSE
										SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE entityType = ANY(e_types);
										IF errorFound THEN
											RAISE EXCEPTION 'Registration with type % conflicts with existing entity', entityType USING ERRCODE='23514';
										END IF;
									END IF;
								END IF;
								INSERT INTO csourceinformation(cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap) values (internalId, NEW.C_ID, entityId, entityIdPattern, entityType, NULL, NULL, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38],operations[39],operations[40],operations[41]);
							END IF;
						END LOOP;
					END LOOP;
				ELSE
					IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/propertyNames' THEN
						FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/propertyNames}') LOOP
							SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' != 'https://uri.etsi.org/ngsi-ld/Relationship');
							IF regMode > 1 AND errorFound THEN
								RAISE EXCEPTION 'Attribute % conflicts with existing entity', attribName USING ERRCODE='23514';
							END IF;
							INSERT INTO csourceinformation(cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap) VALUES (internalId, NEW.C_ID, NULL, NULL, NULL, NULL, attribName, location, scopes, expires, endpoint, tenant, headers, regMode, operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38],operations[39],operations[40],operations[41]);
						END LOOP;
					END IF;
					IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/relationshipNames' THEN
						FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/relationshipNames}') LOOP
							SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship');
							IF regMode > 1 AND errorFound THEN
								RAISE EXCEPTION 'Attribute % conflicts with existing entity', attribName USING ERRCODE='23514';
							END IF;
							INSERT INTO csourceinformation(cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap) VALUES (internalId, NEW.C_ID, NULL, NULL, NULL, attribName, NULL, location, scopes, expires, endpoint, tenant, headers, regMode, operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38],operations[39],operations[40],operations[41]);
						END LOOP;
					END IF;
				END IF;
			END LOOP;
		END;
	END IF;
    RETURN NEW;
END;
$BODY$;