CREATE OR REPLACE FUNCTION ngsild_update_entity(IN old_entity jsonb, IN new_entity jsonb, IN doReplace boolean)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
AS $BODY$
DECLARE
    attrValue jsonb;
	attrKey text;
	datasetId text;
	attrType text;
	attrInstance jsonb;
	oldAttrValue jsonb;
	oldAttrInstance jsonb;
	newTypes jsonb;
	delete boolean;
	counter int;
	found boolean;
BEGIN
	FOR attrKey, attrValue IN SELECT * FROM JSONB_EACH(new_entity) LOOP
		IF attrKey = '@id' OR attrKey = 'https://uri.etsi.org/ngsi-ld/createdAt' THEN
			CONTINUE;
		ELSIF attrKey = '@type' THEN
			SELECT jsonb_agg(distinct e_types) INTO newTypes FROM jsonb_array_elements((attrValue || (old_entity -> '@type'))) as e_types;
			old_entity:= jsonb_set(old_entity, ARRAY[attrKey], newTypes);
		ELSIF attrKey = 'https://uri.etsi.org/ngsi-ld/modifiedAt' THEN
			old_entity:= jsonb_set(old_entity, ARRAY[attrKey], attrValue);
		ELSE
				FOR attrInstance IN SELECT * FROM JSONB_ARRAY_ELEMENTS(attrValue) LOOP
					delete := FALSE;
					IF attrInstance ? 'https://uri.etsi.org/ngsi-ld/datasetId' THEN
						datasetId := attrInstance #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}';
					ELSE
						datasetId := null;
					END IF;
					attrType := attrInstance #>> '{@type,0}';
					CASE attrType
						WHEN 'https://uri.etsi.org/ngsi-ld/Property' THEN
							IF attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasValue":[{"@value": "urn:ngsi-ld:null"}]}' THEN
								delete := TRUE;
							END IF;
						WHEN 'https://uri.etsi.org/ngsi-ld/Relationship' THEN
							IF attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasObject":[{"@id": "urn:ngsi-ld:null"}]}' THEN
								delete := TRUE;
							END IF;
						WHEN 'https://uri.etsi.org/ngsi-ld/ListProperty' THEN
							IF attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasValueList":[{"@list":[{"@value": "urn:ngsi-ld:null"}]}]}' THEN
								delete := TRUE;
							END IF;
						WHEN 'https://uri.etsi.org/ngsi-ld/ListRelationship' THEN
							IF attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasObjectList":[{"@list":[{"https://uri.etsi.org/ngsi-ld/hasObject":[{"@id":"urn:ngsi-ld:null"}]}]}]}' THEN
								delete := TRUE;
							END IF;
						WHEN 'https://uri.etsi.org/ngsi-ld/JsonProperty' THEN
							IF attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasJSON":[{"@type":"@json","@value":"urn:ngsi-ld:null"}]}' THEN
								delete := TRUE;
							END IF;
						WHEN 'https://uri.etsi.org/ngsi-ld/VocabProperty' THEN
							IF attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasVocab":[{"@id":"urn:ngsi-ld:null"}]}' THEN
								delete := TRUE;
							END IF;
						WHEN 'https://uri.etsi.org/ngsi-ld/LanguageProperty' THEN
							IF attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasLanguageMap":[{"@value": "urn:ngsi-ld:null"}]}' THEN
								delete := TRUE;
							END IF;
						WHEN 'https://uri.etsi.org/ngsi-ld/GeoProperty' THEN
							IF attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasValue":[{"@value": "urn:ngsi-ld:null"}]}' THEN
								delete := TRUE;
							END IF;
						ELSE
							CONTINUE;
					END CASE;
					counter := 0;
					found := FALSE;
					oldAttrValue := old_entity -> attrKey;
					FOR oldAttrInstance IN  SELECT * FROM JSONB_ARRAY_ELEMENTS(oldAttrValue) LOOP
						IF (datasetId IS NULL AND NOT oldAttrInstance ? 'https://uri.etsi.org/ngsi-ld/datasetId') OR (oldAttrInstance ? 'https://uri.etsi.org/ngsi-ld/datasetId' AND oldAttrInstance #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}' = datasetId) THEN
							found := TRUE;
							EXIT;
						END IF;
						counter := counter + 1;
					END LOOP;
					IF found THEN
						IF NOT doReplace THEN
							CONTINUE;
						END IF;
						oldAttrValue := oldAttrValue - counter;
					END IF;
					IF NOT delete THEN
						IF oldAttrValue IS NULL THEN
							oldAttrValue := jsonb_build_array(attrInstance);
						ELSE
							oldAttrValue := oldAttrValue || attrInstance;
						END IF;
						old_entity := jsonb_set(old_entity, ARRAY[attrKey], oldAttrValue);
					ELSE
						IF jsonb_array_length(oldAttrValue) > 0 THEN
							old_entity := jsonb_set(old_entity, ARRAY[attrKey], oldAttrValue);
						ELSE
							old_entity := old_entity - attrKey;	
						END IF;
					END IF;
					
				END LOOP;
			
		END IF;
	END LOOP;
	RETURN old_entity;
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
    new_entity jsonb;
    prev_entity jsonb;
    updated_entity jsonb;
    not_overwriting boolean;
BEGIN
    resultObj := '{"success": [], "failure": []}'::jsonb;

    FOR new_entity IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITIES) LOOP
        prev_entity := NULL;
		not_overwriting := false;
        BEGIN
            SELECT ENTITY FROM ENTITY WHERE ID = new_entity->>'@id' INTO prev_entity;
			updated_entity := ngsild_update_entity(prev_entity, new_entity, not nooverwrite);
			IF (prev_entity = updated_entity AND nooverwrite) THEN
				not_overwriting := true;
			END IF;
            IF not_overwriting THEN
				resultObj = jsonb_set(resultObj, '{failure}', resultObj -> 'failure' || jsonb_build_object(new_entity->>'@id', 'Not Overwriting'));
            ELSIF NOT FOUND THEN
				resultObj = jsonb_set(resultObj, '{failure}', resultObj -> 'failure' || jsonb_build_object(new_entity->>'@id', 'Not Found'));
            ELSE
            	UPDATE entity SET entity = updated_entity WHERE id = updated_entity->>'@id';
				resultObj = jsonb_set(resultObj, '{success}', resultObj -> 'success' || jsonb_build_object('id', new_entity->'@id', 'old', prev_entity, 'new', updated_entity)::jsonb);
            END IF;
			
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE '%', SQLERRM;
				resultObj = jsonb_set(resultObj, '{failure}', resultObj -> 'failure' || jsonb_build_object(new_entity->>'@id', SQLSTATE));
        END;
    END LOOP;

    RETURN resultObj;
END;
$BODY$;