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
	delete boolean;
	counter int;
	found boolean;
BEGIN
	FOR attrKey, attrValue IN SELECT * FROM JSONB_EACH(new_entity) LOOP
		IF attrKey = '@id' OR attrKey = 'https://uri.etsi.org/ngsi-ld/createdAt' THEN
			CONTINUE;
		ELSIF attrKey = '@type' THEN
			old_entity:= jsonb_set(old_entity, ARRAY[attrKey], jsonb_agg(distinct jsonb_array_elements(attrValue || old_entity -> '@type')));
		ELSIF attrKey = 'https://uri.etsi.org/ngsi-ld/modifiedAt' THEN
			old_entity:= jsonb_set(old_entity, ARRAY[attrKey], attrValue);
		ELSE
			IF NOT old_entity ? attrKey THEN
				old_entity:= jsonb_set(old_entity, ARRAY[attrKey], attrValue);
			ELSE
				FOR attrInstance IN SELECT * FROM JSONB_ARRAY_ELEMENTS(attrValue) LOOP
					delete := FALSE;
					IF attrInstance ? 'https://uri.etsi.org/ngsi-ld/datasetId' THEN
						datasetId := attrInstance #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@value}';
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
							IF attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasObjectList":[{"@list":[{"@value": "urn:ngsi-ld:null"}]}]}' THEN
								delete := TRUE;
							END IF;
						WHEN 'https://uri.etsi.org/ngsi-ld/JsonProperty' THEN
							IF attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasJSON":[{"@type": "@json","@value": ["urn:ngsi-ld:null"]}]}' THEN
								delete := TRUE;
							END IF;
						WHEN 'https://uri.etsi.org/ngsi-ld/VocabProperty' THEN
							IF attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasVocabs":[{"@list":[{"@value": "urn:ngsi-ld:null"}]}]}' THEN
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
						IF (datasetId IS NULL AND NOT oldAttrInstance ? 'https://uri.etsi.org/ngsi-ld/datasetId') OR (oldAttrInstance ? 'https://uri.etsi.org/ngsi-ld/datasetId' AND oldAttrInstance #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@value}' = datasetId) THEN
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
						oldAttrValue := oldAttrValue || attrInstance;
						old_entity := jsonb_set(old_entity, ARRAY[attrKey], oldAttrValue);
					ELSE
						IF jsonb_array_length(oldAttribValue) > 0 THEN
							old_entity := jsonb_set(old_entity, ARRAY[attrKey], oldAttrValue);
						ELSE
							old_entity := old_entity - attrKey;	
						END IF;
					END IF;
					
				END LOOP;
			END IF;
		END IF;
	END LOOP;
	RETURN old_entity;
END;
$BODY$;