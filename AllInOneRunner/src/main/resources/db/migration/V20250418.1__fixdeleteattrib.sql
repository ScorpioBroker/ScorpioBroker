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

CREATE OR REPLACE FUNCTION public.ngsild_update_entity(IN old_entity jsonb,IN new_entity jsonb,IN doreplace boolean)
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
		ELSIF attrKey = 'https://uri.etsi.org/ngsi-ld/modifiedAt' OR attrKey = 'https://uri.etsi.org/ngsi-ld/scope' THEN
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
							IF attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasObjectList":[{"@list":[{"https://uri.etsi.org/ngsi-ld/hasObject":[{"@id":"urn:ngsi-ld:null"}]}]}]}' OR attrInstance @> '{"https://uri.etsi.org/ngsi-ld/hasObjectList":[{"@list":[{"@value": "urn:ngsi-ld:null"}]}]}' THEN
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

CREATE OR REPLACE FUNCTION public.merge_attrib_instance(IN new_attrib jsonb,IN old_attrib jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
    
AS $BODY$
DECLARE
  merged_json JSONB;
  value JSONB;
  value2 JSONB;
  attrib_type TEXT;
  old_dataset_id TEXT;
  index INTEGER;
  found boolean;
  key text;
BEGIN
	if old_attrib is null then
		old_attrib := new_attrib;
	end if;
	new_attrib := new_attrib - 'https://uri.etsi.org/ngsi-ld/createdAt';
	attrib_type := old_attrib #>> '{@type,0}';
	if attrib_type != new_attrib #>> '{@type,0}' then
		RAISE EXCEPTION 'Cannot change type of an attribute' USING ERRCODE = 'SB001';
	end if;
	if attrib_type = 'https://uri.etsi.org/ngsi-ld/Property' then
		for key, value in SELECT * FROM JSONB_EACH(new_attrib) loop
			if key = '@type' or key = 'https://uri.etsi.org/ngsi-ld/datasetId' then
				continue;
			elsif key = 'https://uri.etsi.org/ngsi-ld/hasValue' then
				merged_json = merge_has_value(value, old_attrib -> key);
				if merged_json is null then
					return null;
				end if;
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
			elsif key = 'https://uri.etsi.org/ngsi-ld/unitCode' then
				if value #>> '{0,@value}' = 'urn:ngsi-ld:null' then
					old_attrib = old_attrib - key;
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], value);
				end if;
			elsif key = 'https://uri.etsi.org/ngsi-ld/modifiedAt' or key = 'https://uri.etsi.org/ngsi-ld/observedAt' then
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], value);
			else
				merged_json = merge_attrib(value, old_attrib -> key) -> 'result';
				if merged_json is null then
					old_attrib = old_attrib - key;
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
				end if;
			end if;
		end loop;
	elsif attrib_type = 'https://uri.etsi.org/ngsi-ld/Relationship' then
		for key, value in SELECT * FROM JSONB_EACH(new_attrib) loop
			if key = '@type' or key = 'https://uri.etsi.org/ngsi-ld/datasetId' then
				continue;
			elsif key = 'https://uri.etsi.org/ngsi-ld/hasObjectType' then
				if value #>> '{0,@id}' = 'urn:ngsi-ld:null' then
					old_attrib = old_attrib - key;
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], value);
				end if;
			elsif key = 'https://uri.etsi.org/ngsi-ld/hasObject' then
				merged_json = merge_has_object(value, old_attrib -> key);
				if merged_json is null then
					return null;
				end if;
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
			elsif key = 'https://uri.etsi.org/ngsi-ld/modifiedAt' or key = 'https://uri.etsi.org/ngsi-ld/observedAt' then
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], value);
			else
				merged_json = merge_attrib(value, old_attrib -> key) -> 'result';
				if merged_json is null then
					old_attrib = old_attrib - key;
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
				end if;
			end if;
		end loop;
	elsif attrib_type = 'https://uri.etsi.org/ngsi-ld/ListProperty' then
		for key, value in SELECT * FROM JSONB_EACH(new_attrib) loop
			if key = '@type' or key = 'https://uri.etsi.org/ngsi-ld/datasetId' then
				continue;
			elsif key = 'https://uri.etsi.org/ngsi-ld/hasValueList' then
				merged_json = merge_has_value_list(value[0], old_attrib #> '{key,0}');
				if merged_json is null then
					return null;
				end if;
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
			elsif key = 'https://uri.etsi.org/ngsi-ld/unitCode' then
				if value #>> '{0,@value}' = 'urn:ngsi-ld:null' then
					old_attrib = old_attrib - key;
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], value);
				end if;
			elsif key = 'https://uri.etsi.org/ngsi-ld/modifiedAt' or key = 'https://uri.etsi.org/ngsi-ld/observedAt' then
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], value);
			else
				merged_json = merge_attrib(value, old_attrib -> key) -> 'result';
				if merged_json is null then
					old_attrib = old_attrib - key;
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
				end if;
			end if;
		end loop;
	elsif attrib_type = 'https://uri.etsi.org/ngsi-ld/ListRelationship' then
		for key, value in SELECT * FROM JSONB_EACH(new_attrib) loop
			if key = '@type' or key = 'https://uri.etsi.org/ngsi-ld/datasetId' then
				continue;
			elsif key = 'https://uri.etsi.org/ngsi-ld/hasObjectType' then
				if value #>> '{0,@id}' = 'urn:ngsi-ld:null' then
					old_attrib = old_attrib - key;
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], value);
				end if;
			elsif key = 'https://uri.etsi.org/ngsi-ld/hasObjectList' then
				merged_json = merge_has_object_list(value, old_attrib -> key);
				if merged_json is null then
					return null;
				end if;
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
			elsif key = 'https://uri.etsi.org/ngsi-ld/modifiedAt' or key = 'https://uri.etsi.org/ngsi-ld/observedAt' then
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], value);
			else
				merged_json = merge_attrib(value, old_attrib -> key) -> 'result';
				if merged_json is null then
					old_attrib = old_attrib - key;
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
				end if;
			end if;
		end loop;
	elsif attrib_type = 'https://uri.etsi.org/ngsi-ld/GeoProperty' then
		for key, value in SELECT * FROM JSONB_EACH(new_attrib) loop
			if key = '@type' or key = 'https://uri.etsi.org/ngsi-ld/datasetId' then
				continue;
			elsif key = 'https://uri.etsi.org/ngsi-ld/hasValue' then
				merged_json = merge_has_value_geo(value, old_attrib -> key);
				if merged_json is null then
					return null;
				end if;
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
			elsif key = 'https://uri.etsi.org/ngsi-ld/modifiedAt' or key = 'https://uri.etsi.org/ngsi-ld/observedAt' then
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], value);
			else
				merged_json = merge_attrib(value, old_attrib -> key) -> 'result';
				if merged_json is null then
					old_attrib = old_attrib - key;
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
				end if;
			end if;
		end loop;
	elsif attrib_type = 'https://uri.etsi.org/ngsi-ld/LanguageProperty' then
		for key, value in SELECT * FROM JSONB_EACH(new_attrib) loop
			if key = '@type' or key = 'https://uri.etsi.org/ngsi-ld/datasetId' then
				continue;
			elsif key = 'https://uri.etsi.org/ngsi-ld/hasLanguageMap' then
				merged_json = merge_has_language_map(value, old_attrib -> key);
				if merged_json is null then
					return null;
				end if;
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
			elsif key = 'https://uri.etsi.org/ngsi-ld/modifiedAt' or key = 'https://uri.etsi.org/ngsi-ld/observedAt' then
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], value);
			else
				merged_json = merge_attrib(value, old_attrib -> key) -> 'result';
				if merged_json is null then
					old_attrib = old_attrib - key;
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
				end if;
			end if;
		end loop;
	elsif attrib_type = 'https://uri.etsi.org/ngsi-ld/VocabProperty' then
		for key, value in SELECT * FROM JSONB_EACH(new_attrib) loop
			if key = '@type' or key = 'https://uri.etsi.org/ngsi-ld/datasetId' then
				continue;
			elsif key = 'https://uri.etsi.org/ngsi-ld/hasVocab' then
				merged_json = merge_has_vocab(value, old_attrib -> key);
				if merged_json is null then
					return null;
				end if;
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
			elsif key = 'https://uri.etsi.org/ngsi-ld/modifiedAt' or key = 'https://uri.etsi.org/ngsi-ld/observedAt' then
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], value);
			else
				merged_json = merge_attrib(value, old_attrib -> key) -> 'result';
				if merged_json is null then
					old_attrib = old_attrib - key;
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
				end if;
			end if;
		end loop;
	elsif attrib_type = 'https://uri.etsi.org/ngsi-ld/JsonProperty' then
		for key, value in SELECT * FROM JSONB_EACH(new_attrib) loop
			if key = '@type' or key = 'https://uri.etsi.org/ngsi-ld/datasetId' then
				continue;
			elsif key = 'https://uri.etsi.org/ngsi-ld/hasJSON' then
				merged_json = merge_has_json(value #> ARRAY[0,'@value']::text[], old_attrib #> ARRAY[key,0,'@value']::text[]);
				if merged_json is null then
					return null;
				end if;
				old_attrib = jsonb_set(old_attrib, ARRAY[key,0,'@value']::text[], merged_json);
			elsif key = 'https://uri.etsi.org/ngsi-ld/modifiedAt' or key = 'https://uri.etsi.org/ngsi-ld/observedAt' then
				old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], value);
			else
				merged_json = merge_attrib(value, old_attrib -> key) -> 'result';
				if merged_json is null then
					old_attrib = old_attrib - key;
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
				end if;
			end if;
		end loop;
	else
		RAISE EXCEPTION 'Unknown type of an attribute %, %, %', attrib_type, old_attrib, new_attrib USING ERRCODE = 'SB002';
	end if;
	return old_attrib;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.merge_has_value_geo(IN new_attrib jsonb,IN old_attrib jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
    
AS $BODY$
DECLARE
	value jsonb;
	value2 jsonb;
	merged_json jsonb;
	index integer;
	removed integer;
	key text;
BEGIN
	if jsonb_array_length(new_attrib) != jsonb_array_length(old_attrib) then
		if new_attrib #>> '{0,https://purl.org/geojson/vocab#coordinates,0,@list,0,@value}' = 'urn:ngsi-ld:null'  or new_attrib #>> '{0,@value}' = 'urn:ngsi-ld:null' then
			return null;
		else
			for value in select * from jsonb_array_elements(new_attrib) loop
				PERFORM validate_geo_json(value);
			end loop;
			return new_attrib;
		end if;
	else
		index := 0;
		removed := 0;
		for value in select * from jsonb_array_elements(new_attrib) loop
			for key, value2 in select * from jsonb_each(value) loop
				if key = 'https://purl.org/geojson/vocab#coordinates' then
					if value2 #>> '{0,@list,0,@value}' = 'urn:ngsi-ld:null' then
						old_attrib = old_attrib - (index - removed);
						removed := removed + 1;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY[(index - removed),key]::text[], value2);
					end if;
				elsif key = '@type' then
					old_attrib = jsonb_set(old_attrib, ARRAY[(index - removed),key]::text[], value2);
				else
					RAISE EXCEPTION 'Unknown type of an attribute for geojson' USING ERRCODE = 'SB003';
				end if;
			end loop;
			PERFORM validate_geo_json(old_attrib[(index - removed)]);
			index := index + 1;
		end loop;
		if jsonb_array_length(old_attrib) = 0 then
			return null;
		end if;
		return old_attrib;
	end if;
END;
$BODY$;