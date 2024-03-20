CREATE OR REPLACE FUNCTION MERGE_JSON_BATCH(b JSONB)
RETURNS JSONB AS $$
DECLARE
  merged_json JSONB;
  key TEXT;
  value JSONB;
  value2 JSONB;
  previous_entity JSONB;
  ret JSONB[];
  newentity jsonb;
  resultObj jsonb;
BEGIN
  resultObj := '{"success": [], "failure": []}'::jsonb;

  FOR newentity IN SELECT jsonb_array_elements(b) LOOP
    IF EXISTS (SELECT entity FROM entity WHERE id = newentity->'@id'->>0) THEN
      merged_json := (SELECT entity FROM entity WHERE id = newentity->'@id'->>0);
      previous_entity := merged_json;
      resultObj['success'] = resultObj['success'] || jsonb_build_object('id',newentity->'@id')::jsonb;
    ELSE
      resultObj['failure'] := resultObj['failure'] || jsonb_object_agg(newentity->'@id'->>0, 'Not Found');
      CONTINUE;
    END IF;

    -- Iterate through keys in JSONB value
    FOR key, value IN SELECT * FROM JSONB_EACH(newentity) LOOP
      IF value::TEXT LIKE '%"https://uri.etsi.org/ngsi-ld/hasValue": [{"@value": "urn:ngsi-ld:null"}]%'::TEXT
         OR value::TEXT LIKE '%"https://uri.etsi.org/ngsi-ld/hasObject": [{"@id": "urn:ngsi-ld:null"}]%'::TEXT THEN
        -- Delete the key
        merged_json := merged_json - key;
      ELSIF merged_json ? key THEN
        -- Update the value
        value2 := (value->0)::jsonb;
        IF jsonb_typeof(value2) = 'object' THEN
          value2 := value2 - 'https://uri.etsi.org/ngsi-ld/createdAt';
        END IF;
        merged_json := jsonb_set(merged_json, ARRAY[key], jsonb_build_array(value2), true);
        IF previous_entity->key->0 ? 'https://uri.etsi.org/ngsi-ld/createdAt' THEN
          merged_json := jsonb_set(merged_json, ARRAY[key,'0','https://uri.etsi.org/ngsi-ld/createdAt'], (previous_entity->key->0->'https://uri.etsi.org/ngsi-ld/createdAt'), true);
        END IF;
      ELSE
        -- Add the key-value pair
        merged_json := jsonb_set(merged_json, ARRAY[key], value, true);
      END IF;
    END LOOP;

    -- Perform cleanup operations on merged_json
    merged_json := jsonb_strip_nulls(replace(merged_json::text,'"urn:ngsi-ld:null"','null')::jsonb);
    merged_json := regexp_replace(merged_json::text, '{"@language": "[^"]*"}', 'null', 'g')::jsonb;

    WHILE merged_json::text LIKE '%[]%' OR merged_json::text LIKE '%{}%' OR merged_json::text LIKE '%null%' LOOP
      merged_json := jsonb_strip_nulls(replace(merged_json::text,'null,','')::jsonb);
      merged_json := jsonb_strip_nulls(replace(merged_json::text,', null','')::jsonb);
      merged_json := jsonb_strip_nulls(replace(merged_json::text,'[null]','null')::jsonb);
      merged_json := jsonb_strip_nulls(replace(merged_json::text,'[]','null')::jsonb);
      merged_json := jsonb_strip_nulls(replace(merged_json::text,'{}','null')::jsonb);
    END LOOP;

    -- Update entity table with merged JSON and extract @type values into an array
    UPDATE entity SET entity = merged_json, e_types = ARRAY(SELECT jsonb_array_elements_text(merged_json->'@type')) WHERE id = newentity->'@id'->>0;
  END LOOP;

  -- Return the result object
  RETURN resultObj;
END;
$$ LANGUAGE plpgsql;
