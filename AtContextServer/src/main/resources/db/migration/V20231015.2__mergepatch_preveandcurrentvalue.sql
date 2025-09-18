DROP FUNCTION merge_json(text,jsonb);

CREATE OR REPLACE FUNCTION MERGE_JSON(a text, b JSONB)
RETURNS JSONB AS $$
DECLARE
  merged_json JSONB;
  key TEXT;
  value JSONB;
  value2 JSONB;
  previous_entity JSONB;
  ret JSONB;
BEGIN
  merged_json := (Select entity from entity where id =a);
  previous_entity := (Select entity from entity where id =a);
  -- Iterate through keys in JSON B
  FOR key, value IN SELECT * FROM JSONB_EACH(b)
  LOOP
    IF  value::TEXT LIKE '%"https://uri.etsi.org/ngsi-ld/hasValue": [{"@value": "urn:ngsi-ld:null"}]%'::TEXT
     OR value::TEXT LIKE '%"https://uri.etsi.org/ngsi-ld/hasObject": [{"@id": "urn:ngsi-ld:null"}]%'::TEXT
     THEN
      -- Delete the key
      merged_json := merged_json - key;
    ELSIF merged_json ? key THEN
          -- Update the value
		  raise notice '%', 'update';
         value2 := (value->0)::jsonb ;
         IF jsonb_typeof(value2) = 'object' THEN
         	value2 :=value2 - 'https://uri.etsi.org/ngsi-ld/createdAt';
         end if;
         merged_json :=  jsonb_set(merged_json, ARRAY[key], jsonb_build_array(value2), true);
         IF previous_entity->key->0 ? 'https://uri.etsi.org/ngsi-ld/createdAt' then
      		 merged_json := jsonb_set(merged_json, ARRAY[key,'0','https://uri.etsi.org/ngsi-ld/createdAt'], (previous_entity->key->0->'https://uri.etsi.org/ngsi-ld/createdAt'), true);
      	 end if;
      ELSE
        -- Add the key-value pair
		raise notice '%', 'add';
        merged_json := jsonb_set(merged_json, ARRAY[key], value, true);
      END IF;

  END LOOP;
merged_json := jsonb_strip_nulls(replace(merged_json::text,'"urn:ngsi-ld:null"','null')::jsonb);
merged_json := regexp_replace(merged_json::text, '{"@language": "[^"]*"}', 'null', 'g')::jsonb;
while merged_json::text like '%[]%'
	or merged_json::text like '%{}%'
	or merged_json::text like '%null%' loop
merged_json := jsonb_strip_nulls(replace(merged_json::text,'null,','')::jsonb);
merged_json := jsonb_strip_nulls(replace(merged_json::text,', null','')::jsonb);
merged_json := jsonb_strip_nulls(replace(merged_json::text,'[null]','null')::jsonb);
merged_json := jsonb_strip_nulls(replace(merged_json::text,'[]','null')::jsonb);
merged_json := jsonb_strip_nulls(replace(merged_json::text,'{}','null')::jsonb);
end loop;
update entity set entity = merged_json, e_types = ARRAY(SELECT jsonb_array_elements_text(merged_json->'@type')) where id = a;
ret := jsonb_build_array(previous_entity, merged_json);

  RETURN ret;
END;
$$ LANGUAGE plpgsql;
