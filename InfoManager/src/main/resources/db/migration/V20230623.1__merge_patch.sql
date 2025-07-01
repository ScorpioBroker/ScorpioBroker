CREATE OR REPLACE FUNCTION MERGE_JSON(a text, b JSONB)
RETURNS JSONB AS $$
DECLARE
  merged_json JSONB;
  key TEXT;
  value JSONB;
  previous_entity JSONB;
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
         merged_json := jsonb_set(merged_json, ARRAY[key], value, true);
      ELSE
        -- Add the key-value pair
		raise notice '%', 'add';
        merged_json := jsonb_set(merged_json, ARRAY[key], value, true);
      END IF;

  END LOOP;
if merged_json::text like '%"urn:ngsi-ld:null"%' THEN
merged_json := jsonb_strip_nulls(replace(merged_json::text,'"urn:ngsi-ld:null"','null')::jsonb);
end if;
update entity set entity = merged_json, e_types = ARRAY(SELECT jsonb_array_elements_text(merged_json->'@type')) where id = a;
  RETURN previous_entity;
END;
$$ LANGUAGE plpgsql;
