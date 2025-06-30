CREATE OR REPLACE FUNCTION public.merge_attrib(IN new_attrib jsonb,IN old_attrib jsonb)
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
  new_dataset_id TEXT;
  old_dataset_id TEXT;
  index INTEGER;
  found boolean;
  deleted jsonb;
  updated jsonb;
  tmp jsonb;
BEGIN
	deleted := '[]'::jsonb;
	updated := '[]'::jsonb;
	if jsonb_typeof(new_attrib) != 'array' then
		RAISE EXCEPTION 'Cannot invalid structure' USING ERRCODE = 'SB002';
	end if;
	if old_attrib is null then
		old_attrib := new_attrib;
	end if;
	for value in select * from jsonb_array_elements(new_attrib) loop
		new_dataset_id = value #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}';
		index := 0;
		found := false;
		for value2 in select * from jsonb_array_elements(old_attrib) loop
			old_dataset_id = value2 #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}';
			if (old_dataset_id is null and new_dataset_id is null) or (old_dataset_id is not null and new_dataset_id is not null and old_dataset_id = new_dataset_id) then
				found := true;
				merged_json = merge_attrib_instance(value, value2);
				EXIT;
			end if;
			index := index + 1;
		end loop;
		if found then
			old_attrib = old_attrib - index;
			if merged_json is not null then
				old_attrib = old_attrib || merged_json;
				if new_dataset_id is null then
					updated := updated || ('null'::jsonb);
				else
					updated := updated || to_jsonb(new_dataset_id);
				end if;
			else
				if new_dataset_id is null then
					deleted := deleted || ('null'::jsonb);
				else
					deleted := deleted || to_jsonb(new_dataset_id);
				end if;
			end if;
		else
			if new_dataset_id is null then
				updated := updated || ('null'::jsonb);
			else
				updated := updated || to_jsonb(new_dataset_id);
			end if;
			old_attrib = old_attrib || value;
		end if;
	end loop;
	if jsonb_array_length(old_attrib) = 0 then
		return jsonb_build_object('result', 'null'::jsonb, 'deleted', deleted, 'updated', updated);
	end if;
return jsonb_build_object('result', old_attrib, 'deleted', deleted, 'updated', updated);
END;
$BODY$;