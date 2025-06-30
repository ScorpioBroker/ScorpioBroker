CREATE OR REPLACE FUNCTION public.validate_geo_point(IN geo_json_entry jsonb)
    RETURNS void
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
AS $BODY$
DECLARE
BEGIN
	if not geo_json_entry ? '@list' or jsonb_array_length(geo_json_entry #> '{@list}') != 2  then
		RAISE EXCEPTION 'Invalid geo point for geo json' USING ERRCODE = 'SB006';
	end if;
RETURN;
END;
$BODY$;

CREATE OR REPLACE FUNCTION PUBLIC.VALIDATE_GEO_JSON(IN GEO_JSON_ENTRY JSONB) RETURNS VOID LANGUAGE 'plpgsql' VOLATILE PARALLEL SAFE COST 100 AS $BODY$
DECLARE
	geo_type text;
	value jsonb;
BEGIN
	geo_type = geo_json_entry #>> '{@type,0}';
	if geo_type = 'https://purl.org/geojson/vocab#Point' then
		PERFORM validate_geo_point(geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0}');
	elsif geo_type = 'https://purl.org/geojson/vocab#LineString' then
		if not geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0}' ? '@list' then
			RAISE EXCEPTION 'Invalid line string update for geo json' USING ERRCODE = 'SB006';
		end if;
		for value in select * from jsonb_array_elements(geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0,@list}') loop
			PERFORM validate_geo_point(value);
		end loop;

	elsif geo_type = 'https://purl.org/geojson/vocab#Polygon' then
		if not geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0}' ? '@list' or not geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0,@list,0}' ? '@list' then
			RAISE EXCEPTION 'Invalid polygon update for geo json' USING ERRCODE = 'SB006';
		end if;
		for value in select * from jsonb_array_elements(geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0,@list,0,@list}') loop
			PERFORM validate_geo_point(value);
		end loop;
	else
		RAISE EXCEPTION 'Invalid geo json type' USING ERRCODE = 'SB007';
	end if;
RETURN;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.clean_ngsi_ld_null(IN json_entry jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
AS $BODY$
DECLARE
	json_type text;
	result jsonb;
	value jsonb;
	cleaned jsonb;
	key text;
BEGIN
	json_type = jsonb_typeof(json_entry);
	if json_type = 'array' then
		result = '[]'::jsonb;
		for value in select * from jsonb_array_elements(json_entry) loop
			cleaned = clean_ngsi_ld_null(value);
			if cleaned is not null then
				result = result || cleaned;
			end if;
		end loop;
		if jsonb_array_length(result) = 0 then
			return null;
		end if;
		return result;
	elsif json_type = 'object' then
		result = '{}';
		for key, value in Select * from jsonb_each(json_entry) loop
			if value::text != '"urn:ngsi-ld:null"' then
				result = jsonb_set(result, '{key}', value);
			end if;
		end loop;
		if result::text = '{}' then
			return null;
		end if;
		return result;
	else
		if json_entry::text = '"urn:ngsi-ld:null"' then
			return null;
		end if;
		return json_entry;
	end if;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.merge_has_json(IN new_attrib jsonb,IN old_attrib jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
AS $BODY$
DECLARE
	new_type text;
	old_type text;
	todelete jsonb;
	deleted integer;
	i integer;
	index integer;
	value jsonb;
	value2 jsonb;
	merged_json jsonb;
	key text;
BEGIN
	new_type = jsonb_typeof(new_attrib);
	old_type = jsonb_typeof(old_attrib);
	if old_attrib is null or new_type != old_type then
		old_attrib := new_attrib;
	end if;
	todelete = '[]'::jsonb;
	if new_type = 'array' then
		if jsonb_array_length(new_attrib) != jsonb_array_length(old_attrib) then
			for i in 0 .. jsonb_array_length(new_attrib) loop
				if new_attrib ->> i = 'urn:ngsi-ld:null' then
					todelete = todelete || i;
				end if;
			end loop;
			deleted = 0;
			if array_length(todelete) > 0 then
				for i in select * from jsonb_array_elements(todelete) loop
					new_attrib = new_attrib - (i - deleted);
					deleted = deleted + 1;
				end loop;
			end if;
			return new_attrib;
		end if;
		index = 0;
		deleted = 0;
		for value in select * from jsonb_array_elements(new_attrib) loop
			if value::text = '"urn:ngsi-ld:null"' then
				old_attrib = old_attrib - (index - deleted);
				deleted = deleted + 1;
				index := index + 1;
				continue;
			end if;
			value2 = old_attrib[index - deleted];
			merged_json = merge_has_json(value, value2);
			if merged_json is null then
				old_attrib = old_attrib - (index - deleted);
				deleted = deleted + 1;
			else
				old_attrib = jsonb_set(old_attrib, ARRAY[(index - deleted)]::text[], merged_json);
			end if;
			index := index + 1;
		end loop;
		if jsonb_array_length(old_attrib) = 0 then
			return null;
		end if;
		return old_attrib;
	elsif new_type = 'object' then
		for key, value in Select * from jsonb_each(new_attrib) loop
			if value::text = '"urn:ngsi-ld:null"' then
				old_attrib = old_attrib - key;
				continue;
			end if;
			merged_json = merge_has_json(value, old_attrib -> key);
			if merged_json is null then
				old_attrib = old_attrib - key;
				continue;
			end if;
			old_attrib = jsonb_set(old_attrib, ARRAY[key]::text[], merged_json);
		end loop;
		if old_attrib::text = '{}' then
			return null;
		end if;
		return old_attrib;
	else
		if new_attrib::text = '"urn:ngsi-ld:null"' then
			return null;
		end if;
		return new_attrib;
	end if;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.merge_has_vocab(IN new_attrib jsonb,IN old_attrib jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
AS $BODY$
DECLARE
	index integer;
	removed integer;
	value jsonb;
	key text;
	value2 jsonb;
BEGIN
	if jsonb_array_length(new_attrib) != jsonb_array_length(old_attrib) then
		if new_attrib #>> '{0,@id}' = 'urn:ngsi-ld:null' then
			return null;
		else
			return new_attrib;
		end if;
	else
		index := 0;
		removed := 0;
		for value in select * from jsonb_array_elements(new_attrib) loop
			for key, value2 in select * from jsonb_each(value) loop
				if key = '@id' then
					if value2::text = '"urn:ngsi-ld:null"' then
						old_attrib = old_attrib - (index - removed);
						removed := removed + 1;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY[(index - removed),key]::text[], value2);
					end if;
				else
					RAISE EXCEPTION 'Unknown type of an attribute for geojson' USING ERRCODE = 'SB003';
				end if;
			end loop;
			index := index + 1;
		end loop;
		if jsonb_array_length(old_attrib) = 0 then
			return null;
		end if;
		return old_attrib;
	end if;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.merge_has_language_map(IN new_attrib jsonb,IN old_attrib jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
AS $BODY$
DECLARE
	value jsonb;
	index integer;
	remove boolean;
	value2 jsonb;
	ln_found boolean;
BEGIN
	if old_attrib is null then
		old_attrib := new_attrib;
	end if;
	for value in Select * from jsonb_array_elements(new_attrib) loop
		if value ->> '@language' = '@none' and value ->> '@value' = 'urn:ngsi-ld:null' then
			return null;
		else
			index = 0;
			ln_found = false;
			remove = false;
			for value2 in Select * from jsonb_array_elements(old_attrib) loop
				if value2 ->> '@language' = value->> '@language' then
					ln_found = true;
					if value ->> '@value' = 'urn:ngsi-ld:null' then
						remove = true;
					end if;
					exit;
				end if;
				index = index + 1;
			end loop;
			if ln_found then
				if remove then
					old_attrib = old_attrib - index;	
				else
					old_attrib = jsonb_set(old_attrib, ARRAY[index,'@value']::text[], value->'@value');
				end if;
			else
				old_attrib = old_attrib || value;
			end if;
		end if;
	end loop;
	RETURN old_attrib;
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
		if new_attrib #>> '{0,https://purl.org/geojson/vocab#coordinates,0,@list,0,@value}' = 'urn:ngsi-ld:null' then
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

CREATE OR REPLACE FUNCTION public.merge_has_object_list(IN new_attrib jsonb,IN old_attrib jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
AS $BODY$
DECLARE
	new_value_list jsonb;
	old_value_list jsonb;
	index integer;
	removed integer;
	value jsonb;
	key text;
	value2 jsonb;
	merged_json jsonb;
BEGIN
	new_value_list = new_attrib #> '{0,@list}';
	if old_attrib is null then
		old_attrib = new_attrib;
	end if;
	old_value_list = old_attrib #> '{0,@list}';
	if jsonb_array_length(new_value_list) != jsonb_array_length(old_value_list) then
		if new_value_list #>> '{0,@id}' = 'urn:ngsi-ld:null' then
			return null;
		else
			return new_attrib;
		end if;
	else
		index := 0;
		removed := 0;
		for value in select * from jsonb_array_elements(new_value_list) loop
			for key, value2 in select * from jsonb_each(value) loop
				if key = 'https://uri.etsi.org/ngsi-ld/hasObject' then
					merged_json = merge_has_object(value2, old_value_list[index - removed] -> key);
					if merged_json is null then
						old_attrib = jsonb_set(old_attrib, ARRAY[0,'@list',(index-removed)]::text[], (old_attrib #> ARRAY[0,'@list',(index-removed)]::text[]) - key);
						removed := removed + 1;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY[0,'@list',(index-removed),key]::text[], merged_json);
					end if;
				else
					RAISE EXCEPTION 'Unknown type of an attribute for relationship' USING ERRCODE = 'SB004';
				end if;
			end loop;
			index := index + 1;
		end loop;
		if jsonb_array_length(old_value_list) = 0 then
			return null;
		end if;
		return old_attrib;
	end if;

END;
$BODY$;

CREATE OR REPLACE FUNCTION public.merge_has_object(IN new_attrib jsonb,IN old_attrib jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
AS $BODY$
DECLARE
	index integer;
	removed integer;
	value jsonb;
	key text;
	value2 jsonb;
BEGIN
	if jsonb_array_length(new_attrib) != jsonb_array_length(old_attrib) then
		if new_attrib #>> '{0,@id}' = 'urn:ngsi-ld:null' then
			return null;
		else
			return new_attrib;
		end if;
	else
		index := 0;
		removed := 0;
		for value in select * from jsonb_array_elements(new_attrib) loop
			for key, value2 in select * from jsonb_each(value) loop
				if key = '@id' then
					if value2::text = '"urn:ngsi-ld:null"' then
						old_attrib = old_attrib - (index - removed);
						removed := removed + 1;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY[(index - removed),key]::text[], value2);
					end if;
				else
					RAISE EXCEPTION 'Unknown type of an attribute for relationship' USING ERRCODE = 'SB003';
				end if;
			end loop;
			index := index + 1;
		end loop;
		if jsonb_array_length(old_attrib) = 0 then
			return null;
		end if;
		return old_attrib;
	end if;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.merge_has_value_list(IN new_attrib jsonb,IN old_attrib jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
AS $BODY$
DECLARE
	new_value_list jsonb;
	old_value_list jsonb;
	index integer;
	removed integer;
	value jsonb;
	key text;
	value2 jsonb;
	merged_json jsonb;
BEGIN
	new_value_list = new_attrib -> '@list';
	if old_attrib is null then
		old_attrib := new_attrib;
	end if;
	old_value_list = old_attrib -> '@list';
	if jsonb_array_length(new_value_list) != jsonb_array_length(old_value_list) then
		if new_value_list #>> '{0,@value}' = 'urn:ngsi-ld:null' then
			return null;
		else
			return new_attrib;
		end if;
	else
		index := 0;
		removed := 0;
		for value in select * from jsonb_array_elements(new_value_list) loop
			for key, value2 in select * from jsonb_each(value) loop
				if key = '@value' then
					if value2::text = '"urn:ngsi-ld:null"' then
						old_attrib = jsonb_set(old_attrib, ARRAY['@list']::text[], (old_attrib #> ARRAY['@list']::text[]) - (index-removed));
						removed := removed + 1;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY['@list',(index-removed),key]::text[], value2);
					end if;
				elsif key = '@list' then
					merged_json = merge_has_value_list(value, old_value_list[index - removed]);
					if merged_json is null then
						old_attrib = jsonb_set(old_attrib, ARRAY['@list']::text[], (old_attrib #> ARRAY['@list']::text[]) - (index-removed));
						removed := removed + 1;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY['@list',(index-removed),key]::text[], merged_json);
					end if;
				
				else
					merged_json = merge_has_value(value2, old_value_list[index - removed] -> key);
					if merged_json is null then
						old_attrib = jsonb_set(old_attrib, ARRAY['@list']::text[], (old_attrib #> ARRAY['@list']::text[]) - (index-removed));
						removed := removed + 1;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY['@list',(index-removed),key]::text[], merged_json);
					end if;
				end if;
			end loop;
			index := index + 1;
		end loop;
		if jsonb_array_length(old_value_list) = 0 then
			return null;
		end if;
		return old_attrib;
	end if;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.merge_has_value(IN new_attrib jsonb,IN old_attrib jsonb)
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
	arr_idx integer;
	key text;
BEGIN
	if old_attrib is null then
		old_attrib := new_attrib;
	end if;
	if jsonb_array_length(new_attrib) != jsonb_array_length(old_attrib) then
		if new_attrib #>> '{0,@value}' = 'urn:ngsi-ld:null' then
			return null;
		else
			return new_attrib;
		end if;
	else
		index := 0;
		removed := 0;
		for value in select * from jsonb_array_elements(new_attrib) loop
			for key, value2 in select * from jsonb_each(value) loop
				if key = '@value' then
					arr_idx := index - removed;
					if value2::text = '"urn:ngsi-ld:null"' then
						old_attrib = old_attrib - arr_idx;
						removed := removed + 1;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY[arr_idx,key]::text[], value2);
					end if;
				else
					arr_idx := index - removed;
					merged_json = merge_has_value(value2, old_attrib #> ARRAY[arr_idx,key]::text[]);
					if merged_json is null then
						old_attrib[arr_idx] = old_attrib[arr_idx] - key;
					else
						old_attrib = jsonb_set(old_attrib, ARRAY[arr_idx,key]::text[], merged_json);
					end if;
				end if;
			end loop;
			index := index + 1;
		end loop;
		if jsonb_array_length(old_attrib) = 0 then
			return null;
		end if;
		return old_attrib;
	end if;
END;
$BODY$;

CREATE OR REPLACE FUNCTION PUBLIC.MERGE_ATTRIB_INSTANCE(IN NEW_ATTRIB JSONB,

																																			IN OLD_ATTRIB JSONB) RETURNS JSONB LANGUAGE 'plpgsql' VOLATILE PARALLEL SAFE COST 100 AS $BODY$
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
				merged_json = merge_attrib(value, old_attrib -> key);
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
				merged_json = merge_attrib(value, old_attrib -> key);
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
				merged_json = merge_attrib(value, old_attrib -> key);
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
				merged_json = merge_attrib(value, old_attrib -> key);
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
				merged_json = merge_attrib(value, old_attrib -> key);
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
				merged_json = merge_attrib(value, old_attrib -> key);
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
				merged_json = merge_attrib(value, old_attrib -> key);
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
				merged_json = merge_attrib(value, old_attrib -> key);
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
					updated := updated || 'null';
				else
					updated := updated || new_dataset_id;
				end if;
			else
				if new_dataset_id is null then
					deleted := deleted || 'null';
				else
					deleted := deleted || new_dataset_id;
				end if;
			end if;
		else
			if new_dataset_id is null then
				updated := updated || 'null';
			else
				updated := updated || new_dataset_id;
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

CREATE OR REPLACE FUNCTION PUBLIC.MERGE_JSON(IN A text,IN B JSONB) RETURNS JSONB LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE COST 100 AS $BODY$
DECLARE
  merged_json JSONB;
  key TEXT;
  value JSONB;
  value2 JSONB;
  previous_entity JSONB;
  deleted JSONB;
  updated JSONB;
BEGIN

Select entity into previous_entity from entity where id =a;
if previous_entity is null then
	RAISE EXCEPTION 'Entity not found.' USING ERRCODE = '02000';
end if;
Select entity into merged_json from entity where id =a;
deleted := '{}';
updated := '{}';
-- Iterate through keys in JSON B
FOR key, value IN SELECT * FROM JSONB_EACH(b)
LOOP
    if key = '@id' or key = 'https://uri.etsi.org/ngsi-ld/createdAt'then
		continue;
	elsif key = '@type' then
		value2 = merged_json -> key;
		WITH combined AS (
			SELECT jsonb_array_elements(value) AS elem
			UNION
			SELECT jsonb_array_elements(value2) AS elem
		)
		SELECT jsonb_agg(elem) into value2 AS merged_array FROM combined;
		merged_json = jsonb_set(merged_json, ARRAY[key]::text[], value2);
	elsif key = 'https://uri.etsi.org/ngsi-ld/modifiedAt' then
		merged_json = jsonb_set(merged_json, ARRAY[key]::text[], value);
	else		
		value2 = merged_json -> key;
		value2 = merge_attrib(value, value2);
		if value2 ->'result' = 'null'::jsonb or jsonb_array_length(value2 ->'result') = 0 then
			merged_json = merged_json - key;
			deleted = jsonb_set(deleted, ARRAY[key]::text[], '["@all"]'::jsonb);
		else
			merged_json = jsonb_set(merged_json, ARRAY[key]::text[], value2 -> 'result');
			if jsonb_array_length(value2 -> 'deleted') != 0 then
				if deleted ? key then
					deleted = jsonb_set(deleted, ARRAY[key], ((deleted -> key) || (value2 -> 'deleted')));
				else
					deleted = jsonb_set(deleted, ARRAY[key], ((value2 -> 'deleted')));
				end if;
			end if;
			
			if jsonb_array_length(value2 -> 'updated') != 0 then
				if updated ? key then
					updated = jsonb_set(updated, ARRAY[key], ((updated -> key) || (value2 -> 'updated')));
				else
					updated = jsonb_set(updated, ARRAY[key], ((value2 -> 'updated')));
				end if;
			end if;
			
		end if;
		
		
	end if;
END LOOP;
update entity set entity = merged_json, e_types = ARRAY(SELECT jsonb_array_elements_text(merged_json->'@type')) where id = a;

RETURN jsonb_build_object('old', previous_entity, 'new', merged_json, 'deleted', deleted, 'updated', updated);
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.merge_json_batch(IN b jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL UNSAFE
    COST 100
    
AS $BODY$
DECLARE
  merged_json JSONB;
  key TEXT;
  value JSONB;
  value2 JSONB;
  previous_entity JSONB;
  ret JSONB;
  newentity jsonb;
  resultObj jsonb;
  entityId text;
  index integer;
BEGIN
  resultObj := '{"success": [], "failure": []}'::jsonb;
  index := 0;
  FOR newentity IN SELECT jsonb_array_elements(b) LOOP
	entityId := newentity->>'@id';
	IF entityId is null then
		resultObj = jsonb_set(resultObj, '{failure}', resultObj -> 'failure' || jsonb_build_object('no id row nr ' || index, 'No entity id provided'));
	else
		BEGIN
			ret := MERGE_JSON(entityId, newentity);
			resultObj = jsonb_set(resultObj, '{success}', resultObj -> 'success' || jsonb_build_object('id', entityId, 'old', ret -> 'old', 'new', ret -> 'new', 'deleted', ret -> 'deleted', 'updated', ret -> 'updated')::jsonb);
		EXCEPTION 
		WHEN OTHERS THEN
			RAISE NOTICE '%, %', SQLSTATE, SQLERRM;
			resultObj = jsonb_set(resultObj, '{failure}', resultObj -> 'failure' || jsonb_object_agg(entityId, SQLSTATE));
		END;
	end if;
	index := index + 1;
  END LOOP;
  RETURN resultObj;
END;
$BODY$;