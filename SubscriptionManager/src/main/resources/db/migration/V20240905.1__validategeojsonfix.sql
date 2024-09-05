CREATE OR REPLACE FUNCTION public.validate_geo_json(IN geo_json_entry jsonb)
    RETURNS void
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
    
AS $BODY$
DECLARE
	geo_type text;
	value jsonb;
	value2 jsonb;
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
	elsif geo_type = 'https://purl.org/geojson/vocab#MultiPoint' then
		if not geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0}' ? '@list' then
			RAISE EXCEPTION 'Invalid multi point update for geo json' USING ERRCODE = 'SB006';
		end if;
		for value in select * from jsonb_array_elements(geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0,@list}') loop
			PERFORM validate_geo_point(value);
		end loop;
	elsif geo_type = 'https://purl.org/geojson/vocab#MultiLineString' then
		if not geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0}' ? '@list' then
			RAISE EXCEPTION 'Invalid multi line string update for geo json' USING ERRCODE = 'SB006';
		end if;
		for value in select * from jsonb_array_elements(geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0,@list}') loop
			if not value #> '{https://purl.org/geojson/vocab#coordinates,0}' ? '@list' then
				RAISE EXCEPTION 'Invalid multi line string update for geo json' USING ERRCODE = 'SB006';
			end if;
			for value2 in select * from jsonb_array_elements(value #> '{https://purl.org/geojson/vocab#coordinates,0,@list}') loop
				PERFORM validate_geo_point(value);
			end loop;
		end loop;

	elsif geo_type = 'https://purl.org/geojson/vocab#MultiPolygon' then
		if not geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0}' ? '@list' or not geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0,@list,0}' ? '@list' then
			RAISE EXCEPTION 'Invalid multi polygon update for geo json' USING ERRCODE = 'SB006';
		end if;
		for value in select * from jsonb_array_elements(geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0,@list}') loop
			if not value #> '{https://purl.org/geojson/vocab#coordinates,0}' ? '@list' or not geo_json_entry #> '{https://purl.org/geojson/vocab#coordinates,0,@list,0}' ? '@list' then
				RAISE EXCEPTION 'Invalid multi polygon update for geo json' USING ERRCODE = 'SB006';
			end if;
			for value2 in select * from jsonb_array_elements(value #> '{https://purl.org/geojson/vocab#coordinates,0,@list,0,@list}') loop
				PERFORM validate_geo_point(value);
			end loop;
		end loop;
	else
		RAISE EXCEPTION 'Invalid geo json type' USING ERRCODE = 'SB007';
	end if;
RETURN;
END;
$BODY$;