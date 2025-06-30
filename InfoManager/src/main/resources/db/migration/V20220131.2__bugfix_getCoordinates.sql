CREATE OR REPLACE FUNCTION getCoordinates (coordinateList jsonb)
RETURNS jsonb AS $coordinates$
declare
	coordinates jsonb := '[]';
	i jsonb;
BEGIN
     FOR i IN SELECT jsonb_array_elements FROM jsonb_array_elements(coordinateList)
		LOOP
		   IF i ? '@list' THEN
		     SELECT jsonb_insert(coordinates, '{-1}', getCoordinates(i#>'{@list}'), true) into coordinates;
		   ELSE
			 SELECT jsonb_insert(coordinates,'{-1}',  (i#>'{@value}'), true) into coordinates;
		   END IF;
		END LOOP;
   RETURN coordinates;
END;
$coordinates$ LANGUAGE plpgsql;
