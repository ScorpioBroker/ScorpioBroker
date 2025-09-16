CREATE OR REPLACE FUNCTION matchScope (scopes text[], scopeQuery text)
RETURNS boolean AS $result$
declare
	i text;
BEGIN
     IF scopes IS NULL THEN
         return false;
     END IF;
     FOREACH i IN ARRAY scopes
		LOOP
		   IF i ~ scopeQuery THEN
		     return true;
		   END IF;
		END LOOP;
   RETURN false;
END;
$result$ LANGUAGE plpgsql;