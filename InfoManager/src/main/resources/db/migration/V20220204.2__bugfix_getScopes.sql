CREATE OR REPLACE FUNCTION getScopes (scopeList jsonb)
RETURNS text[] AS $scopes$
declare
	scopes text[];
	i jsonb;
BEGIN
     FOR i IN SELECT jsonb_array_elements FROM jsonb_array_elements(scopeList)
		LOOP
		   SELECT array_append(scopes, (i#>>'{@value}')::text) into scopes;
		END LOOP;
   RETURN scopes;
END;
$scopes$ LANGUAGE plpgsql;