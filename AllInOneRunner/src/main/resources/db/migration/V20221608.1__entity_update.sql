ALTER TABLE entity ALTER COLUMN context TYPE integer[] USING null;
ALTER TABLE entity ADD ogcompacted jsonb;
ALTER TABLE entity ADD ogkvcompacted jsonb;
ALTER TABLE entity ADD ogwscompacted jsonb;
ALTER TABLE entity ADD ogcontext jsonb;
CREATE OR REPLACE FUNCTION matchContext (ogContext jsonb, ogContextHash integer[], newContext jsonb, newContextHash integer, id text)
RETURNS boolean AS $result$
DECLARE
tmp integer[];
BEGIN
	IF newContextHash=ANY(ogContextHash)  THEN
		return true;
	END IF;
	IF ogContext @> newContext THEN
		tmp = array_append(bgContextHash, id);
		UPDATE entity SET context = tmp WHERE id = '@id';
		return true;
	END IF;
	RETURN false;
END;
$result$ LANGUAGE plpgsql;


