ALTER TABLE entity ALTER COLUMN context TYPE integer[] USING null;
ALTER TABLE entity ADD ogcompacted jsonb;
ALTER TABLE entity ADD ogkvcompacted jsonb;
ALTER TABLE entity ADD ogwscompacted jsonb;
ALTER TABLE entity ADD ogcontext jsonb;,
CREATE OR REPLACE FUNCTION matchContext (ogContext jsonb, ogContextHash integer[], newContext jsonb, newContextHash integer, id text)
RETURNS boolean AS $result$
DECLARE
tmp integer[];
BEGIN
	IF ogContextHash @> newContextHash THEN
		return true;
	END IF;
	IF ogContext @> newContext THEN
		tmp = array_append(bgContextHash, id);
		UPDATE entity SET context = '@tmp' WHERE id = '@id';
		return true;
	END IF;
	RETURN false;
END;
$result$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION entity_new_request_context_hash() RETURNS trigger AS $_$
	DECLARE 
		tmp integer;
    BEGIN
		RAISE NOTICE 'CALLED';
		RAISE NOTICE '%', NEW.context;
		RAISE NOTICE '%', OLD.context;
		IF NEW.context IS NOT NULL AND OLD.context IS NOT NULL THEN
			RAISE NOTICE 'UPDATE';
			tmp = NEW.context[0];
			UPDATE entity SET context = NEW.context WHERE context[0] = tmp;
		END IF;
        RETURN NEW;
    END;

$_$ LANGUAGE plpgsql;

CREATE TRIGGER entity_new_request_context_hash AFTER UPDATE OF context ON entity
    FOR EACH ROW EXECUTE PROCEDURE entity_new_request_context_hash();
