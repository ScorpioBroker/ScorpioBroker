
DROP TABLE IF EXISTS public.entitymap;
DROP TABLE IF EXISTS public.entitymap_management;

CREATE TABLE public.entitymap
(
		id text,
		expires_at timestamp without time zone,
		last_access timestamp without time zone,
		entity_map jsonb,
		followup_select text,
    PRIMARY KEY (id)
);

CREATE OR REPLACE FUNCTION public.getmode(IN modetext text)
    RETURNS smallint
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL SAFE
    COST 100
    
AS $BODY$
declare
	registry_mode smallint;
BEGIN
	IF (modeText = 'auxiliary') THEN
		registry_mode = 0;
	ELSIF (modeText = 'inclusive') THEN
	    registry_mode = 1;
	ELSIF (modeText = 'redirect') THEN
		registry_mode = 2;
	ELSIF (modeText = 'exclusive') THEN
		registry_mode = 3;
	ELSE
		registry_mode = 1;
	END IF;
	RETURN registry_mode;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.updateMapIfNeeded(IN ids text[], ientityMap jsonb, entityMapToken text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    VOLATILE
    COST 100
    
AS $BODY$
DECLARE
	entityMapEntry jsonb;
	
BEGIN
	if array_length(ids, 1) = 0 or ids is null then
		return ientityMap;
	else
		entityMapEntry := ientityMap -> 'entityMap';
		SELECT jsonb_agg(entry) INTO entityMapEntry FROM jsonb_array_elements(entityMapEntry) as entry, jsonb_object_keys(entry) as id WHERE NOT(id = ANY(ids));
		ientityMap := jsonb_set(ientityMap, '{entityMap}', entityMapEntry);
		UPDATE ENTITYMAP SET LAST_ACCESS = NOW(), entity_map = ientityMap WHERE id=entityMapToken;
		return ientityMap;
	end if;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.getEntityMapAndEntities(IN entityMapToken text, ids text[], ilimit int, ioffset int)
    RETURNS TABLE(id text, entity jsonb, parent boolean, e_types text[], entity_map jsonb)
    LANGUAGE 'plpgsql'
    VOLATILE
    COST 100
    
AS $BODY$
DECLARE
	entitymap jsonb;
	regempty boolean;
	noRootLevelRegEntry boolean;
	queryText text;
BEGIN
	if ids is null or array_length(ids, 1) = 0 then
		UPDATE ENTITYMAP SET LAST_ACCESS = NOW() WHERE ENTITYMAP.id=entityMapToken RETURNING ENTITYMAP.ENTITY_MAP INTO entitymap;
		if entitymap is null then
			RAISE EXCEPTION 'Nonexistent ID --> %', entityMapToken USING ERRCODE = 'S0001';
		end if;
		regempty := entitymap -> 'regEmptyOrNoRegEntryAndNoLinkedQuery';
		noRootLevelRegEntry := entitymap -> 'noRootLevelRegEntryAndLinkedQuery';
		
		if regempty or noRootLevelRegEntry then
			return query execute ('WITH a as (SELECT entityIdEntry.key as id, val.ordinality as ordinality FROM JSONB_ARRAY_ELEMENTS($1) WITH ORDINALITY as val, jsonb_each(val.value) as entityIdEntry where val.ORDINALITY > $2), ' 
					|| (entitymap ->> 'selectPart') || (entitymap ->> 'wherePart') || ' limit $3), X as (SELECT D0.ID as id, max(D0.ordinality) as maxOrdinality FROM D0 GROUP BY D0.ID), C as (SELECT updateMapIfNeeded(ids.aggIds, $4, $5) as entity_map FROM (SELECT ARRAY_AGG(a.id) as aggIds FROM a LEFT JOIN X ON a.id = X.ID WHERE X.ID IS NULL AND a.ordinality <= X.maxOrdinality) as ids)' 
					|| (entitymap ->> 'finalselect')) using (entitymap->'entityMap'), ioffset, ilimit, entitymap, entityMapToken;
		else
			return query execute ('WITH a as (SELECT entityIdEntry.key as id, val.ordinality as ordinality FROM JSONB_ARRAY_ELEMENTS($1) WITH ORDINALITY as val, jsonb_each(val.value) as entityIdEntry where val.ORDINALITY between $2 and ($2 + $3) and entityIdEntry.value ? ''@none''), C as (SELECT $4 as entity_map), ' || (entitymap ->> 'selectPart') || (entitymap ->> 'wherePart')  || ')' ||(entitymap ->> 'finalselect')) using entitymap->'entityMap', ioffset, ilimit, entitymap;
		end if;
	else
		if regempty or noRootLevelRegEntry then
			return query execute ((entitymap ->> 'selectPart') || ' id=any($1) AND ' || (entitymap ->> 'wherePart')  || ')' || (entitymap ->> 'finalselect')) using ids;
		else
			return query execute ((entitymap ->> 'selectPart') || ' id=any($1) AND ' || (entitymap ->> 'wherePart')  || ')' || (entitymap ->> 'finalselect')) using ids;
		end if;
	end if;
END;
$BODY$;

