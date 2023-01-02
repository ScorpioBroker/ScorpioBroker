--TEMPORAL SETUP
DROP FUNCTION public.temporalentityattrinstance_extract_jsonb_fields CASCADE;
DROP FUNCTION public.temporalentityattrinstance_update_static CASCADE;
ALTER TABLE IF EXISTS public.temporalentity
    RENAME id TO e_id;
ALTER TABLE IF EXISTS public.temporalentity
    ADD CONSTRAINT unique_temp_e_id UNIQUE (E_ID);
ALTER TABLE IF EXISTS public.temporalentity
	ADD COLUMN deletedat timestamp without time zone;
--kills fkey from temporalattrinstance
ALTER TABLE IF EXISTS public.temporalentity DROP CONSTRAINT IF EXISTS temporalentity_pkey CASCADE;
ALTER TABLE IF EXISTS public.temporalentity
    ADD COLUMN id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 );
ALTER TABLE public.temporalentity ADD PRIMARY KEY (id);

ALTER TABLE IF EXISTS public.temporalentityattrinstance
    RENAME internalid TO id;
ALTER TABLE IF EXISTS public.temporalentityattrinstance
    ADD COLUMN iid bigint;
ALTER TABLE IF EXISTS public.temporalentityattrinstance
	ADD COLUMN deletedat timestamp without time zone;
ALTER TABLE IF EXISTS public.temporalentityattrinstance
    ADD COLUMN dataset_id text;
ALTER TABLE IF EXISTS public.temporalentityattrinstance
    ADD COLUMN attr_value jsonb;
ALTER TABLE IF EXISTS public.temporalentityattrinstance
    ADD COLUMN is_rel boolean;
ALTER TABLE IF EXISTS public.temporalentityattrinstance
    ADD COLUMN is_geo boolean;
ALTER TABLE IF EXISTS public.temporalentityattrinstance
    ADD COLUMN is_lang boolean;
ALTER TABLE IF EXISTS public.temporalentityattrinstance
    ADD COLUMN is_toplevel boolean;
ALTER TABLE IF EXISTS public.temporalentityattrinstance
    DROP COLUMN value;
ALTER TABLE IF EXISTS public.temporalentityattrinstance
    DROP COLUMN static;
--todo add update to generate the values from existing entries


ALTER TABLE IF EXISTS public.temporalentityattrinstance
    ADD CONSTRAINT iid_fkey FOREIGN KEY (iid)
    REFERENCES public.temporalentity (id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE;

CREATE TABLE public.tempetype2iid
(
    e_type text,
    iid bigint,
    CONSTRAINT "tempPrKey" PRIMARY KEY (e_type, iid)
);

CREATE TABLE public.tempescope2iid
(
    e_scope text,
    iid bigint
	CONSTRAINT "tempScopePrKey" PRIMARY KEY (e_scope, iid)
);
ALTER TABLE IF EXISTS public.tempescope2iid
    ADD CONSTRAINT iid_fkey FOREIGN KEY (iid)
    REFERENCES public.temporalentity (id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE;



	
CREATE INDEX tempt_index
    ON public.tempetype2iid USING hash
    (e_type text_pattern_ops);
	
ALTER TABLE IF EXISTS public.tempetype2iid
    ADD CONSTRAINT iid_fkey FOREIGN KEY (iid)
    REFERENCES public.temporalentity (id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS fki_iid_fkey
    ON public.tempetype2iid(iid);

INSERT INTO public.tempetype2iid SELECT type, id FROM public.temporalentity;

ALTER TABLE PUBLIC.temporalentity DROP COLUMN type;
ALTER TABLE PUBLIC.temporalentity DROP COLUMN scopes;

ALTER TABLE PUBLIC.temporalentityattrinstance DROP COLUMN temporalentity_id;
ALTER TABLE PUBLIC.temporalentityattrinstance DROP COLUMN attributetype;


--TEMPORAL FUNCTIONS

CREATE OR REPLACE FUNCTION addAttribValueFromTemp(attribName text, entityIid bigint, isRel boolean, isGeo boolean, isLang boolean, datasetId text, attrValue jsonb, rootLevel boolean, createdAt TIMESTAMP, modifiedAt TIMESTAMP, observedAt TIMESTAMP) RETURNS void AS $$
declare
	entry jsonb;
	entryKey text;
	entryValue jsonb;
	temp text;
	instance_Id text;
BEGIN
	FOR entry IN SELECT jsonb_array_elements FROM jsonb_array_elements(attrValue) LOOP
		instance_id = gen_random_uuid()::text;
		FOR entryKey IN SELECT jsonb_object_keys FROM jsonb_object_keys(entry) LOOP
			IF isGeo THEN
				IF entryKey = '@value' THEN
					INSERT INTO temporalentityattrinstance(attributeId, instanceId, geovalue, createdat, modifiedat, observedat, data, iid, deletedat, dataset_id, attr_value, is_rel, is_geo, is_lang, is_toplevel) VALUES (temp, instance_id, ST_SetSRID( ST_GeomFromGeoJSON( getGeoJson(entry->entryKey)::text ), 4326), createdAt, modifiedAt, observedAt, jsonb_set(entry, '{https://uri.etsi.org/ngsi-ld/instanceId}', ('[{"@id":"' || instance_id || '"}]')::jsonb), entityIid, null, datasetId, null, isRel, isGeo, isLang, rootLevel);
				END IF;
			ELSIF isLang THEN
				IF entryKey = '@value' THEN
					temp:= attribName || '[' || entry->>'@language' || ']';
					INSERT INTO temporalentityattrinstance(attributeId, instanceId, geovalue, createdat, modifiedat, observedat, data, iid, deletedat, dataset_id, attr_value, is_rel, is_geo, is_lang, is_toplevel) VALUES (temp, instance_id, null, createdAt, modifiedAt, observedAt, jsonb_set(entry, '{https://uri.etsi.org/ngsi-ld/instanceId}', ('[{"@id":"' || instance_id || '"}]')::jsonb), entityIid, null, datasetId, entry->entryKey, isRel, isGeo, isLang, rootLevel);
				END IF;
			ELSE
				temp := attribName;
				IF entryKey = '@value' OR entryKey = '@id' THEN
					IF NOT rootLevel THEN
						temp := temp || ']';
						INSERT INTO temporalentityattrinstance(attributeId, instanceId, geovalue, createdat, modifiedat, observedat, data, iid, deletedat, dataset_id, attr_value, is_rel, is_geo, is_lang, is_toplevel) VALUES (temp, null, null, createdAt, modifiedAt, observedAt, null, entityIid, null, datasetId, entry->entryKey, isRel, isGeo, isLang, rootLevel);
					ELSE
						INSERT INTO temporalentityattrinstance(attributeId, instanceId, geovalue, createdat, modifiedat, observedat, data, iid, deletedat, dataset_id, attr_value, is_rel, is_geo, is_lang, is_toplevel) VALUES (temp, instance_id, null, createdAt, modifiedAt, observedAt, jsonb_set(entry, '{https://uri.etsi.org/ngsi-ld/instanceId}', ('[{"@id":"' || instance_id || '"}]')::jsonb), entityIid, null, datasetId, entry->entryKey, isRel, isGeo, isLang, rootLevel);
					END IF;
				ELSE
					IF rootLevel THEN
						temp := temp || '[' || attribName;
					ELSE
						temp := temp || '.' || attribName;
					END IF;
					PERFORM addAttribValueFromTemp(temp,entityIid, isRel, isGeo, isLang, datasetId, entry->entryKey,false, createdAt, modifiedAt, observedAt);
				END IF;
			END IF;
		END LOOP;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION addAttribFromTemp(attribName text, attribValueArray jsonb, entityIid bigint, isToplevel boolean) RETURNS void AS $$
declare	
	tempJson jsonb;
	attrValue jsonb;
	datasetId text;
	subAttribName text;
	subAttribValue jsonb;
	isRel boolean;
	isGeo boolean;
	isLang boolean;
	attribType text;
	attribValue jsonb;
	createdAt jsonb;
	modifiedAt jsonb;
	observedAt jsonb;
BEGIN
	FOR attribValue IN SELECT jsonb_array_elements(attribValueArray) LOOP
		attribType := attribValue#>>'{@type,0}';
		datasetId := attribValue->>'https://uri.etsi.org/ngsi-ld/datasetId';
		tempJson := attribValue - '@type';
		tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/datasetId';
		tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/createdAt';
		tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/modifiedAt';
		IF attribType = 'https://uri.etsi.org/ngsi-ld/Relationship' THEN
			attrValue := attribValue#>'{https://uri.etsi.org/ngsi-ld/hasObject}';
			tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/hasObject';
			isRel := true;
			isGeo := false;
			isLang := false;
		ELSIF attribType = 'https://uri.etsi.org/ngsi-ld/LanguageProperty' THEN
			attrValue := attribValue#>'{https://uri.etsi.org/ngsi-ld/hasLanguageMap}';
			tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/hasLanguageMap';
			isRel := false;
			isGeo := false;
			isLang := true;
		ELSIF attribType = 'https://uri.etsi.org/ngsi-ld/GeoProperty' THEN
			attrValue := attribValue#>'{https://uri.etsi.org/ngsi-ld/hasValue}';
			tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/hasValue';
			isRel := false;
			isGeo := true;
			isLang := false;
		ELSE
			attrValue := attribValue#>'{https://uri.etsi.org/ngsi-ld/hasValue}';
			tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/hasValue';
			isRel := false;
			isGeo := false;
			isLang := false;
		END IF;
		createdAt := attribValue#>'{https://uri.etsi.org/ngsi-ld/createdAt,0,@value}';
		modifiedAt := attribValue#>'{https://uri.etsi.org/ngsi-ld/createdAt,0,@value}';
		IF attribValue ? 'https://uri.etsi.org/ngsi-ld/observedAt' THEN
			INSERT INTO temporalentityattrinstance(attributeId, instanceId, geovalue, createdat, modifiedat, observedat, data, iid, deletedat, dataset_id, attr_value, is_rel, is_geo, is_lang, is_toplevel) VALUES (attribName || '.https://uri.etsi.org/ngsi-ld/observedAt' , null, null, null, null, null, null, entityIid, null, datasetId, FALSE, FALSE, FALSE, datasetId, observedAt, false);
			tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/observedAt';
			observedAt := attribValue#>'{https://uri.etsi.org/ngsi-ld/observedAt,0,@value}';
		END IF;
		INSERT INTO temporalentityattrinstance(attributeId, instanceId, geovalue, createdat, modifiedat, observedat, data, iid, deletedat, dataset_id, attr_value, is_rel, is_geo, is_lang, is_toplevel) VALUES (attribName || '.https://uri.etsi.org/ngsi-ld/createdAt' , null, null, null, null, null, null, entityIid, null, datasetId, FALSE, FALSE, FALSE, datasetId, createdAt, false);
		INSERT INTO temporalentityattrinstance(attributeId, instanceId, geovalue, createdat, modifiedat, observedat, data, iid, deletedat, dataset_id, attr_value, is_rel, is_geo, is_lang, is_toplevel) VALUES (attribName || '.https://uri.etsi.org/ngsi-ld/modifiedAt' , null, null, null, null, null, null, entityIid, null, datasetId, FALSE, FALSE, FALSE, datasetId, modifiedAt, false);
		PERFORM addAttribValueFromTemp(attribName, entityIid, isRel, isGeo, isLang, datasetId, attrValue, isToplevel, createdAt::TIMESTAMP, modifiedAt::TIMESTAMP, observedAt::TIMESTAMP);
		FOR subAttribName IN SELECT jsonb_object_keys FROM jsonb_object_keys(tempJson) LOOP
			PERFORM addAttribFromTemp(attribName || "." || subAttribName, tempJson->subAttribName, entityIid, false);
		END LOOP;
	END LOOP;
    RETURN;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION NGSILD_CREATETEMPORALENTITY (INSERTENTITY JSONB) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB, C_ID text, singleOp boolean, batchOp boolean) AS $$
declare
	attribName text;
	templateEntity jsonb;
	entityType text;
	entityTypes jsonb;
	attribValue jsonb;
	i_rec record;
	countInt integer;
	removeAttrib boolean;
	entityIid bigint;
	entityId text;
	isRel boolean;
	isLang boolean;
	isGeo boolean;
	createdat timestamp without time zone;
	modifiedat timestamp without time zone;
	insertLocation GEOMETRY(Geometry, 4326);
	insertScopes text[];
	insertScope text;
	tempJson jsonb;
BEGIN
    CREATE TEMP TABLE resultTable (endPoint text, tenant text, headers jsonb, forwardEntity jsonb, c_id text UNIQUE, singleOp boolean, batchOp boolean) ON COMMIT DROP;
	templateEntity := '{}'::jsonb;
	templateEntity := jsonb_set(templateEntity, '{@id}', insertEntity->'@id');
	entityId := insertEntity->>'@id';
	insertEntity := insertEntity - '@id';
	templateEntity := jsonb_set(templateEntity, '{@type}', insertEntity->'@type');
	entityTypes := insertEntity->'@type';
	insertEntity := insertEntity - '@type';
	templateEntity := jsonb_set(templateEntity, '{https://uri.etsi.org/ngsi-ld/createdAt}', insertEntity->'https://uri.etsi.org/ngsi-ld/createdAt');
	createdat := insertEntity->'https://uri.etsi.org/ngsi-ld/createdAt'::TIMESTAMP;
	insertEntity := insertEntity - 'https://uri.etsi.org/ngsi-ld/createdAt';
	modifiedat := insertEntity->'https://uri.etsi.org/ngsi-ld/modifiedAt'::TIMESTAMP;
	templateEntity := jsonb_set(templateEntity, '{https://uri.etsi.org/ngsi-ld/modifiedAt}', insertEntity->'https://uri.etsi.org/ngsi-ld/modifiedAt');
	insertEntity := insertEntity - 'https://uri.etsi.org/ngsi-ld/modifiedAt';
	IF insertEntity ? 'https://uri.etsi.org/ngsi-ld/location' THEN
		IF (insertEntity@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
			insertLocation = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(insertEntity#>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0}')::text ), 4326);
		ELSE
			insertLocation = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(insertEntity#>'{https://uri.etsi.org/ngsi-ld/location,0}')::text ), 4326);
		END IF;
	ELSE
		insertLocation = null;
	END IF;
	IF (insertEntity ? 'https://uri.etsi.org/ngsi-ld/scope') THEN
		insertScopes = getScopes(insertEntity#>'{https://uri.etsi.org/ngsi-ld/scope}');
		insertEntity := insertEntity - 'https://uri.etsi.org/ngsi-ld/scope';
	ELSE
		insertScopes = NULL;
	END IF;
	entityIid := null;
	FOR entityType IN select jsonb_array_elements_text from jsonb_array_elements_text(entityTypes) LOOP
		FOR attribName IN select jsonb_object_keys from jsonb_object_keys(insertEntity)	LOOP
			attribValue := insertEntity->attribName;
			removeAttrib := false;
			IF attribValue->>'{0,@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship' THEN
				FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode, c.c_id FROM csourceinformation AS c WHERE c.reg_mode > 0 AND (c.upsertTemporal) AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL) AND (c.e_rel IS NULL OR c.e_rel = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
					INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity, i_rec.c_id, true, false) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
					IF i_rec.reg_mode > 1 THEN
						removeAttrib := true;
						IF i_rec.reg_mode = 3 THEN
							EXIT;
						END IF;
					END IF;
				END LOOP;
			ELSE
				FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode, c.c_id FROM csourceinformation AS c WHERE c.reg_mode > 0 AND (c.upsertTemporal) AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_rel IS NULL) AND (c.e_prop IS NULL OR c.e_prop = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
					INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity, i_rec.c_id, true, false) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
					IF i_rec.reg_mode > 1 THEN
						removeAttrib := true;
						IF i_rec.reg_mode = 3 THEN
							EXIT;
						END IF;
					END IF;
				END LOOP;
			END IF;
			IF NOT removeAttrib THEN
				IF entityIid is null THEN
					BEGIN
						INSERT INTO TEMPORALENTITY(e_id, modifiedat, createdat) VALUES (entityId, modifiedAt, createdAt);
					EXCEPTION WHEN OTHERS THEN
						UPDATE TEMPORALENTITY SET modifiedat=modifiedAt WHERE e_id = entityId;
						INSERT INTO resultTable(endpoint) VALUES ('UPDATED');
					END;
					SELECT id FROM TEMPORALENTITY WHERE E_ID = entityId INTO entityIid;
					IF insertScopes is not null THEN
						FOREACH insertScope IN ARRAY insertScopes LOOP
							INSERT INTO tempescope2iid VALUES (insertScope, entityIid);
						END LOOP;
					END IF;
				END IF;
				PERFORM addAttribFromTemp(attribName, attribValue, entityIid, true);
			ELSE
				insertEntity := insertEntity - attribName;
			END IF;
		END LOOP;
	END LOOP;
	
	SELECT count(jsonb_object_keys) FROM jsonb_object_keys(insertEntity) INTO countInt;
	IF countInt > 0 THEN
		SELECT insertEntity || templateEntity INTO insertEntity;
		INSERT INTO resultTable VALUES ('ADDED ENTITY', null, null, insertEntity, 'ADDED ENTITY', false, false);
	END IF;
	RETURN QUERY SELECT * FROM resultTable;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION NGSILD_DELETETEMPENTITY (ENTITYID text) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB, C_ID text, singleOp boolean, batchOp boolean) AS $$
declare
	LOCALENTITY jsonb;
	IID bigint;
	i_rec record;
BEGIN
	CREATE TEMP TABLE resultTable (endPoint text, tenant text, headers jsonb, forwardEntity jsonb, c_id text UNIQUE, singleOp boolean, batchOp boolean) ON COMMIT DROP;
	FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode, c.c_id FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.deleteTemporal AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') ORDER BY c.reg_mode DESC LOOP
		INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, NULL, i_rec.c_id, true, false) ON CONFLICT DO NOTHING;
	END LOOP;
	--very expensive potentially
	with a as (select id, ('{"@id": "'||e_id||'", "https://uri.etsi.org/ngsi-ld/createdAt": [{"@type": "https://uri.etsi.org/ngsi-ld/DateTime", "@value": "'|| createdat||'"}], "https://uri.etsi.org/ngsi-ld/modifiedAt": [{"@type": "https://uri.etsi.org/ngsi-ld/DateTime", "@value": "'|| modifiedat||'"}]}')::jsonb as entity from temporalentity where e_id =ENTITYID),
	b as (select '@type' as key, jsonb_agg(e_type) as value from a left join tempetype2iid on a.id = tempetype2iid.iid group by a.entity),
	c as (select attributeId as key, jsonb_agg(data) as value from a left join temporalentityattrinstance on a.id = temporalentityattrinstance.iid group by attributeId),
	d as (select jsonb_object_agg(c.key, c.value) as attrs FROM c),
	e as (select 'htttpscope' as key, jsonb_agg(jsonb_build_object('@id', e_scope)) as value from b, a left join tempescope2iid on a.id = tempescope2iid.iid where e_scope is not null)
	select jsonb_build_object(b.key, b.value, e.key, e.value) || d.attrs || a.entity from a, b, d, e into localentity;
	IF LOCALENTITY IS NOT NULL THEN
		BEGIN
			DELETE FROM TEMPORALENTITY WHERE TEMPORALENTITY.id = IID;
			INSERT INTO resultTable VALUES ('DELETED ENTITY', NULL, NULL, LOCALENTITY, 'DELETED ENTITY', false, false);
		EXCEPTION WHEN OTHERS THEN
			INSERT INTO resultTable VALUES ('ERROR', sqlstate::text, SQLERRM::text, null, 'ERROR', false, false);
		END;
	END IF;
	RETURN QUERY SELECT * FROM resultTable;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION NGSILD_APPENDTOTEMPORALENTITY(ENTITYID TEXT, ENTITY JSONB) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB, C_ID text, singleOp boolean, batchOp boolean) AS $$
declare
	LOCALENTITY jsonb;
	IID bigint;
	i_rec record;
BEGIN
	CREATE TEMP TABLE resultTable (endPoint text, tenant text, headers jsonb, forwardEntity jsonb, c_id text UNIQUE, singleOp boolean, batchOp boolean) ON COMMIT DROP;
	FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode, c.c_id FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.deleteTemporal AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') ORDER BY c.reg_mode DESC LOOP
		INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, NULL, i_rec.c_id, true, false) ON CONFLICT DO NOTHING;
	END LOOP;
	RETURN QUERY SELECT * FROM resultTable;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION NGSILD_UPDATETEMPORALATTRINSTANCE(ENTITYID TEXT, ATTRIBID TEXT, INSTANCEID TEXT, ATTRIB JSONB) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB, C_ID text, singleOp boolean, batchOp boolean) AS $$
declare
	LOCALENTITY jsonb;
	IID bigint;
	i_rec record;
BEGIN
	CREATE TEMP TABLE resultTable (endPoint text, tenant text, headers jsonb, forwardEntity jsonb, c_id text UNIQUE, singleOp boolean, batchOp boolean) ON COMMIT DROP;
	FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode, c.c_id FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.deleteTemporal AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') ORDER BY c.reg_mode DESC LOOP
		INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, NULL, i_rec.c_id, true, false) ON CONFLICT DO NOTHING;
	END LOOP;
	--todo reject changes in property type???
	select id from temporalentity where e_id =ENTITYID INTO IID;
	UPDATE temporalentityattrinstance SET data = jsonb_set(ATTRIB, '{https://uri.etsi.org/ngsi-ld/createdAt}', '[{"@type": "https://uri.etsi.org/ngsi-ld/DateTime", "@value": "'|| createdat ||'"}]'::jsonb), modifiedat = (ATTRIB#>'{https://uri.etsi.org/ngsi-ld/modifiedAt,0,@value}')::TIMESTAMP, observedat = (ATTRIB#>'{https://uri.etsi.org/ngsi-ld/observedAt,0,@value}')::TIMESTAMP WHERE iid = IID AND attributeId = attribid AND instanceId = INSTANCEID RETURNING data INTO LOCALENTITY;
	IF LOCALENTITY IS NOT NULL THEN
		INSERT INTO resultTable VALUES ('UPDATED ATTRS', null, null, LOCALENTITY, null, false, false);
	END IF;
	RETURN QUERY SELECT * FROM resultTable;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION NGSILD_DELETEATTRFROMTEMPENTITY(ENTITYID TEXT, ATTRIBID TEXT, DATASETID TEXT, DELETEALL BOOLEAN) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB, C_ID text, singleOp boolean, batchOp boolean) AS $$
declare
	LOCALATTRS jsonb;
	IID bigint;
	i_rec record;
BEGIN
	CREATE TEMP TABLE resultTable (endPoint text, tenant text, headers jsonb, forwardEntity jsonb, c_id text UNIQUE, singleOp boolean, batchOp boolean) ON COMMIT DROP;
	FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode, c.c_id FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.deleteAttrsTemporal AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop is null or c.e_prop = ATTRIBID) AND (c.e_rel is null or c.e_rel = ATTRIBID) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') ORDER BY c.reg_mode DESC LOOP
		INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, NULL, i_rec.c_id, true, false) ON CONFLICT DO NOTHING;
	END LOOP;
	IF DELETEALL THEN
		--returning all might be very expensive ... 
		with a as (select id from temporalentity where e_id =ENTITYID),
		with c as (select attributeId as key, jsonb_agg(data) as value from a left join temporalentityattrinstance on a.id = temporalentityattrinstance.iid WHERE temporalentityattrinstance.attributeId = ATTRIBID group by temporalentityattrinstance.attributeId),
		select jsonb_object_agg(c.key, c.value), a.id FROM c, a INTO LOCALATTRS, IID;
		IF LOCALATTRS IS NOT NULL THEN
			INSERT INTO resultTable VALUES ('DELETED ATTRS', null, null, LOCALATTRS, null, false, false);
			DELETE FROM temporalentityattrinstance WHERE temporalentityattrinstance.attributeId = ATTRIBID AND temporalentityattrinstance.iid = IID;
		END IF;
	ELSE
		with a as (select id from temporalentity where e_id =ENTITYID),
		with c as (select attributeId as key, jsonb_agg(data) as value from a left join temporalentityattrinstance on a.id = temporalentityattrinstance.iid WHERE temporalentityattrinstance.attributeId = ATTRIBID AND temporalentityattrinstance.attributeId = DATASETID group by temporalentityattrinstance.attributeId),
		select jsonb_object_agg(c.key, c.value), a.id FROM c, a INTO LOCALATTRS, IID;
		IF LOCALATTRS IS NOT NULL THEN
			INSERT INTO resultTable VALUES ('DELETED ATTRS', null, null, LOCALATTRS, null, false, false);
			DELETE FROM temporalentityattrinstance WHERE temporalentityattrinstance.attributeId = ATTRIBID AND temporalentityattrinstance.iid = IID;
		END IF;
	END IF;
	RETURN QUERY SELECT * FROM resultTable;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION NGSILD_DELETEATTRSTNSTANCEFROMTEMPENTITY(ENTITYID TEXT, ATTRIBID TEXT, INSTANCEID TEXT) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB, C_ID text, singleOp boolean, batchOp boolean) AS $$
declare
	LOCALINSTANCE jsonb;
	IID bigint;
	i_rec record;
BEGIN
	CREATE TEMP TABLE resultTable (endPoint text, tenant text, headers jsonb, forwardEntity jsonb, c_id text UNIQUE, singleOp boolean, batchOp boolean) ON COMMIT DROP;
	FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode, c.c_id FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.deleteAttrsTemporal AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop is null or c.e_prop = ATTRIBID) AND (c.e_rel is null or c.e_rel = ATTRIBID) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') ORDER BY c.reg_mode DESC LOOP
		INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, NULL, i_rec.c_id, true, false) ON CONFLICT DO NOTHING;
	END LOOP;
	delete from temporalentityattrinstance where temporalentityattrinstance.instanceId = INSTANCEID AND temporalentityattrinstance.attributeId = ATTRIBID RETURNING data INTO LOCALINSTANCE;
	IF LOCALINSTANCE IS NOT NULL THEN
		INSERT INTO resultTable VALUES ('DELETED ATTRS', null, null, LOCALATTRS, null, false, false);
	END IF;
	RETURN QUERY SELECT * FROM resultTable;
END;
$$ LANGUAGE PLPGSQL;
