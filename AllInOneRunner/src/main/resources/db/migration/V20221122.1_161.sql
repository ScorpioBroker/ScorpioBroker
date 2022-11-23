DROP TABLE csourceinformation;

Alter table public.csource DROP COLUMN "location",DROP COLUMN "name", DROP COLUMN  endpoint,DROP COLUMN  description,DROP COLUMN  timestamp_end,DROP COLUMN  timestamp_start,DROP COLUMN  tenant_id,DROP COLUMN  internal,DROP COLUMN  has_registrationinfo_with_attrs_only,DROP COLUMN  has_registrationinfo_with_entityinfo_only,DROP COLUMN  data_without_sysattrs,DROP COLUMN  scopes, DROP COLUMN  expires, DROP COLUMN type;

ALTER TABLE PUBLIC.CSOURCE RENAME COLUMN data TO REG;

alter table public.csource rename column id to c_id;

ALTER TABLE PUBLIC.CSOURCE DROP CONSTRAINT csource_pkey;

ALTER TABLE IF EXISTS public.csource
    ADD CONSTRAINT unique_c_id UNIQUE (c_id);

ALTER TABLE IF EXISTS public.csource
    ADD COLUMN id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 );
	
ALTER TABLE public.csource ADD PRIMARY KEY (id);

CREATE INDEX i_csource_c_id
    ON public.csource USING hash
    (c_id text_pattern_ops);

CREATE INDEX i_csource_id
    ON public.csource USING btree
    (id);


CREATE TABLE public.csourceinformation(
	id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 ),
	cs_id bigint,
	e_id text,
	e_id_p text,
	e_type text,
	e_prop text,
	e_rel text,
	i_location GEOMETRY(Geometry, 4326),
	scopes text[],
	expires timestamp without time zone,
	endpoint text,
	tenant_id text,
	headers jsonb,
	reg_mode smallint,
	createEntity boolean,
	updateEntity boolean,
	appendAttrs boolean,
	updateAttrs boolean,
	deleteAttrs boolean,
	deleteEntity boolean,
	createBatch boolean,
	upsertBatch boolean,
	updateBatch boolean,
	deleteBatch boolean,
	upsertTemporal boolean,
	appendAttrsTemporal boolean,
	deleteAttrsTemporal boolean,
	updateAttrsTemporal boolean,
	deleteAttrInstanceTemporal boolean,
	deleteTemporal boolean,
	mergeEntity boolean,
	replaceEntity boolean,
	replaceAttrs boolean,
	mergeBatch boolean,
	retrieveEntity boolean,
	queryEntity boolean,
	queryBatch boolean,
	retrieveTemporal boolean,
	queryTemporal boolean,
	retrieveEntityTypes boolean,
	retrieveEntityTypeDetails boolean,
	retrieveEntityTypeInfo boolean,
	retrieveAttrTypes boolean,
	retrieveAttrTypeDetails boolean,
	retrieveAttrTypeInfo boolean,
	createSubscription boolean,
	updateSubscription boolean,
	retrieveSubscription boolean,
	querySubscription boolean,
	deleteSubscription boolean,
	CONSTRAINT id_pkey PRIMARY KEY (id),
	CONSTRAINT cs_id_fkey FOREIGN KEY (cs_id)
    REFERENCES public.csource (id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
);


CREATE INDEX IF NOT EXISTS fki_cs_id_fkey
    ON public.csourceinformation(cs_id);
	
CREATE INDEX i_csourceinformation_e_type
    ON public.csourceinformation USING hash
    (e_type text_pattern_ops);

CREATE INDEX i_csourceinformation_e_rel
    ON public.csourceinformation USING hash
    (e_rel text_pattern_ops);

CREATE INDEX i_csourceinformation_e_prop
    ON public.csourceinformation USING hash
    (e_prop text_pattern_ops);
	
CREATE INDEX i_csourceinformation_e_id
    ON public.csourceinformation USING hash
    (e_id text_pattern_ops);
	
CREATE INDEX i_csourceinformation_i_location
    ON public.csourceinformation USING gist
    (i_location gist_geometry_ops_nd);
	
DROP FUNCTION public.csource_extract_jsonb_fields_to_information_table cascade;

DROP Trigger csource_extract_jsonb_fields ON csource;


CREATE OR REPLACE FUNCTION CSOURCE_EXTRACT_JSONB_FIELDS() RETURNS TRIGGER AS $_$
DECLARE
BEGIN
    NEW.C_ID = NEW.REG#>>'{@id}';
	RETURN NEW;
END;
$_$ LANGUAGE PLPGSQL;

CREATE TRIGGER csource_extract_jsonb_fields BEFORE INSERT ON csource
    FOR EACH ROW EXECUTE PROCEDURE csource_extract_jsonb_fields();

CREATE OR REPLACE FUNCTION CSOURCEINFORMATION_EXTRACT_JSONB_FIELDS() RETURNS TRIGGER AS $_$
DECLARE
    infoEntry jsonb;
	entitiesEntry jsonb;
    entityType text;
	entityId text;
	entityIdPattern text;
	attribsAdded boolean;
	location GEOMETRY(Geometry, 4326);
	scopes text[];
	tenant text;
	regMode smallint;
	operations boolean[];
	endpoint text;
	expires timestamp without time zone;
	headers jsonb;	
	internalId bigint;
	attribName text;
	errorFound boolean;
BEGIN
    IF (TG_OP = 'INSERT' AND NEW.REG IS NOT NULL) OR
        (TG_OP = 'UPDATE' AND OLD.REG IS NULL AND NEW.REG IS NOT NULL) OR
        (TG_OP = 'UPDATE' AND OLD.REG IS NOT NULL AND NEW.REG IS NULL) OR
        (TG_OP = 'UPDATE' AND OLD.REG IS NOT NULL AND NEW.REG IS NOT NULL AND OLD.REG <> NEW.REG) THEN
		errorFound := false;		
		internalId = NEW.id;
		endpoint = NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/endpoint,0,@value}';
		IF NEW.REG ? 'https://uri.etsi.org/ngsi-ld/location' THEN
			IF (NEW.REG@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
				location = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson( NEW.REG#>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0}')::text ), 4326);
			ELSE
				location = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/location,0}')::text ), 4326);
			END IF;
		ELSE
			location = null;
		END IF;
		
		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/scope') THEN
			scopes = getScopes(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/scope}');
		ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/scope') THEN
			scopes = getScopes(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/default-context/scope}');
		ELSE
			scopes = NULL;
		END IF;
		
		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/tenant') THEN
			tenant = NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/tenant,0,@value}';
		ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/tenant') THEN
			tenant = NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/default-context/tenant,0,@value}';
		ELSE
			tenant = NULL;
		END IF;
		
		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/operations') THEN
			operations = getOperations(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/operations}');
		ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/operations') THEN
			operations = getOperations(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/default-context/operations}');
		ELSE
			operations = array[false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true]::boolean[];
		END IF;

		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/contextSourceInfo') THEN
			headers = NEW.REG#>'{https://uri.etsi.org/ngsi-ld/contextSourceInfo}';
		ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/contextSourceInfo') THEN
			headers = NEW.REG#>'{https://uri.etsi.org/ngsi-ld/default-context/contextSourceInfo}';
		ELSE
			headers = NULL;
		END IF;

		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/mode') THEN
			regMode = getMode(NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/mode,0,@value}');
		ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/mode') THEN
			regMode = getMode(NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/default-context/mode,0,@value}');
		ELSE
			regMode = 1;
		END IF;
		
		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/expires') THEN
			expires = (NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/expires,0,@value}')::TIMESTAMP;
		ELSE
			expires = NULL;
		END IF;
		BEGIN
			IF TG_OP = 'UPDATE' THEN
				DELETE FROM csourceinformation where cs_id = NEW.id;
			END IF;
			FOR infoEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/information}') LOOP
				IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/entities' THEN
					FOR entitiesEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/entities}') LOOP
						FOR entityType IN SELECT jsonb_array_elements_text FROM jsonb_array_elements_text(entitiesEntry#>'{@type}') LOOP
							entityId := NULL;
							entityIdPattern := NULL;
							attribsAdded := false;
							IF entitiesEntry ? '@id' THEN
								entityId = entitiesEntry#>>'{@id}';
							END IF;
							IF entitiesEntry ? 'https://uri.etsi.org/ngsi-ld/idPattern' THEN
								entityIdPattern = entitiesEntry#>>'{https://uri.etsi.org/ngsi-ld/idPattern,0,@value}';
							END IF;
							IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/propertyNames' THEN
								attribsAdded = true;
								FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/propertyNames}') LOOP
									IF regMode > 1 THEN
										IF entityId IS NOT NULL THEN 
											WITH iids AS (SELECT id FROM ENTITY, etype2iid WHERE e_id = entityId AND ENTITY.id = etype2iid.iid AND etype2iid.e_type = entityType) SELECT count(attr2iid.iid)>0 INTO errorFound FROM iids left join attr2iid on iids.iid = attr2iid.iid WHERE attr2iid.attr = attribName AND NOT attr2iid.is_rel;
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and id % conflicts with existing entry', attribName, entityId USING ERRCODE='23514';
											END IF;
										ELSIF entityIdPattern IS NOT NULL THEN
											WITH iids AS (SELECT id FROM ENTITY, etype2iid WHERE e_id ~ entityIdPattern AND ENTITY.id = etype2iid.iid AND etype2iid.e_type = entityType) SELECT count(attr2iid.iid)>0 INTO errorFound FROM iids left join attr2iid on iids.iid = attr2iid.iid WHERE attr2iid.attr = attribName AND NOT attr2iid.is_rel;
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and idpattern % conflicts with existing entry', attribName, entityIdPattern USING ERRCODE='23514';
											END IF;
										ELSE
											WITH iids AS (SELECT iid FROM etype2iid WHERE e_type = entityType) SELECT count(attr2iid.iid)>0 INTO errorFound FROM iids left join attr2iid on iids.iid = attr2iid.iid WHERE attr2iid.attr = attribName AND NOT attr2iid.is_rel;
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and type % conflicts with existing entry', attribName, entityType USING ERRCODE='23514';
											END IF;
										END IF;
									END IF;
									INSERT INTO csourceinformation (cs_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription) VALUES (internalId, entityId, entityIdPattern, entityType, attribName, NULL, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36]);
								END LOOP;
							END IF;
							IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/relationshipNames' THEN
								attribsAdded = true;
								FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/relationshipNames}') LOOP
									IF regMode > 1 THEN
										IF entityId IS NOT NULL THEN 
											WITH iids AS (SELECT id FROM ENTITY, etype2iid WHERE e_id = entityId AND ENTITY.id = etype2iid.iid AND etype2iid.e_type = entityType) SELECT count(attr2iid.iid)>0 INTO errorFound FROM iids left join attr2iid on iids.iid = attr2iid.iid WHERE attr2iid.attr = attribName AND attr2iid.is_rel;
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and id % conflicts with existing entry', attribName, entityId USING ERRCODE='23514';
											END IF;
										ELSIF entityIdPattern IS NOT NULL THEN
											WITH iids AS (SELECT id FROM ENTITY, etype2iid WHERE e_id ~ entityIdPattern AND ENTITY.id = etype2iid.iid AND etype2iid.e_type = entityType) SELECT count(attr2iid.iid)>0 INTO errorFound FROM iids left join attr2iid on iids.iid = attr2iid.iid WHERE attr2iid.attr = attribName AND attr2iid.is_rel;
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and idpattern % conflicts with existing entry', attribName, entityIdPattern USING ERRCODE='23514';
											END IF;
										ELSE
											WITH iids AS (SELECT iid FROM etype2iid WHERE e_type = entityType) SELECT count(attr2iid.iid)>0 INTO errorFound FROM iids left join attr2iid on iids.iid = attr2iid.iid WHERE attr2iid.attr = attribName AND attr2iid.is_rel;
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and type % conflicts with existing entry', attribName, entityType USING ERRCODE='23514';
											END IF;
										END IF;
									END IF;
									INSERT INTO csourceinformation (cs_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription) VALUES (internalId, entityId, entityIdPattern, entityType,NULL, attribName, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36]);
									
								END LOOP;
							END IF;
							IF NOT attribsAdded THEN
								IF regMode > 1 THEN
									IF entityId IS NOT NULL THEN 
										WITH e_ids AS (SELECT id FROM entity WHERE e_id = entityId) SELECT count(iid) INTO errorFound FROM etype2iid  WHERE etype2iid.e_type = entityType;
										IF errorFound THEN
											RAISE EXCEPTION 'Registration with entityId % conflicts with existing entity', entityId USING ERRCODE='23514';
										END IF;
									ELSIF entityIdPattern IS NOT NULL THEN
										WITH e_ids AS (SELECT id FROM entity WHERE e_id ~ entityIdPattern) SELECT count(iid)>0 INTO errorFound FROM etype2iid LEFT JOIN e_ids ON etype2iid.iid = e_ids.id WHERE etype2iid.e_type = entityType;
										IF errorFound THEN
											RAISE EXCEPTION 'Registration with idPattern % and type % conflicts with existing entity', entityIdPattern, entityType USING ERRCODE='23514';
										END IF;
									ELSE
										SELECT count(iid)>0 INTO errorFound FROM etype2iid WHERE e_type = entityType;
										IF errorFound THEN
											RAISE EXCEPTION 'Registration with type % conflicts with existing entity', entityType USING ERRCODE='23514';
										END IF;
									END IF;
								END IF;
								INSERT INTO csourceinformation(cs_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription) values (internalId, entityId, entityIdPattern, entityType, NULL, NULL, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36]);
							END IF;
						END LOOP;
					END LOOP;
				ELSE
					IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/propertyNames' THEN
						FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/propertyNames}') LOOP
							SELECT count(attr2iid.iid)>0 INTO errorFound FROM attr2iid WHERE attr2iid.attr = attribName AND NOT attr2iid.is_rel;
							IF regMode > 1 AND errorFound THEN
								RAISE EXCEPTION 'Attribute % conflicts with existing entity', attribName USING ERRCODE='23514';
							END IF;
							INSERT INTO csourceinformation(cs_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription) VALUES (internalId, NULL, NULL, NULL, attribName, NULL, location, scopes, expires, endpoint, tenant, headers, regMode, operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36]);
						END LOOP;
					END IF;
					IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/relationshipNames' THEN
						FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/relationshipNames}') LOOP
							SELECT count(attr2iid.iid)>0 INTO errorFound FROM attr2iid WHERE attr2iid.attr = attribName AND attr2iid.is_rel;
							IF regMode > 1 AND errorFound THEN
								RAISE EXCEPTION 'Attribute % conflicts with existing entity', attribName USING ERRCODE='23514';
							END IF;
							INSERT INTO csourceinformation(cs_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription) VALUES (internalId, NULL, NULL, NULL, NULL, attribName, location, scopes, expires, endpoint, tenant, headers, regMode, operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36]);
						END LOOP;
					END IF;
				END IF;
			END LOOP;
		END;
	END IF;
    RETURN NEW;
END;
$_$ LANGUAGE PLPGSQL;

CREATE TRIGGER csource_extract_jsonb_fields_to_information_table AFTER INSERT OR UPDATE ON csource
    FOR EACH ROW EXECUTE PROCEDURE CSOURCEINFORMATION_EXTRACT_JSONB_FIELDS();	

ALTER TABLE PUBLIC.ENTITY RENAME COLUMN DATA TO ENTITY;

ALTER TABLE PUBLIC.ENTITY DROP COLUMN DATA_WITHOUT_SYSATTRS;

ALTER TABLE PUBLIC.ENTITY DROP COLUMN KVDATA;

ALTER TABLE PUBLIC.ENTITY DROP CONSTRAINT entity_pkey;

ALTER TABLE PUBLIC.ENTITY RENAME COLUMN id TO E_ID;

ALTER TABLE IF EXISTS public.entity
    ADD CONSTRAINT unique_e_id UNIQUE (E_ID);
	
CREATE INDEX i_entity_id
    ON public.entity USING hash
    (E_ID text_pattern_ops);



ALTER TABLE IF EXISTS public.entity
    ADD COLUMN id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 );
	
ALTER TABLE public.entity ADD PRIMARY KEY (id);



CREATE TABLE public.etype2iid
(
    e_type text,
    iid bigint,
    CONSTRAINT "prKey" PRIMARY KEY (e_type, iid)
);

CREATE TABLE public.attr2iid
(
    attr text,
    iid bigint,
	is_rel boolean,
	dataset_id text,
	attr_value jsonb,
	created timestamp without time zone,
	observed timestamp without time zone,
	modified timestamp without time zone
);

ALTER TABLE IF EXISTS public.attr2iid
    ADD CONSTRAINT iid_fkey FOREIGN KEY (iid)
    REFERENCES public.entity (id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE;
	
CREATE INDEX t_index
    ON public.etype2iid USING hash
    (e_type text_pattern_ops);
	
ALTER TABLE IF EXISTS public.etype2iid
    ADD CONSTRAINT iid_fkey FOREIGN KEY (iid)
    REFERENCES public.entity (id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS fki_iid_fkey
    ON public.etype2iid(iid);

INSERT INTO public.etype2iid SELECT type, id FROM public.entity;

ALTER TABLE PUBLIC.ENTITY DROP COLUMN type;

CREATE OR REPLACE FUNCTION GETMODE (MODETEXT text) RETURNS smallint AS $registry_mode$
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
$registry_mode$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION GETOPERATIONS (OPERATIONJSON JSONB) RETURNS boolean[] AS $operations$
declare
	operations boolean[];
	operationEntry jsonb;
BEGIN
	operations = array[false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true]::boolean[];
	FOR operationEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(OPERATIONJSON) LOOP
		CASE operationEntry#>>'{@value}'
			WHEN 'createEntity' THEN
				operations[1] = true;
			WHEN 'updateEntity' THEN
				operations[2] = true;
			WHEN 'appendAttrs' THEN
				operations[3] = true;
			WHEN 'updateAttrs' THEN
				operations[4] = true;
			WHEN 'deleteAttrs' THEN
				operations[5] = true;
			WHEN 'deleteEntity' THEN
				operations[6] = true;
			WHEN 'createBatch' THEN
				operations[7] = true;
			WHEN 'upsertBatch' THEN
				operations[8] = true;
			WHEN 'updateBatch' THEN
				operations[9] = true;
			WHEN 'deleteBatch' THEN
				operations[10] = true;
			WHEN 'upsertTemporal' THEN
				operations[11] = true;
			WHEN 'appendAttrsTemporal' THEN
				operations[12] = true;
			WHEN 'deleteAttrsTemporal' THEN
				operations[13] = true;
			WHEN 'updateAttrsTemporal' THEN
				operations[14] = true;
			WHEN 'deleteAttrInstanceTemporal' THEN
				operations[15] = true;
			WHEN 'deleteTemporal' THEN
				operations[16] = true;
			WHEN 'mergeEntity' THEN
				operations[17] = true;
			WHEN 'replaceEntity' THEN
				operations[18] = true;
			WHEN 'replaceAttrs' THEN
				operations[19] = true;
			WHEN 'mergeBatch' THEN
				operations[20] = true;
			WHEN 'retrieveEntity' THEN
				operations[21] = true;
			WHEN 'queryEntity' THEN
				operations[22] = true;
			WHEN 'queryBatch' THEN
				operations[23] = true;
			WHEN 'retrieveTemporal' THEN
				operations[24] = true;
			WHEN 'queryTemporal' THEN
				operations[25] = true;
			WHEN 'retrieveEntityTypes' THEN
				operations[26] = true;
			WHEN 'retrieveEntityTypeDetails' THEN
				operations[27] = true;
			WHEN 'retrieveEntityTypeInfo' THEN
				operations[28] = true;
			WHEN 'retrieveAttrTypes' THEN
				operations[29] = true;
			WHEN 'retrieveAttrTypeDetails' THEN
				operations[30] = true;
			WHEN 'retrieveAttrTypeInfo' THEN
				operations[31] = true;
			WHEN 'createSubscription' THEN
				operations[32] = true;
			WHEN 'updateSubscription' THEN
				operations[33] = true;
			WHEN 'retrieveSubscription' THEN
				operations[34] = true;
			WHEN 'querySubscription' THEN
				operations[35] = true;
			WHEN 'deleteSubscription' THEN
				operations[36] = true;
		END CASE;
	END LOOP;
	RETURN operations;
END;
$operations$ LANGUAGE PLPGSQL;






CREATE OR REPLACE FUNCTION addAttrib(attribName text, attribValue jsonb, entityIid bigint) RETURNS void AS $$
declare	
	tempJson jsonb;
	attrValue jsonb;
	datasetId text;
	created timestamp without time zone;
	observed timestamp without time zone;
	modified timestamp without time zone;
	subAttribName text;
	subAttribValue jsonb;
	isRel boolean;
BEGIN
	IF attribValue#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship' THEN
		attrValue := attribValue#>'{https://uri.etsi.org/ngsi-ld/hasObject}';
		isRel := true;
	ELSE
		attrValue := attribValue#>'{https://uri.etsi.org/ngsi-ld/hasValue}';
		isRel := false;
	END IF;
	datasetId := attribValue->'https://uri.etsi.org/ngsi-ld/datasetId';
	created := (attribValue#>>'{https://uri.etsi.org/ngsi-ld/createdAt,0,@value}')::TIMESTAMP;
	observed := (attribValue#>>'{https://uri.etsi.org/ngsi-ld/observedAt,0,@value}')::TIMESTAMP;
	modified := (attribValue#>>'{https://uri.etsi.org/ngsi-ld/modifiedAt,0,@value}')::TIMESTAMP;
	INSERT INTO attr2iid VALUES (attribName, entityIid, isRel, datasetId, attrValue, created, observed, modified);
	tempJson := attribValue - '@type';
	tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/datasetId';
	tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/createdAt';
	tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/observedAt';
	tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/modifiedAt';
	tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/hasObject';
	tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/hasValue';
	FOR subAttribName IN SELECT jsonb_object_keys FROM jsonb_object_keys(tempJson) LOOP
		FOR subAttribValue IN SELECT jsonb_array_elements FROM jsonb_array_elements(tempJson->subAttribName) LOOP
			PERFORM addAttrib(attribName || "." || subAttribName, subAttribValue);
		END LOOP;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION addAttribs(entity_iid bigint, entity jsonb) RETURNS void AS $$
declare
	attribName text;
	attribValueEntry jsonb;
	tempJson jsonb;
BEGIN
	tempJson := entity - '@id';
	tempJson := tempJson - '@type';
	tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/createdAt';
	tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/observedAt';
	tempJson := tempJson - 'https://uri.etsi.org/ngsi-ld/modifiedAt';
	FOR attribName IN SELECT jsonb_object_keys FROM jsonb_object_keys(tempJson) LOOP
		FOR attribValueEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(tempJson->attribName) LOOP
			PERFORM addAttrib(attribName, attribValueEntry, entity_iid);
		END LOOP;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION public.entity_extract_jsonb_fields() RETURNS trigger LANGUAGE plpgsql AS $function$
	DECLARE
		eUpdate boolean;
		tempOld jsonb;
		tempNew jsonb;
	BEGIN
		NEW.e_id = NEW.ENTITY->>'@id';
		NEW.createdat = (NEW.ENTITY#>>'{https://uri.etsi.org/ngsi-ld/createdAt,0,@value}')::TIMESTAMP;
		NEW.modifiedat = (NEW.ENTITY#>>'{https://uri.etsi.org/ngsi-ld/modifiedAt,0,@value}')::TIMESTAMP;
		eUpdate := TG_OP = 'UPDATE';
		tempNew := NEW.ENTITY->'https://uri.etsi.org/ngsi-ld/location';
		tempOld := NULL;
		IF eUpdate THEN
			tempOld := OLD.ENTITY->'https://uri.etsi.org/ngsi-ld/location';
		END IF;
		IF tempNew IS NOT NULL AND (NOT eUpdate OR tempOld IS NULL OR tempOld<>tempNew) THEN
			NEW.location = ST_SetSRID( ST_GeomFromGeoJSON( getGeoJson(tempNew#>'{0,https://uri.etsi.org/ngsi-ld/hasValue,0}')::text ), 4326);
		ELSE 
			NEW.location = NULL;
		END IF;
		tempNew := NEW.ENTITY->'https://uri.etsi.org/ngsi-ld/observationSpace';
		tempOld := NULL;
		IF eUpdate THEN
			tempOld := OLD.ENTITY->'https://uri.etsi.org/ngsi-ld/observationSpace';
		END IF;
		
		IF tempNew IS NOT NULL AND (NOT eUpdate OR tempOld IS NULL OR tempOld<>tempNew) THEN
			NEW.observationSpace = ST_SetSRID( ST_GeomFromGeoJSON( getGeoJson(tempNew#>'{0,https://uri.etsi.org/ngsi-ld/hasValue,0}')::text ), 4326);
		ELSE 
			NEW.observationSpace = NULL;
		END IF;
		
		tempNew := NEW.ENTITY->'https://uri.etsi.org/ngsi-ld/operationSpace';
		tempOld := NULL;
		IF eUpdate THEN
			tempOld := OLD.ENTITY->'https://uri.etsi.org/ngsi-ld/operationSpace';
		END IF;
		IF tempNew IS NOT NULL AND (NOT eUpdate OR tempOld IS NULL OR tempOld<>tempNew) THEN
			NEW.operationSpace = ST_SetSRID( ST_GeomFromGeoJSON( getGeoJson(tempNew#>'{0,https://uri.etsi.org/ngsi-ld/hasValue,0}')::text ), 4326);
		ELSE
			NEW.operationSpace = NULL;
		END IF;
		
		tempNew := NEW.ENTITY->'https://uri.etsi.org/ngsi-ld/scope';
		tempOld := NULL;
		IF eUpdate THEN
			tempOld := OLD.ENTITY->'https://uri.etsi.org/ngsi-ld/scope';
		END IF;
		IF tempNew IS NOT NULL AND (NOT eUpdate OR tempOld IS NULL OR tempOld<>tempNew) THEN
			NEW.scopes = getScopes(tempNew);
		ELSE
			NEW.scopes = NULL;
		END IF;
		tempNew := NEW.ENTITY->'https://uri.etsi.org/ngsi-ld/default-context/scope';
		tempOld := NULL;
		IF eUpdate THEN
			tempOld := OLD.ENTITY->'https://uri.etsi.org/ngsi-ld/default-context/scope';
		END IF;
		
		IF NEW.scopes IS NULL AND tempNew IS NOT NULL AND (NOT eUpdate OR tempOld IS NULL OR tempOld<>tempNew) THEN
			NEW.scopes = getScopes(tempNew);
		ELSE
			NEW.scopes = NULL;
		END IF;
		RETURN NEW;
	END;
$function$;

CREATE OR REPLACE FUNCTION public.build_entity_lookup()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
	DECLARE
		entityType text;
	BEGIN
		IF TG_OP = 'UPDATE' THEN
			DELETE FROM etype2iid WHERE iid = NEW.id;
			DELETE FROM attr2iid WHERE iid = NEW.id;
		END IF;
		FOR entityType IN SELECT jsonb_array_elements_text FROM jsonb_array_elements_text(NEW.ENTITY->'@type') LOOP
			INSERT INTO etype2iid VALUES (entityType, NEW.id);
		END LOOP;
			PERFORM addAttribs(NEW.id, NEW.entity);
		RETURN NEW;
	END;
$function$;

CREATE TRIGGER entity_build_entity_lookup AFTER INSERT OR UPDATE ON entity
    FOR EACH ROW EXECUTE PROCEDURE build_entity_lookup();
	

SELECT addAttribs(id, entity) FROM public.entity;



CREATE OR REPLACE FUNCTION INSERTNGSILDENTITY (INSERTENTITY JSONB) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB) AS $$
declare
	attribName text;
	templateEntity jsonb;
	entityType text;
	entityTypes jsonb;
	attribValue jsonb;
	i_rec record;
	countInt integer;
	removeAttrib boolean;
	entityId text;
	insertLocation GEOMETRY(Geometry, 4326);
	insertScopes text[];
BEGIN
    CREATE TEMP TABLE resultTable (endPoint text UNIQUE, tenant text, headers jsonb, forwardEntity jsonb) ON COMMIT DROP;
	templateEntity := '{}'::jsonb;
	templateEntity := jsonb_set(templateEntity, '{@id}', insertEntity->'@id');
	entityId := insertEntity->>'@id';
	insertEntity := insertEntity - '@id';
	templateEntity := jsonb_set(templateEntity, '{@type}', insertEntity->'@type');
	entityTypes := insertEntity->'@type';
	insertEntity := insertEntity - '@type';
	templateEntity := jsonb_set(templateEntity, '{https://uri.etsi.org/ngsi-ld/createdAt}', insertEntity->'https://uri.etsi.org/ngsi-ld/createdAt');
	insertEntity := insertEntity - 'https://uri.etsi.org/ngsi-ld/createdAt';
	templateEntity := jsonb_set(templateEntity, '{https://uri.etsi.org/ngsi-ld/modifiedAt}', insertEntity->'https://uri.etsi.org/ngsi-ld/modifiedAt');
	insertEntity := insertEntity - 'https://uri.etsi.org/ngsi-ld/modifiedAt';
	IF insertEntity ? 'https://uri.etsi.org/ngsi-ld/observedAt' THEN
		templateEntity := jsonb_set(templateEntity, '{https://uri.etsi.org/ngsi-ld/observedAt}', insertEntity->'https://uri.etsi.org/ngsi-ld/observedAt');
		insertEntity := insertEntity - 'https://uri.etsi.org/ngsi-ld/observedAt';
	END IF;
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
	ELSIF (insertEntity ? 'https://uri.etsi.org/ngsi-ld/default-context/scope') THEN
		insertScopes = getScopes(insertEntity#>'{https://uri.etsi.org/ngsi-ld/default-context/scope}');
	ELSE
		insertScopes = NULL;
	END IF;
	FOR entityType IN select jsonb_array_elements_text from jsonb_array_elements_text(entityTypes) LOOP
		FOR attribName IN select jsonb_object_keys from jsonb_object_keys(insertEntity)	LOOP
			attribValue := insertEntity->attribName;
			removeAttrib := false;
			IF attribValue->>'{0,@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship' THEN
				FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.createEntity = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL) AND (c.e_rel IS NULL OR c.e_rel = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
					INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
					IF i_rec.reg_mode > 1 THEN
						removeAttrib := true;
						IF i_rec.reg_mode = 3 THEN
							EXIT;
						END IF;
					END IF;
				END LOOP;
			ELSE
				FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.createEntity = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_rel IS NULL) AND (c.e_prop IS NULL OR c.e_prop = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
					INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
					IF i_rec.reg_mode > 1 THEN
						removeAttrib := true;
						IF i_rec.reg_mode = 3 THEN
							EXIT;
						END IF;
					END IF;
				END LOOP;
			END IF;
			IF removeAttrib THEN
				insertEntity := insertEntity - attribName;
			END IF;
		END LOOP;
	END LOOP;
	SELECT count(jsonb_object_keys) FROM jsonb_object_keys(insertEntity) INTO countInt;
	IF countInt > 0 THEN
		SELECT insertEntity || templateEntity INTO insertEntity;
		BEGIN
			INSERT INTO ENTITY (E_ID, ENTITY) VALUES (entityId, insertEntity);
			INSERT INTO resultTable VALUES ('ADDED ENTITY', null, null, insertEntity);
		EXCEPTION WHEN OTHERS THEN
			INSERT INTO resultTable VALUES ('ERROR', null, null, ('{"sqlstate": "' || sqlstate::text || '", "sqlmessage": "'||SQLERRM::text ||'"}')::jsonb);
		END;
	END IF;
	RETURN QUERY SELECT * FROM resultTable;
END;
$$ LANGUAGE PLPGSQL;







CREATE OR REPLACE FUNCTION NOLOCALINFOOPERATION (ENTITYID text, DELTAENTITY JSONB, OPERATIONFIELD smallint) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB) AS $$
declare
	attribName text;
	templateEntity jsonb;
	entityType text;
	entityTypes jsonb;
	attribValue jsonb;
	insertLocation GEOMETRY(Geometry, 4326);
	insertScopes text[];
	i_rec record;
BEGIN
    CREATE TEMP TABLE resultTable (endPoint text UNIQUE, tenant text, headers jsonb, forwardEntity jsonb) ON COMMIT DROP;
	templateEntity := '{}'::jsonb;
	IF DELTAENTITY ? '@id' THEN
		templateEntity := jsonb_set(templateEntity, '@id', DELTAENTITY->'@id');
		DELTAENTITY := DELTAENTITY - '@id';
	END IF;
	templateEntity := jsonb_set(templateEntity, '{https://uri.etsi.org/ngsi-ld/createdAt}', DELTAENTITY->'https://uri.etsi.org/ngsi-ld/createdAt');
	DELTAENTITY := DELTAENTITY - 'https://uri.etsi.org/ngsi-ld/createdAt';
	templateEntity := jsonb_set(templateEntity, '{https://uri.etsi.org/ngsi-ld/modifiedAt}', DELTAENTITY->'https://uri.etsi.org/ngsi-ld/modifiedAt');
	DELTAENTITY := DELTAENTITY - 'https://uri.etsi.org/ngsi-ld/modifiedAt';
	IF DELTAENTITY ? 'https://uri.etsi.org/ngsi-ld/observedAt' THEN
		templateEntity := jsonb_set(templateEntity, '{https://uri.etsi.org/ngsi-ld/observedAt}', DELTAENTITY->'https://uri.etsi.org/ngsi-ld/observedAt');
		DELTAENTITY := DELTAENTITY - 'https://uri.etsi.org/ngsi-ld/observedAt';
	END IF;
	IF DELTAENTITY ? 'https://uri.etsi.org/ngsi-ld/location' THEN
		IF (DELTAENTITY@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
			insertLocation = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(DELTAENTITY#>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0}')::text ), 4326);
		ELSE
			insertLocation = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(DELTAENTITY#>'{https://uri.etsi.org/ngsi-ld/location,0}')::text ), 4326);
		END IF;
	ELSE
		insertLocation = null;
	END IF;
	IF (DELTAENTITY ? 'https://uri.etsi.org/ngsi-ld/scope') THEN
		insertScopes = getScopes(DELTAENTITY#>'{https://uri.etsi.org/ngsi-ld/scope}');
	ELSIF (DELTAENTITY ? 'https://uri.etsi.org/ngsi-ld/default-context/scope') THEN
		insertScopes = getScopes(DELTAENTITY#>'{https://uri.etsi.org/ngsi-ld/default-context/scope}');
	ELSE
		insertScopes = NULL;
	END IF;
	IF DELTAENTITY ? '@type' THEN
		templateEntity := jsonb_set(templateEntity, '@type', DELTAENTITY->'@type');
		entityTypes := DELTAENTITY->'@type';
		DELTAENTITY := DELTAENTITY - '@type';
		FOR entityType IN select jsonb_array_elements_text from jsonb_array_elements_text(entityTypes) LOOP
			FOR attribName IN select jsonb_object_keys from jsonb_object_keys(DELTAENTITY)	LOOP
				attribValue := DELTAENTITY->attribName;
				IF attribValue->>'{0,@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship' THEN
					CASE OPERATIONFIELD
						WHEN 2 THEN
							FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateEntity = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL) AND (c.e_rel IS NULL OR c.e_rel = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
								INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
								IF i_rec.reg_mode = 3 THEN
									EXIT;
								END IF;
							END LOOP;
						WHEN 3 THEN
							FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.appendAttrs = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL) AND (c.e_rel IS NULL OR c.e_rel = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
								INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
								IF i_rec.reg_mode = 3 THEN
									EXIT;
								END IF;
							END LOOP;
						WHEN 4 THEN
							FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateAttrs = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL) AND (c.e_rel IS NULL OR c.e_rel = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
								INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
								IF i_rec.reg_mode = 3 THEN
									EXIT;
								END IF;
							END LOOP;
					END CASE;
				ELSE
					CASE OPERATIONFIELD
						WHEN 2 THEN
							FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateEntity = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_rel IS NULL) AND (c.e_prop IS NULL OR c.e_prop = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
								INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
								IF i_rec.reg_mode = 3 THEN
									EXIT;
								END IF;
							END LOOP;
						WHEN 3 THEN
							FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.appendAttrs = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_rel IS NULL) AND (c.e_prop IS NULL OR c.e_prop = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
								INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
								IF i_rec.reg_mode = 3 THEN
									EXIT;
								END IF;
							END LOOP;
						WHEN 4 THEN
							FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateAttrs = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_rel IS NULL) AND (c.e_prop IS NULL OR c.e_prop = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
								INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
								IF i_rec.reg_mode = 3 THEN
									EXIT;
								END IF;
							END LOOP;
					END CASE;
				END IF;
				DELTAENTITY := DELTAENTITY - attribName;
			END LOOP;
		END LOOP;
		
		FOR attribName IN SELECT jsonb_object_keys FROM jsonb_object_keys(DELTAENTITY) LOOP
			INSERT INTO resultTable VALUES ('NOT ADDED', null, null, '{{"attribName": attribName, "datasetId": "any"}}'::jsonb) ON CONFLICT DO UPDATE SET forwardEntity=forwardEntity || '{{"attribName": attribName, "datasetId": "any"}}'::jsonb;
		END LOOP;
	ELSE
		FOR attribName IN select jsonb_object_keys from jsonb_object_keys(DELTAENTITY)	LOOP
			attribValue := DELTAENTITY->attribName;
			IF attribValue->>'{0,@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship' THEN
				CASE OPERATIONFIELD
					WHEN 2 THEN
						FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateEntity = true AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL) AND (c.e_rel IS NULL OR c.e_rel = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
							INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
							IF i_rec.reg_mode = 3 THEN
								EXIT;
							END IF;
						END LOOP;
					WHEN 3 THEN
						FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.appendAttrs = true AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL) AND (c.e_rel IS NULL OR c.e_rel = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
							INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
							IF i_rec.reg_mode = 3 THEN
								EXIT;
							END IF;
						END LOOP;
					WHEN 4 THEN
						FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateAttrs = true AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL) AND (c.e_rel IS NULL OR c.e_rel = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
							INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
							IF i_rec.reg_mode = 3 THEN
								EXIT;
							END IF;
						END LOOP;
				END CASE;
			ELSE
				CASE OPERATIONFIELD
					WHEN 2 THEN
						FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateEntity = true AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_rel IS NULL) AND (c.e_prop IS NULL OR c.e_prop = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
							INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
							IF i_rec.reg_mode = 3 THEN
								EXIT;
							END IF;
						END LOOP;
					WHEN 3 THEN
						FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.appendAttrs = true AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_rel IS NULL) AND (c.e_prop IS NULL OR c.e_prop = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
							INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
							IF i_rec.reg_mode = 3 THEN
								EXIT;
							END IF;
						END LOOP;
					WHEN 4 THEN
						FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateAttrs = true AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_rel IS NULL) AND (c.e_prop IS NULL OR c.e_prop = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
							INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
							IF i_rec.reg_mode = 3 THEN
								EXIT;
							END IF;
						END LOOP;
				END CASE;
			END IF;
			DELTAENTITY := DELTAENTITY - attribName;
		END LOOP;
		FOR attribName IN SELECT jsonb_object_keys FROM jsonb_object_keys(DELTAENTITY) LOOP
			INSERT INTO resultTable VALUES ('NOT ADDED', null, null, '{{"attribName": attribName, "datasetId": "any"}}'::jsonb) ON CONFLICT DO UPDATE SET forwardEntity=resultTable.forwardEntity || '{{"attribName": attribName, "datasetId": "any"}}'::jsonb;
		END LOOP;
	END IF;
	RETURN QUERY SELECT * FROM resultTable;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION LOCALINFOOPERATION (ENTITYID text, DELTAENTITY JSONB, LOCALENTITY JSONB, IID BIGINT, NOOVERWRITE BOOLEAN, OPERATIONFIELD SMALLINT) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB) AS $$
declare
	attribName text;
	templateEntity jsonb;
	entityType text;
	entityTypes jsonb;
	attribValue jsonb;
	i_rec record;
	countInt integer;
	removeAttrib boolean;
	insertLocation GEOMETRY(Geometry, 4326);
	insertScopes text[];
	localEntity jsonb;
	notUpdated text[];
	localAttribValue jsonb;
	localDatasetId text;
	datasetId text;
	tempArray jsonb;
BEGIN
    CREATE TEMP TABLE resultTable (endPoint text UNIQUE, tenant text, headers jsonb, forwardEntity jsonb) ON COMMIT DROP;
	templateEntity := '{}'::jsonb;
	entityTypes := localEntity->'@type';
	IF DELTAENTITY ? '@type' THEN
		IF localEntity->'@type'@>DELTAENTITY->'@type' THEN
			localEntity := jsonb_set(localEntity, '@type', DELTAENTITY->'@type');
			templateEntity := jsonb_set(templateEntity, '@type', DELTAENTITY->'@type');
			DELTAENTITY := DELTAENTITY - '@type';
		ELSE
			INSERT INTO resultTable (endpoint, forwardEntity) VALUES ('ERROR', '{"Removing a type is not allowed"}'::jsonb);
			RETURN QUERY SELECT * FROM resultTable;
		END IF;
	END IF;
	IF DELTAENTITY ? '@id' THEN
		templateEntity := jsonb_set(templateEntity, '@id', DELTAENTITY->'@id');
		DELTAENTITY := DELTAENTITY - '@id';
	END IF;
	templateEntity := jsonb_set(templateEntity, '{https://uri.etsi.org/ngsi-ld/createdAt}', DELTAENTITY->'https://uri.etsi.org/ngsi-ld/createdAt');
	localEntity := jsonb_set(localEntity, '{https://uri.etsi.org/ngsi-ld/createdAt}', DELTAENTITY->'https://uri.etsi.org/ngsi-ld/createdAt');
	DELTAENTITY := DELTAENTITY - 'https://uri.etsi.org/ngsi-ld/createdAt';
	templateEntity := jsonb_set(templateEntity, '{https://uri.etsi.org/ngsi-ld/modifiedAt}', DELTAENTITY->'https://uri.etsi.org/ngsi-ld/modifiedAt');
	localEntity := jsonb_set(localEntity, '{https://uri.etsi.org/ngsi-ld/modifiedAt}', DELTAENTITY->'https://uri.etsi.org/ngsi-ld/modifiedAt');
	DELTAENTITY := DELTAENTITY - 'https://uri.etsi.org/ngsi-ld/modifiedAt';
	IF DELTAENTITY ? 'https://uri.etsi.org/ngsi-ld/observedAt' THEN
		templateEntity := jsonb_set(templateEntity, '{https://uri.etsi.org/ngsi-ld/observedAt}', DELTAENTITY->'https://uri.etsi.org/ngsi-ld/observedAt');
		localEntity := jsonb_set(localEntity, '{https://uri.etsi.org/ngsi-ld/observedAt}', DELTAENTITY->'https://uri.etsi.org/ngsi-ld/observedAt');
			DELTAENTITY := DELTAENTITY - 'https://uri.etsi.org/ngsi-ld/observedAt';
	END IF;
	IF DELTAENTITY ? 'https://uri.etsi.org/ngsi-ld/location' THEN
		IF (DELTAENTITY@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
			insertLocation = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(DELTAENTITY#>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0}')::text ), 4326);
		ELSE
			insertLocation = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(DELTAENTITY#>'{https://uri.etsi.org/ngsi-ld/location,0}')::text ), 4326);
		END IF;
	ELSIF localEntity ? 'https://uri.etsi.org/ngsi-ld/location' THEN
		IF (localEntity@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
			insertLocation = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(localEntity#>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0}')::text ), 4326);
		ELSE
			insertLocation = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(localEntity#>'{https://uri.etsi.org/ngsi-ld/location,0}')::text ), 4326);
		END IF;
	ELSE
		insertLocation = null;
	END IF;
	IF (DELTAENTITY ? 'https://uri.etsi.org/ngsi-ld/scope') THEN
		insertScopes = getScopes(DELTAENTITY#>'{https://uri.etsi.org/ngsi-ld/scope}');
	ELSIF (DELTAENTITY ? 'https://uri.etsi.org/ngsi-ld/default-context/scope') THEN
		insertScopes = getScopes(DELTAENTITY#>'{https://uri.etsi.org/ngsi-ld/default-context/scope}');
	ELSIF (localEntity ? 'https://uri.etsi.org/ngsi-ld/scope') THEN
		insertScopes = getScopes(localEntity#>'{https://uri.etsi.org/ngsi-ld/scope}');
	ELSIF (localEntity ? 'https://uri.etsi.org/ngsi-ld/default-context/scope') THEN
		insertScopes = getScopes(localEntity#>'{https://uri.etsi.org/ngsi-ld/default-context/scope}');
	ELSE
		insertScopes = NULL;
	END IF;
	FOR entityType IN select jsonb_array_elements_text from jsonb_array_elements_text(entityTypes) LOOP
		FOR attribName IN select jsonb_object_keys from jsonb_object_keys(DELTAENTITY)	LOOP
			attribValue := DELTAENTITY->attribName;
			removeAttrib := false;
			IF attribValue->>'{0,@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship' THEN
				CASE OPERATIONFIELD
					WHEN 2 THEN
						FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateEntity = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL) AND (c.e_rel IS NULL OR c.e_rel = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
							INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
							IF i_rec.reg_mode > 1 THEN
								removeAttrib := true;
								IF i_rec.reg_mode = 3 THEN
									EXIT;
								END IF;
							END IF;
						END LOOP;
					WHEN 3 THEN
						FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.appendAttrs = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL) AND (c.e_rel IS NULL OR c.e_rel = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
							INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
							IF i_rec.reg_mode > 1 THEN
								removeAttrib := true;
								IF i_rec.reg_mode = 3 THEN
									EXIT;
								END IF;
							END IF;
						END LOOP;
					WHEN 4 THEN
						FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateAttrs = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL) AND (c.e_rel IS NULL OR c.e_rel = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
							INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
							IF i_rec.reg_mode > 1 THEN
								removeAttrib := true;
								IF i_rec.reg_mode = 3 THEN
									EXIT;
								END IF;
							END IF;
						END LOOP;
				END CASE;
				
			ELSE
				CASE OPERATIONFIELD
					WHEN 2 THEN
						FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateEntity = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_rel IS NULL) AND (c.e_prop IS NULL OR c.e_prop = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
							INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
							IF i_rec.reg_mode > 1 THEN
								removeAttrib := true;
								IF i_rec.reg_mode = 3 THEN
									EXIT;
								END IF;
							END IF;
						END LOOP;
					WHEN 3 THEN
						FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateEntity = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_rel IS NULL) AND (c.e_prop IS NULL OR c.e_prop = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
							INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
							IF i_rec.reg_mode > 1 THEN
								removeAttrib := true;
								IF i_rec.reg_mode = 3 THEN
									EXIT;
								END IF;
							END IF;
						END LOOP;
					WHEN 4 THEN
						FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.updateEntity = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_rel IS NULL) AND (c.e_prop IS NULL OR c.e_prop = attribName) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
							INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, templateEntity) ON CONFLICT DO UPDATE SET forwardEntity = jsonb_set(resultTable.forwardEntity, '{attribName}', attribValue);
							IF i_rec.reg_mode > 1 THEN
								removeAttrib := true;
								IF i_rec.reg_mode = 3 THEN
									EXIT;
								END IF;
							END IF;
						END LOOP;
				END CASE;
			END IF;
			IF removeAttrib THEN
				DELTAENTITY := DELTAENTITY - attribName;
			END IF;
		END LOOP;
	END LOOP;
	FOR attribName IN SELECT jsonb_object_keys FROM jsonb_object_keys(DELTAENTITY) LOOP
		FOR attribValue IN SELECT jsonb_array_elements FROM jsonb_array_elements(DELTAENTITY->attribName) LOOP
			datasetId := attribValue->'https://uri.etsi.org/ngsi-ld/datasetId';
			IF NOOVERWRITE THEN
				SELECT COUNT(iid)=1 INTO removeAttrib FROM attr2iid WHERE attr=attribName AND dataset_id = datasetId;
				IF removeAttrib THEN
					INSERT INTO resultTable VALUES ('NOT ADDED', null, null, '{{"attribName": attribName, "datasetId": datasetId}}'::jsonb) ON CONFLICT DO UPDATE SET forwardEntity=resultTable.forwardEntity || '{{"attribName": attribName, "datasetId": datasetId}}'::jsonb;
					CONTINUE;
				END IF;
			END IF;
			tempArray = '[]'::jsonb;
			removeAttrib := false;
			FOR localAttribValue IN SELECT jsonb_array_elements FROM jsonb_array_elements(localEntity->attribName) LOOP
				localDatasetId := localAttribValue->'https://uri.etsi.org/ngsi-ld/datasetId';
				IF (localDatasetId IS NULL AND datasetId IS NOT NULL) OR (localDatasetId IS NOT NULL AND datasetId IS NULL) OR (localDatasetId <> datasetId) THEN
					tempArray = tempArray || localAttribValue;
				ELSIF OPERATIONFIELD = 4 THEN
					removeAttrib := true;
					tempArray = tempArray || (localAttribValue || attribValue);
				END IF;
			END LOOP;
			IF OPERATIONFIELD = 2 OR OPERATIONFIELD = 3 THEN
				tempArray = tempArray || attribValue;
				INSERT INTO resultTable VALUES ('ADDED', null, null, '{{"attribName": attribName, "datasetId": datasetId}}'::jsonb) ON CONFLICT DO UPDATE SET forwardEntity=resultTable.forwardEntity || '{{"attribName": attribName, "datasetId": datasetId}}'::jsonb;
			ELSIF OPERATIONFIELD = 4 THEN 
				IF removeAttrib THEN
					INSERT INTO resultTable VALUES ('NOT ADDED', null, null, '{{"attribName": attribName, "datasetId": datasetId}}'::jsonb) ON CONFLICT DO UPDATE SET forwardEntity=resultTable.forwardEntity || '{{"attribName": attribName, "datasetId": datasetId}}'::jsonb;
				ELSE
					INSERT INTO resultTable VALUES ('ADDED', null, null, '{{"attribName": attribName, "datasetId": datasetId}}'::jsonb) ON CONFLICT DO UPDATE SET forwardEntity=resultTable.forwardEntity || '{{"attribName": attribName, "datasetId": datasetId}}'::jsonb;
				END IF;
			END IF;
			localEntity := jsonb_set(localEntity, attribName, tempArray);
		END LOOP;
	END LOOP;
	UPDATE ENTITY SET ENTITY.ENTITY=localEntity||templateEntity WHERE ENTITY.id = iid;
	RETURN QUERY SELECT * FROM resultTable;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION APPENDNGSILDENTITY (ENTITYID text, DELTAENTITY JSONB, NOOVERWRITE boolean) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB) AS $$
declare
	LOCALENTITY jsonb;
	IID bigint;
BEGIN
	SELECT entity.ENTITY, entity.id INTO LOCALENTITY, IID FROM entity WHERE entity.E_ID = ENTITYID;
	IF LOCALENTITY IS NOT NULL THEN
		RETURN QUERY SELECT * FROM LOCALINFOOPERATION (ENTITYID, DELTAENTITY, LOCALENTITY, IID, NOOVERWRITE, 3);
	ELSE	
		RETURN QUERY SELECT * FROM NOLOCALINFOOPERATION (ENTITYID, DELTAENTITY, 3);
	END IF;	
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION UPDATENGSILDENTITY (ENTITYID text, DELTAENTITY JSONB) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB) AS $$
declare
	LOCALENTITY jsonb;
	IID bigint;
BEGIN
	SELECT entity.ENTITY, entity.id INTO LOCALENTITY, IID FROM entity WHERE entity.e_id = ENTITYID;
	IF LOCALENTITY IS NOT NULL THEN
		RETURN QUERY SELECT * FROM LOCALINFOOPERATION (ENTITYID, DELTAENTITY, LOCALENTITY, IID, false, 2);
	ELSE
		RETURN QUERY SELECT * FROM NOLOCALINFOOPERATION (ENTITYID, DELTAENTITY, 2);
	END IF;	
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION PARTIALUPDATENGSI (ENTITYID text, DELTAENTITY JSONB) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB) AS $$
declare
	LOCALENTITY jsonb;
	IID bigint;
BEGIN
	SELECT entity.ENTITY, entity.id INTO LOCALENTITY, IID FROM entity WHERE entity.e_id = ENTITYID;
	IF LOCALENTITY IS NOT NULL THEN
		RETURN QUERY SELECT * FROM LOCALINFOOPERATION (ENTITYID, DELTAENTITY, LOCALENTITY, IID, false, 4);
	ELSE	
		RETURN QUERY SELECT * FROM NOLOCALINFOOPERATION (ENTITYID, DELTAENTITY, 4);
	END IF;	
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION DELETEENTITY (ENTITYID text) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB) AS $$
declare
	LOCALENTITY jsonb;
	IID bigint;
	i_rec record;
	entityType text;
	entityTypes jsonb;
	insertLocation GEOMETRY(Geometry, 4326);
	insertScopes text[];
BEGIN
	SELECT entity.ENTITY, entity.id INTO LOCALENTITY, IID FROM entity WHERE entity.e_id = ENTITYID;
	IF LOCALENTITY IS NOT NULL THEN
		entityTypes := LOCALENTITY->'@type';
		IF LOCALENTITY ? 'https://uri.etsi.org/ngsi-ld/location' THEN
			IF (LOCALENTITY@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
				insertLocation = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(LOCALENTITY#>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0}')::text ), 4326);
			ELSE
				insertLocation = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(LOCALENTITY#>'{https://uri.etsi.org/ngsi-ld/location,0}')::text ), 4326);
			END IF;
		ELSE
			insertLocation = null;
		END IF;
		IF (localEntity ? 'https://uri.etsi.org/ngsi-ld/scope') THEN
			insertScopes = getScopes(localEntity#>'{https://uri.etsi.org/ngsi-ld/scope}');
		ELSIF (localEntity ? 'https://uri.etsi.org/ngsi-ld/default-context/scope') THEN
			insertScopes = getScopes(localEntity#>'{https://uri.etsi.org/ngsi-ld/default-context/scope}');
		ELSE
			insertScopes = NULL;
		END IF;
		FOR entityType IN select jsonb_array_elements_text from jsonb_array_elements_text(entityTypes) LOOP
			FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.deleteEntity = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
				INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, NULL) ON CONFLICT DO NOTHING;
			END LOOP;
		END LOOP;
		BEGIN
			DELETE FROM ENTITY WHERE ENTITY.id = IID;
			INSERT INTO resultTable VALUES ("DELETED ENTITY", NULL, NULL, LOCALENTITY);
		EXCEPTION WHEN OTHERS THEN
			INSERT INTO resultTable VALUES ('ERROR', null, null, ('{"sqlstate": "' || sqlstate::text || '", "sqlmessage": "'||SQLERRM::text ||'"}')::jsonb);
		END;
	ELSE
		FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.deleteEntity = true AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') ORDER BY c.reg_mode DESC LOOP
				INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, NULL) ON CONFLICT DO NOTHING;
			END LOOP;
	END IF;
	RETURN QUERY SELECT * FROM resultTable;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION DELETEATTR (ENTITYID text, ATTR text, DATASETID text) RETURNS TABLE (ENDPOINT text, TENANT text, HEADERS jsonb, FORWARDENTITY JSONB) AS $$
DECLARE
	LOCALENTITY jsonb;
	IID bigint;
	entityType text;
	entityTypes jsonb;
	insertLocation GEOMETRY(Geometry, 4326);
	insertScopes text[];
	i_rec record;
	attribValue jsonb;
	localAttribValue jsonb;
	localDatasetId text;
	tempArray jsonb;
	EMPTYATTR boolean;
BEGIN
	SELECT entity.ENTITY, entity.id INTO LOCALENTITY, IID FROM entity WHERE entity.e_id = ENTITYID;
	IF LOCALENTITY IS NOT NULL THEN
		entityTypes := LOCALENTITY->'@type';
		IF LOCALENTITY ? 'https://uri.etsi.org/ngsi-ld/location' THEN
			IF (LOCALENTITY@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
				insertLocation = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(LOCALENTITY#>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0}')::text ), 4326);
			ELSE
				insertLocation = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson(LOCALENTITY#>'{https://uri.etsi.org/ngsi-ld/location,0}')::text ), 4326);
			END IF;
		ELSE
			insertLocation = null;
		END IF;
		IF (localEntity ? 'https://uri.etsi.org/ngsi-ld/scope') THEN
			insertScopes = getScopes(localEntity#>'{https://uri.etsi.org/ngsi-ld/scope}');
		ELSIF (localEntity ? 'https://uri.etsi.org/ngsi-ld/default-context/scope') THEN
			insertScopes = getScopes(localEntity#>'{https://uri.etsi.org/ngsi-ld/default-context/scope}');
		ELSE
			insertScopes = NULL;
		END IF;
		FOR entityType IN select jsonb_array_elements_text from jsonb_array_elements_text(entityTypes) LOOP
			FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.deleteEntity = true AND (c.e_type = entityType OR c.e_type IS NULL) AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL OR c.e_prop = ATTR) AND (c.e_rel IS NULL OR c.e_rel = ATTR) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') AND (c.i_location IS NULL OR ST_INTERSECTS(c.i_location, insertLocation)) AND (c.scopes IS NULL OR c.scopes && insertScopes) ORDER BY c.reg_mode DESC LOOP
				INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, NULL) ON CONFLICT DO NOTHING;
			END LOOP;
		END LOOP;
		BEGIN
			attribValue := LOCALENTITY->ATTR;
			IF attribValue IS NOT NULL THEN
				tempArray = '[]'::jsonb;
				FOR localAttribValue IN SELECT jsonb_array_elements FROM jsonb_array_elements(attribValue) LOOP
					localDatasetId := localAttribValue->'https://uri.etsi.org/ngsi-ld/datasetId';
					IF (localDatasetId IS NULL AND datasetId IS NOT NULL) OR (localDatasetId IS NOT NULL AND datasetId IS NULL) OR (localDatasetId <> datasetId) THEN
						tempArray = tempArray || localAttribValue;
					END IF;
				END LOOP;
				SELECT COUNT(jsonb_array_elements)=0 FROM jsonb_array_elements(attribValue) INTO EMPTYATTR;
				IF EMPTYATTR THEN
					LOCALENTITY := LOCALENTITY - ATTR;
				END IF;
				UPDATE ENTITY SET ENTITY.ENTITY=localEntity||templateEntity WHERE ENTITY.id = iid;
				INSERT INTO resultTable VALUES ("DELETED ATTRIB", NULL, NULL, '{"attr": ATTR, "datasetId": DATASETID}'::jsonb);
			ELSE 
				INSERT INTO resultTable VALUES ('ERROR', null, null, ('{"sqlstate": "02000", "sqlmessage": "Not found"}')::jsonb);
			END IF;
			
		EXCEPTION WHEN OTHERS THEN
			INSERT INTO resultTable VALUES ('ERROR', null, null, ('{"sqlstate": "' || sqlstate::text || '", "sqlmessage": "'||SQLERRM::text ||'"}')::jsonb);
		END;
	ELSE
		FOR i_rec IN SELECT c.endpoint, c.tenant_id, c.headers, c.reg_mode FROM csourceinformation AS c WHERE c.reg_mode > 0 AND c.deleteEntity = true AND (c.e_id IS NULL OR c.e_id = entityId) AND (c.e_id_p IS NULL OR entityId ~ c.e_id_p) AND (c.e_prop IS NULL OR c.e_prop = ATTR) AND (c.e_rel IS NULL OR c.e_rel = ATTR)AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') ORDER BY c.reg_mode DESC LOOP
				INSERT INTO resultTable VALUES (i_rec.endPoint, i_rec.tenant_id, i_rec.headers, NULL) ON CONFLICT DO NOTHING;
			END LOOP;
	END IF;
	RETURN QUERY SELECT * FROM resultTable;	
END;
$$ LANGUAGE PLPGSQL;

