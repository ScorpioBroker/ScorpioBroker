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
	c_id text,
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
	entityMap boolean,
	canCompress boolean,
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
    (i_location gist_geometry_ops_2d);
	
DROP FUNCTION public.csource_extract_jsonb_fields_to_information_table cascade;
DROP Trigger csource_extract_jsonb_fields ON csource;
	
CREATE TABLE temp (
	c_id text,
	reg jsonb
);
INSERT INTO temp SELECT c_id, reg FROM csource;

DELETE FROM csource;

INSERT INTO csource SELECT c_id, reg FROM temp;

drop table temp;

ALTER TABLE PUBLIC.ENTITY RENAME COLUMN DATA TO ENTITY;

ALTER TABLE PUBLIC.ENTITY DROP COLUMN DATA_WITHOUT_SYSATTRS;

ALTER TABLE PUBLIC.ENTITY DROP COLUMN KVDATA;

ALTER TABLE PUBLIC.ENTITY DROP COLUMN OBSERVATIONSPACE;

ALTER TABLE PUBLIC.ENTITY DROP COLUMN OPERATIONSPACE;

ALTER TABLE PUBLIC.ENTITY DROP COLUMN CONTEXT;

ALTER TABLE PUBLIC.ENTITY ADD COLUMN E_TYPES TEXT[];

CREATE INDEX "I_entity_scopes"
    ON public.entity USING gin
    (scopes array_ops);

CREATE INDEX "I_entity_types"
    ON public.entity USING gin
    (e_types array_ops);

CREATE OR REPLACE FUNCTION public.entity_extract_jsonb_fields() RETURNS trigger LANGUAGE plpgsql AS $function$
	BEGIN
		
		-- do not reprocess if it is just an update on another column
        IF (TG_OP = 'INSERT' AND NEW.ENTITY IS NOT NULL) OR 
        	(TG_OP = 'UPDATE' AND OLD.ENTITY IS NULL AND NEW.ENTITY IS NOT NULL) OR 
            (TG_OP = 'UPDATE' AND OLD.ENTITY IS NOT NULL AND NEW.ENTITY IS NULL) OR 
			(TG_OP = 'UPDATE' AND OLD.ENTITY IS NOT NULL AND NEW.ENTITY IS NOT NULL AND OLD.ENTITY <> NEW.ENTITY) THEN 
          NEW.createdat = (NEW.ENTITY#>>'{https://uri.etsi.org/ngsi-ld/createdAt,0,@value}')::TIMESTAMP;
          NEW.modifiedat = (NEW.ENTITY#>>'{https://uri.etsi.org/ngsi-ld/modifiedAt,0,@value}')::TIMESTAMP;
          IF (NEW.ENTITY@>'{"https://uri.etsi.org/ngsi-ld/location": [ {"@type": [ "https://uri.etsi.org/ngsi-ld/GeoProperty" ] } ] }') THEN
              NEW.location = ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson( NEW.ENTITY#>'{https://uri.etsi.org/ngsi-ld/location,0,https://uri.etsi.org/ngsi-ld/hasValue,0}') ), 4326);
          ELSE 
              NEW.location = NULL;
          END IF;
          IF (NEW.ENTITY ? 'https://uri.etsi.org/ngsi-ld/scope') THEN
              NEW.scopes = getScopes(NEW.ENTITY#>'{https://uri.etsi.org/ngsi-ld/scope}');
          ELSE 
              NEW.scopes = NULL;
          END IF;
        END IF;
		RETURN NEW;
	END;
$function$;
	
UPDATE ENTITY SET E_TYPES=array_append(E_TYPES,TYPE);

ALTER TABLE PUBLIC.ENTITY DROP COLUMN type;


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
			operations = array[false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,false,false]::boolean[];
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
											SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE id = entityId AND entityType = ANY(e_types) AND ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' != 'https://uri.etsi.org/ngsi-ld/Relationship');
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and id % conflicts with existing entry', attribName, entityId USING ERRCODE='23514';
											END IF;
										ELSIF entityIdPattern IS NOT NULL THEN
											SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE id ~ entityIdPattern AND entityType = ANY(e_types) AND ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' != 'https://uri.etsi.org/ngsi-ld/Relationship');
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and idpattern % conflicts with existing entry', attribName, entityIdPattern USING ERRCODE='23514';
											END IF;
										ELSE
											SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE entityType = ANY(e_types) AND ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' != 'https://uri.etsi.org/ngsi-ld/Relationship');
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and type % conflicts with existing entry', attribName, entityType USING ERRCODE='23514';
											END IF;
										END IF;
									END IF;
									INSERT INTO csourceinformation (cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, entityMap, canCompress) VALUES (internalId, NEW.C_ID, entityId, entityIdPattern, entityType, NULL, attribName, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38]);
								END LOOP;
							END IF;
							IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/relationshipNames' THEN
								attribsAdded = true;
								FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/relationshipNames}') LOOP
									IF regMode > 1 THEN
										IF entityId IS NOT NULL THEN 
											SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE id = entityId AND entityType = ANY(e_types) AND ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship');
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and id % conflicts with existing entry', attribName, entityId USING ERRCODE='23514';
											END IF;
										ELSIF entityIdPattern IS NOT NULL THEN
											SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE id ~ entityIdPattern AND entityType = ANY(e_types) AND ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship');
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and idpattern % conflicts with existing entry', attribName, entityIdPattern USING ERRCODE='23514';
											END IF;
										ELSE
											SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE entityType = ANY(e_types) AND ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship');
											IF errorFound THEN
												RAISE EXCEPTION 'Registration with attrib % and type % conflicts with existing entry', attribName, entityType USING ERRCODE='23514';
											END IF;
										END IF;
									END IF;
									INSERT INTO csourceinformation (cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, entityMap, canCompress) VALUES (internalId, NEW.C_ID, entityId, entityIdPattern, entityType, attribName, NULL, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38]);
									
								END LOOP;
							END IF;
							IF NOT attribsAdded THEN
								IF regMode > 1 THEN
									IF entityId IS NOT NULL THEN 
										SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE id = entityId AND entityType = ANY(e_types);
										IF errorFound THEN
											RAISE EXCEPTION 'Registration with entityId % conflicts with existing entity', entityId USING ERRCODE='23514';
										END IF;
									ELSIF entityIdPattern IS NOT NULL THEN
										SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE id ~ entityIdPattern AND entityType = ANY(e_types);
										IF errorFound THEN
											RAISE EXCEPTION 'Registration with idPattern % and type % conflicts with existing entity', entityIdPattern, entityType USING ERRCODE='23514';
										END IF;
									ELSE
										SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE entityType = ANY(e_types);
										IF errorFound THEN
											RAISE EXCEPTION 'Registration with type % conflicts with existing entity', entityType USING ERRCODE='23514';
										END IF;
									END IF;
								END IF;
								INSERT INTO csourceinformation(cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, entityMap, canCompress) values (internalId, NEW.C_ID, entityId, entityIdPattern, entityType, NULL, NULL, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38]);
							END IF;
						END LOOP;
					END LOOP;
				ELSE
					IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/propertyNames' THEN
						FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/propertyNames}') LOOP
							SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' != 'https://uri.etsi.org/ngsi-ld/Relationship');
							IF regMode > 1 AND errorFound THEN
								RAISE EXCEPTION 'Attribute % conflicts with existing entity', attribName USING ERRCODE='23514';
							END IF;
							INSERT INTO csourceinformation(cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, entityMap, canCompress) VALUES (internalId, NEW.C_ID, NULL, NULL, NULL, NULL, attribName, location, scopes, expires, endpoint, tenant, headers, regMode, operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38]);
						END LOOP;
					END IF;
					IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/relationshipNames' THEN
						FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/relationshipNames}') LOOP
							SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship');
							IF regMode > 1 AND errorFound THEN
								RAISE EXCEPTION 'Attribute % conflicts with existing entity', attribName USING ERRCODE='23514';
							END IF;
							INSERT INTO csourceinformation(cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, entityMap, canCompress) VALUES (internalId, NEW.C_ID, NULL, NULL, NULL, attribName, NULL, location, scopes, expires, endpoint, tenant, headers, regMode, operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38]);
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
	operations = array[false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false]::boolean[];
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
			WHEN 'entityMap' THEN
				operations[37] = true;
			WHEN 'canCompress' THEN
				operations[38] = true;
		END CASE;
	END LOOP;
	RETURN operations;
END;
$operations$ LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION NGSILD_PARTIALUPDATE(ENTITY jsonb, attribName text, attribValues jsonb) RETURNS jsonb AS $ENTITYPU$
declare
	tmp jsonb;
	datasetId text;
	insertDatasetId text;
	originalEntry jsonb;
	insertEntry jsonb;
	inUpdate boolean;
BEGIN
	tmp := '[]'::jsonb;
	FOR originalEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITY->attribName) LOOP
		inUpdate := False;
		datasetId := originalEntry #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}';
		FOR insertEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(attribValues) LOOP
			insertDatasetId := insertEntry #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}';
			IF (insertDatasetId is null and datasetId is null)or (insertDatasetId is not null and datasetId is not null and insertDatasetId = datasetId) THEN
				inUpdate = true;
				EXIT;
			END IF;
		END LOOP;
		IF NOT inUpdate THEN
			tmp := tmp || originalEntry;
		END IF;
	END LOOP;
	tmp := tmp || attribValues;
	RETURN jsonb_set(ENTITY,ARRAY[attribName], tmp);
END;
$ENTITYPU$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION NGSILD_DELETEATTRIB(ENTITY jsonb, attribName text, deleteDatasetId text) RETURNS jsonb AS $ENTITYPD$
declare
	tmp jsonb;
	datasetId text;
	originalEntry jsonb;
BEGIN
	tmp := '[]'::jsonb;
	FOR originalEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITY->attribName) LOOP
		datasetId := originalEntry #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}';
		IF NOT ((deleteDatasetId is null and datasetId is null)or (deleteDatasetId is not null and datasetId is not null and deleteDatasetId = datasetId)) THEN
			tmp := tmp || originalEntry;
		END IF;
	END LOOP;
	IF jsonb_array_length(tmp) > 0 THEN
		RETURN jsonb_set(ENTITY,'{attribName}', tmp);
	ELSE
		RETURN ENTITY - attribName;
	END IF;
END;
$ENTITYPD$ LANGUAGE PLPGSQL;


