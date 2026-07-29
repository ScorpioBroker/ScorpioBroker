-- Exclusive registrations must name a specific entity id and attributes (ETSI GS CIM 009 V1.9.1 clause 4.3.6.3).
-- Reject exclusive/redirect registrations that overlap an existing exclusive/redirect on the same entity id + attribute.

CREATE OR REPLACE FUNCTION public.csourceinformation_extract_jsonb_fields()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    VOLATILE
    COST 100
AS $BODY$
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
	csourceAlias text;
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
			operations = array[false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,false]::boolean[];
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
		IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/contextSourceAlias') THEN
			csourceAlias = (NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/contextSourceAlias,0,@value}');
		ELSE
			csourceAlias = NULL;
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
							IF regMode = 3 THEN
								IF entityId IS NULL OR entityIdPattern IS NOT NULL THEN
									RAISE EXCEPTION 'Exclusive registration requires a specific entity id (idPattern/type-only not allowed)' USING ERRCODE='22023';
								END IF;
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
									IF regMode > 1 AND entityId IS NOT NULL THEN
										SELECT count(*)>0 INTO errorFound FROM csourceinformation WHERE cs_id <> internalId AND e_id = entityId AND reg_mode > 1 AND ((e_prop = attribName OR e_rel = attribName) OR (e_prop IS NULL AND e_rel IS NULL));
										IF errorFound THEN
											RAISE EXCEPTION 'Registration with attrib % and id % conflicts with existing registration', attribName, entityId USING ERRCODE='23514';
										END IF;
									END IF;
									INSERT INTO csourceinformation (cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap, csource_alias, purgeEntity) VALUES (internalId, NEW.C_ID, entityId, entityIdPattern, entityType, NULL, attribName, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38],operations[39],operations[40],operations[41], csourceAlias, operations[42]);
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
									IF regMode > 1 AND entityId IS NOT NULL THEN
										SELECT count(*)>0 INTO errorFound FROM csourceinformation WHERE cs_id <> internalId AND e_id = entityId AND reg_mode > 1 AND ((e_prop = attribName OR e_rel = attribName) OR (e_prop IS NULL AND e_rel IS NULL));
										IF errorFound THEN
											RAISE EXCEPTION 'Registration with attrib % and id % conflicts with existing registration', attribName, entityId USING ERRCODE='23514';
										END IF;
									END IF;
									INSERT INTO csourceinformation (cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap, csource_alias, purgeEntity) VALUES (internalId, NEW.C_ID, entityId, entityIdPattern, entityType, attribName, NULL, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38],operations[39],operations[40],operations[41], csourceAlias, operations[42]);
									
								END LOOP;
							END IF;
							IF NOT attribsAdded THEN
								IF regMode = 3 THEN
									RAISE EXCEPTION 'Exclusive registration requires attributes' USING ERRCODE='22023';
								END IF;
								IF regMode > 1 THEN
									IF entityId IS NOT NULL THEN
										SELECT count(*)>0 INTO errorFound FROM csourceinformation WHERE cs_id <> internalId AND e_id = entityId AND reg_mode > 1;
										IF errorFound THEN
											RAISE EXCEPTION 'Registration with entityId % conflicts with existing registration', entityId USING ERRCODE='23514';
										END IF;
									END IF;
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
								INSERT INTO csourceinformation(cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap, csource_alias, purgeEntity) values (internalId, NEW.C_ID, entityId, entityIdPattern, entityType, NULL, NULL, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38],operations[39],operations[40],operations[41], csourceAlias, operations[42]);
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
							INSERT INTO csourceinformation(cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap, csource_alias, purgeEntity) VALUES (internalId, NEW.C_ID, NULL, NULL, NULL, NULL, attribName, location, scopes, expires, endpoint, tenant, headers, regMode, operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38],operations[39],operations[40],operations[41], csourceAlias, operations[42]);
						END LOOP;
					END IF;
					IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/relationshipNames' THEN
						FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/relationshipNames}') LOOP
							SELECT count(id)>0 INTO errorFound FROM ENTITY WHERE ENTITY ? attribName AND EXISTS (SELECT FROM jsonb_array_elements(ENTITY->attribName) as attribBody WHERE attribBody#>>'{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship');
							IF regMode > 1 AND errorFound THEN
								RAISE EXCEPTION 'Attribute % conflicts with existing entity', attribName USING ERRCODE='23514';
							END IF;
							INSERT INTO csourceinformation(cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap, csource_alias, purgeEntity) VALUES (internalId, NEW.C_ID, NULL, NULL, NULL, attribName, NULL, location, scopes, expires, endpoint, tenant, headers, regMode, operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36],operations[37],operations[38],operations[39],operations[40],operations[41], csourceAlias, operations[42]);
						END LOOP;
					END IF;
				END IF;
			END LOOP;
		END;
	END IF;
    RETURN NEW;
END;
$BODY$;
