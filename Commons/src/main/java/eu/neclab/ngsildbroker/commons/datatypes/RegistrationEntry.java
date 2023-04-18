package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.SpatialPredicate;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.core.MultiMap;

public record RegistrationEntry(String cId, String eId, String eIdp, String type, String eProp, String eRel,
		Shape location, String[] scopes, long expiresAt, int regMode, boolean createEntity, boolean updateEntity,
		boolean appendAttrs, boolean updateAttrs, boolean deleteAttrs, boolean deleteEntity, boolean createBatch,
		boolean upsertBatch, boolean updateBatch, boolean deleteBatch, boolean upsertTemporal,
		boolean appendAttrsTemporal, boolean deleteAttrsTemporal, boolean updateAttrsTemporal,
		boolean deleteAttrInstanceTemporal, boolean deleteTemporal, boolean mergeEntity, boolean replaceEntity,
		boolean replaceAttrs, boolean mergeBatch, boolean retrieveEntity, boolean queryEntity, boolean queryBatch,
		boolean retrieveTemporal, boolean queryTemporal, boolean retrieveEntityTypes, boolean retrieveEntityTypeDetails,
		boolean retrieveEntityTypeInfo, boolean retrieveAttrTypes, boolean retrieveAttrTypeDetails,
		boolean retrieveAttrTypeInfo, boolean createSubscription, boolean updateSubscription,
		boolean retrieveSubscription, boolean querySubscription, boolean deleteSubscription, boolean canDoIdQuery,
		boolean canDoZip, RemoteHost host) {

	/**
	 * 
	 * @param id
	 * @param type
	 * @param prop
	 * @param rel
	 * @param originalScopes
	 * @return Matching types and matching scopes if no scopes are in the registry-
	 *         the original scopes will be returned scopes can be null if there no
	 *         scopes in the entity if no match is possible it will return null
	 */
	public Tuple2<Set<String>, Set<String>> matches(String id, List<String> types, String prop, String rel,
			Object originalScopes, Shape location) {
		if (id != null && this.eId != null && !id.equals(eId)) {
			return null;
		}
		if (this.eIdp != null && !id.matches(eIdp)) {
			return null;
		}
		if (prop != null && eProp != null && !prop.equals(eProp)) {
			return null;
		}
		if (rel != null && eRel != null && !rel.equals(eRel)) {
			return null;
		}
		if ((location != null && this.location == null) || (location != null && this.location != null
				&& SpatialPredicate.IsWithin.evaluate(location, this.location))) {
			return null;
		}
		if (this.type != null && types != null && !types.contains(this.type)) {
			return null;
		}
		if (this.scopes != null && originalScopes == null) {
			return null;
		}
		Set<String> resultScopes = getOverlap((List<Map<String, String>>) originalScopes);
		Set<String> resultType = null;
		if (this.type == null && types != null) {
			resultType = Sets.newHashSet(types);
		} else if (types != null) {
			resultType = Sets.newHashSet(this.type);
		}

		return Tuple2.of(resultType, resultScopes);
	}

	private Set<String> getOverlap(List<Map<String, String>> originalScopes) {
		Set<String> result = Sets.newHashSet();
		for (Map<String, String> scopeEntry : originalScopes) {
			String scope = scopeEntry.get(NGSIConstants.JSON_LD_VALUE);
			if (this.scopes == null || ArrayUtils.contains(this.scopes, scope)) {
				result.add(scope);
			}
		}
		return result;
	}

	public static List<RegistrationEntry> fromRegPayload(Map<String, Object> payload) {
		List<RegistrationEntry> result = Lists.newArrayList();
		boolean canDoSingleOp = false;
		boolean canDoBatchOp = false;
		String host = (String) ((List<Map<String, Object>>) payload.get(NGSIConstants.NGSI_LD_ENDPOINT)).get(0)
				.get(NGSIConstants.JSON_LD_VALUE);
		String tenant;
		if (payload.containsKey(NGSIConstants.NGSI_LD_TENANT)) {
			tenant = (String) ((List<Map<String, Object>>) payload.get(NGSIConstants.NGSI_LD_TENANT)).get(0)
					.get(NGSIConstants.JSON_LD_VALUE);
		} else {
			tenant = AppConstants.INTERNAL_NULL_KEY;
		}
		String cSourceId = (String) payload.get(NGSIConstants.JSON_LD_ID);
		MultiMap headers = null;
		Shape location;
		if (payload.containsKey(NGSIConstants.NGSI_LD_LOCATION)) {
			location = getShapeFromLocation(payload.get(NGSIConstants.NGSI_LD_LOCATION));
		} else {
			location = null;
		}
		String[] scopes;
		if (payload.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
			scopes = getScopesFromPayload(payload.get(NGSIConstants.NGSI_LD_SCOPE));
		} else {
			scopes = null;
		}
		int mode;

		RemoteHost remoteHost = new RemoteHost(host, tenant, headers, cSourceId, canDoSingleOp, canDoBatchOp, 0, false,
				false);
//		new RegistrationEntry(null, null, null, null, null, null, null, null, 0, 0, false, false, false, false, false,
//				false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//				false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
//				false, null);
//		
//			
//			IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/operations') THEN
//				operations = getOperations(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/operations}');
//			ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/operations') THEN
//				operations = getOperations(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/default-context/operations}');
//			ELSE
//				operations = array[false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,false,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true,true]::boolean[];
//			END IF;
//
//			IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/contextSourceInfo') THEN
//				headers = NEW.REG#>'{https://uri.etsi.org/ngsi-ld/contextSourceInfo}';
//			ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/contextSourceInfo') THEN
//				headers = NEW.REG#>'{https://uri.etsi.org/ngsi-ld/default-context/contextSourceInfo}';
//			ELSE
//				headers = NULL;
//			END IF;
//
//			IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/mode') THEN
//				regMode = getMode(NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/mode,0,@value}');
//			ELSIF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/default-context/mode') THEN
//				regMode = getMode(NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/default-context/mode,0,@value}');
//			ELSE
//				regMode = 1;
//			END IF;
//			
//			IF (NEW.REG ? 'https://uri.etsi.org/ngsi-ld/expires') THEN
//				expires = (NEW.REG#>>'{https://uri.etsi.org/ngsi-ld/expires,0,@value}')::TIMESTAMP;
//			ELSE
//				expires = NULL;
//			END IF;
//			BEGIN
//				IF TG_OP = 'UPDATE' THEN
//					DELETE FROM csourceinformation where cs_id = NEW.id;
//				END IF;
//				FOR infoEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(NEW.REG#>'{https://uri.etsi.org/ngsi-ld/information}') LOOP
//					IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/entities' THEN
//						FOR entitiesEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/entities}') LOOP
//							FOR entityType IN SELECT jsonb_array_elements_text FROM jsonb_array_elements_text(entitiesEntry#>'{@type}') LOOP
//								entityId := NULL;
//								entityIdPattern := NULL;
//								attribsAdded := false;
//								IF entitiesEntry ? '@id' THEN
//									entityId = entitiesEntry#>>'{@id}';
//								END IF;
//								IF entitiesEntry ? 'https://uri.etsi.org/ngsi-ld/idPattern' THEN
//									entityIdPattern = entitiesEntry#>>'{https://uri.etsi.org/ngsi-ld/idPattern,0,@value}';
//								END IF;
//								IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/propertyNames' THEN
//									attribsAdded = true;
//									FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/propertyNames}') LOOP
//										IF regMode > 1 THEN
//											IF entityId IS NOT NULL THEN 
//												WITH iids AS (SELECT id FROM ENTITY, etype2iid WHERE e_id = entityId AND ENTITY.id = etype2iid.iid AND etype2iid.e_type = entityType) SELECT count(attr2iid.iid)>0 INTO errorFound FROM iids left join attr2iid on iids.iid = attr2iid.iid WHERE attr2iid.attr = attribName AND NOT attr2iid.is_rel;
//												IF errorFound THEN
//													RAISE EXCEPTION 'Registration with attrib % and id % conflicts with existing entry', attribName, entityId USING ERRCODE='23514';
//												END IF;
//											ELSIF entityIdPattern IS NOT NULL THEN
//												WITH iids AS (SELECT id FROM ENTITY, etype2iid WHERE e_id ~ entityIdPattern AND ENTITY.id = etype2iid.iid AND etype2iid.e_type = entityType) SELECT count(attr2iid.iid)>0 INTO errorFound FROM iids left join attr2iid on iids.iid = attr2iid.iid WHERE attr2iid.attr = attribName AND NOT attr2iid.is_rel;
//												IF errorFound THEN
//													RAISE EXCEPTION 'Registration with attrib % and idpattern % conflicts with existing entry', attribName, entityIdPattern USING ERRCODE='23514';
//												END IF;
//											ELSE
//												WITH iids AS (SELECT iid FROM etype2iid WHERE e_type = entityType) SELECT count(attr2iid.iid)>0 INTO errorFound FROM iids left join attr2iid on iids.iid = attr2iid.iid WHERE attr2iid.attr = attribName AND NOT attr2iid.is_rel;
//												IF errorFound THEN
//													RAISE EXCEPTION 'Registration with attrib % and type % conflicts with existing entry', attribName, entityType USING ERRCODE='23514';
//												END IF;
//											END IF;
//										END IF;
//										INSERT INTO csourceinformation (cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription) VALUES (internalId, NEW.C_ID, entityId, entityIdPattern, entityType, attribName, NULL, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36]);
//									END LOOP;
//								END IF;
//								IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/relationshipNames' THEN
//									attribsAdded = true;
//									FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/relationshipNames}') LOOP
//										IF regMode > 1 THEN
//											IF entityId IS NOT NULL THEN 
//												WITH iids AS (SELECT id FROM ENTITY, etype2iid WHERE e_id = entityId AND ENTITY.id = etype2iid.iid AND etype2iid.e_type = entityType) SELECT count(attr2iid.iid)>0 INTO errorFound FROM iids left join attr2iid on iids.iid = attr2iid.iid WHERE attr2iid.attr = attribName AND attr2iid.is_rel;
//												IF errorFound THEN
//													RAISE EXCEPTION 'Registration with attrib % and id % conflicts with existing entry', attribName, entityId USING ERRCODE='23514';
//												END IF;
//											ELSIF entityIdPattern IS NOT NULL THEN
//												WITH iids AS (SELECT id FROM ENTITY, etype2iid WHERE e_id ~ entityIdPattern AND ENTITY.id = etype2iid.iid AND etype2iid.e_type = entityType) SELECT count(attr2iid.iid)>0 INTO errorFound FROM iids left join attr2iid on iids.iid = attr2iid.iid WHERE attr2iid.attr = attribName AND attr2iid.is_rel;
//												IF errorFound THEN
//													RAISE EXCEPTION 'Registration with attrib % and idpattern % conflicts with existing entry', attribName, entityIdPattern USING ERRCODE='23514';
//												END IF;
//											ELSE
//												WITH iids AS (SELECT iid FROM etype2iid WHERE e_type = entityType) SELECT count(attr2iid.iid)>0 INTO errorFound FROM iids left join attr2iid on iids.iid = attr2iid.iid WHERE attr2iid.attr = attribName AND attr2iid.is_rel;
//												IF errorFound THEN
//													RAISE EXCEPTION 'Registration with attrib % and type % conflicts with existing entry', attribName, entityType USING ERRCODE='23514';
//												END IF;
//											END IF;
//										END IF;
//										INSERT INTO csourceinformation (cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription) VALUES (internalId, NEW.C_ID, entityId, entityIdPattern, entityType,NULL, attribName, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36]);
//										
//									END LOOP;
//								END IF;
//								IF NOT attribsAdded THEN
//									IF regMode > 1 THEN
//										IF entityId IS NOT NULL THEN 
//											WITH e_ids AS (SELECT id FROM entity WHERE e_id = entityId) SELECT count(iid) INTO errorFound FROM etype2iid  WHERE etype2iid.e_type = entityType;
//											IF errorFound THEN
//												RAISE EXCEPTION 'Registration with entityId % conflicts with existing entity', entityId USING ERRCODE='23514';
//											END IF;
//										ELSIF entityIdPattern IS NOT NULL THEN
//											WITH e_ids AS (SELECT id FROM entity WHERE e_id ~ entityIdPattern) SELECT count(iid)>0 INTO errorFound FROM etype2iid LEFT JOIN e_ids ON etype2iid.iid = e_ids.id WHERE etype2iid.e_type = entityType;
//											IF errorFound THEN
//												RAISE EXCEPTION 'Registration with idPattern % and type % conflicts with existing entity', entityIdPattern, entityType USING ERRCODE='23514';
//											END IF;
//										ELSE
//											SELECT count(iid)>0 INTO errorFound FROM etype2iid WHERE e_type = entityType;
//											IF errorFound THEN
//												RAISE EXCEPTION 'Registration with type % conflicts with existing entity', entityType USING ERRCODE='23514';
//											END IF;
//										END IF;
//									END IF;
//									INSERT INTO csourceinformation(cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription) values (internalId, NEW.C_ID, entityId, entityIdPattern, entityType, NULL, NULL, location, scopes, expires, endpoint, tenant, headers, regMode,operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36]);
//								END IF;
//							END LOOP;
//						END LOOP;
//					ELSE
//						IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/propertyNames' THEN
//							FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/propertyNames}') LOOP
//								SELECT count(attr2iid.iid)>0 INTO errorFound FROM attr2iid WHERE attr2iid.attr = attribName AND NOT attr2iid.is_rel;
//								IF regMode > 1 AND errorFound THEN
//									RAISE EXCEPTION 'Attribute % conflicts with existing entity', attribName USING ERRCODE='23514';
//								END IF;
//								INSERT INTO csourceinformation(cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription) VALUES (internalId, NEW.C_ID, NULL, NULL, NULL, attribName, NULL, location, scopes, expires, endpoint, tenant, headers, regMode, operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36]);
//							END LOOP;
//						END IF;
//						IF infoEntry ? 'https://uri.etsi.org/ngsi-ld/relationshipNames' THEN
//							FOR attribName IN SELECT value#>>'{@id}' FROM jsonb_array_elements(infoEntry#>'{https://uri.etsi.org/ngsi-ld/relationshipNames}') LOOP
//								SELECT count(attr2iid.iid)>0 INTO errorFound FROM attr2iid WHERE attr2iid.attr = attribName AND attr2iid.is_rel;
//								IF regMode > 1 AND errorFound THEN
//									RAISE EXCEPTION 'Attribute % conflicts with existing entity', attribName USING ERRCODE='23514';
//								END IF;
//								INSERT INTO csourceinformation(cs_id, c_id, e_id, e_id_p, e_type, e_rel, e_prop, i_location, scopes, expires, endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription) VALUES (internalId, NEW.C_ID, NULL, NULL, NULL, NULL, attribName, location, scopes, expires, endpoint, tenant, headers, regMode, operations[1],operations[2],operations[3],operations[4],operations[5],operations[6],operations[7],operations[8],operations[9],operations[10],operations[11],operations[12],operations[13],operations[14],operations[15],operations[16],operations[17],operations[18],operations[19],operations[20],operations[21],operations[22],operations[23],operations[24],operations[25],operations[26],operations[27],operations[28],operations[29],operations[30],operations[31],operations[32],operations[33],operations[34],operations[35],operations[36]);
//							END LOOP;
//						END IF;
//					END IF;
//				END LOOP;
//			END;
//		END IF;
//	    RETURN NEW;
//	END;
//	$BODY$;

		return result;
	}

	private static String[] getScopesFromPayload(Object object) {
		// TODO Auto-generated method stub
		return null;
	}

	public QueryInfos matches(String[] id, String idPattern, TypeQueryTerm typeQuery, AttrsQueryTerm attrsQuery,
			QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery) {
		QueryInfos result = new QueryInfos();
		Set<String> idSet;
		if (id != null) {
			idSet = Sets.newHashSet(id);
		} else {
			idSet = new HashSet<>(0);
		}

		if (!idSet.isEmpty()) {
			if (eId != null) {
				if (idSet.contains(eId)) {
					result.addId(eId);
				} else {
					return null;
				}
			} else if (eIdp != null) {
				boolean matchFound = false;
				for (String entry : idSet) {
					if (entry.matches(eIdp)) {
						matchFound = true;
						result.addAttr(entry);
					}
				}
				if (!matchFound) {
					return null;
				}
			} else {
				result.getIds().addAll(idSet);
			}
		} else {
			if (eId != null) {
				result.addId(eId);
			}
		}

		if (idPattern != null) {
			if (eId == null) {
				result.setIdPattern(idPattern);
			} else {
				if (idPattern.matches(eId)) {
					result.addId(eId);
				} else {
					return null;
				}
			}
		}
		if (typeQuery != null) {
			if (type != null) {
				if (typeQuery.getAllTypes().contains(type)) {
					result.addType(type);

				} else {
					return null;
				}
			} else {
				result.getTypes().addAll(typeQuery.getAllTypes());
				result.setFullTypesFound(true);
			}
		} else {
			if (type != null) {
				result.addType(type);
			}
		}
		if (attrsQuery != null) {
			if (eProp == null && eRel == null) {
				result.getAttrs().addAll(attrsQuery.getAttrs());
				result.setFullAttrsFound(true);
			} else if (eProp != null) {
				if (attrsQuery.getAttrs().contains(eProp)) {
					result.addAttr(eProp);
				} else {
					return null;
				}
			} else {
				if (attrsQuery.getAttrs().contains(eRel)) {
					result.addAttr(eRel);
				} else {
					return null;
				}
			}
		} else {
			if (eProp != null) {
				result.addAttr(eProp);
			}
			if (eRel != null) {
				result.addAttr(eRel);
			}
		}
		if (geoQuery != null) {
			Shape geoShape = geoQuery.getShape();
			if (location == null) {
				result.setGeo(geoShape);
			} else {

				if (SpatialPredicate.IsEqualTo.evaluate(geoShape, location)) {
					result.setGeo(geoShape);
				} else if (SpatialPredicate.IsWithin.evaluate(geoShape, location)) {
					result.setGeo(geoShape);
				} else if (SpatialPredicate.IsWithin.evaluate(location, geoShape)) {
					result.setGeo(location);
				} else if (SpatialPredicate.Intersects.evaluate(geoShape, location)) {
					if (!(geoShape instanceof JtsGeometry) || !(location instanceof JtsGeometry)) {
						return null;
					}
					Geometry geom1 = ((JtsGeometry) geoShape).getGeom();
					Geometry geom2 = ((JtsGeometry) location).getGeom();
					Geometry intersection = geom1.intersection(geom2);
					result.setGeo(new JtsGeometry(intersection, JtsSpatialContext.GEO, true, true));
				} else {
					return null;
				}
			}

		} else {
			if (location != null) {
				result.setGeo(location);
			}
		}
		return result;

	}

	private static Shape getShapeFromLocation(Object object) {
		// TODO Auto-generated method stub
		return null;
	}

}
