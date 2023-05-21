package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.spatial4j.SpatialPredicate;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsPoint;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.commons.tools.SubscriptionTools;
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
			try {
				location = SubscriptionTools
						.getShape((Map<String, Object>) payload.get(NGSIConstants.NGSI_LD_LOCATION));
			} catch (ResponseException e) {
				location = null;
			}
		} else {
			location = null;
		}
		String[] scopes;
		if (payload.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
			scopes = getScopesFromPayload(payload.get(NGSIConstants.NGSI_LD_SCOPE));
		} else {
			scopes = null;
		}
		int mode = 1;
		if (payload.containsKey(NGSIConstants.NGSI_LD_REG_MODE)) {
			String modeText = ((List<Map<String, String>>) payload.get(NGSIConstants.NGSI_LD_REG_MODE)).get(0)
					.get(NGSIConstants.JSON_LD_VALUE);
			switch (modeText) {
			case NGSIConstants.NGSI_LD_REG_MODE_AUX:
				mode = 0;
				break;
			case NGSIConstants.NGSI_LD_REG_MODE_INC:
				mode = 1;
				break;
			case NGSIConstants.NGSI_LD_REG_MODE_RED:
				mode = 2;
				break;
			case NGSIConstants.NGSI_LD_REG_MODE_EXC:
				mode = 3;
				break;

			}
		}
		long tmpEexpiresAt = -1l;
		if (payload.containsKey(NGSIConstants.NGSI_LD_EXPIRES)) {
			tmpEexpiresAt = SerializationTools
					.date2Long(((List<Map<String, String>>) payload.get(NGSIConstants.NGSI_LD_EXPIRES)).get(0)
							.get(NGSIConstants.JSON_LD_VALUE));
		}
		RemoteHost remoteHost = new RemoteHost(host, tenant, headers, cSourceId, canDoSingleOp, canDoBatchOp, 0, false,
				false);

		boolean tmpCreateEntity = false;
		boolean tmpUpdateEntity = false;
		boolean tmpAppendAttrs = false;
		boolean tmpUpdateAttrs = false;
		boolean tmpDeleteAttrs = false;
		boolean tmpDeleteEntity = false;
		boolean tmpCreateBatch = false;
		boolean tmpUpsertBatch = false;
		boolean tmpUpdateBatch = false;
		boolean tmpDeleteBatch = false;
		boolean tmpUpsertTemporal = false;
		boolean tmpAppendAttrsTemporal = false;
		boolean tmpDeleteAttrsTemporal = false;
		boolean tmpUpdateAttrsTemporal = false;
		boolean tmpDeleteAttrInstanceTemporal = false;
		boolean tmpDeleteTemporal = false;
		boolean tmpMergeEntity = false;
		boolean tmpReplaceEntity = false;
		boolean tmpReplaceAttrs = false;
		boolean tmpMergeBatch = false;
		boolean tmpRetrieveEntity = false;
		boolean tmpQueryEntity = false;
		boolean tmpQueryBatch = false;
		boolean tmpRetrieveTemporal = false;
		boolean tmpQueryTemporal = false;
		boolean tmpRetrieveEntityTypes = false;
		boolean tmpRetrieveEntityTypeDetails = false;
		boolean tmpRetrieveEntityTypeInfo = false;
		boolean tmpRetrieveAttrTypes = false;
		boolean tmpRetrieveAttrTypeDetails = false;
		boolean tmpRetrieveAttrTypeInfo = false;
		boolean tmpCreateSubscription = false;
		boolean tmpUpdateSubscription = false;
		boolean tmpRetrieveSubscription = false;
		boolean tmpCanCompress = false;
		boolean tmpEntityMap = false;
		boolean tmpDeleteSubscription = false;
		boolean tmpQuerySubscription = false;
		if (payload.containsKey(NGSIConstants.NGSI_LD_REG_OPERATIONS)) {
			for (Map<String, String> opEntry : (List<Map<String, String>>) payload
					.get(NGSIConstants.NGSI_LD_REG_OPERATIONS)) {
				String operation = opEntry.get(NGSIConstants.JSON_LD_VALUE);
				switch (operation) {
				case NGSIConstants.NGSI_LD_REG_OPERATION_CREATEENTITY:
					tmpCreateEntity = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_UPDATEENTITY:
					tmpUpdateEntity = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_APPENDATTRS:
					tmpAppendAttrs = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_UPDATEATTRS:
					tmpUpdateAttrs = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_DELETEATTRS:
					tmpDeleteAttrs = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_DELETEENTITY:
					tmpDeleteEntity = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_CREATEBATCH:
					tmpCreateBatch = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_UPSERTBATCH:
					tmpUpsertBatch = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_UPDATEBATCH:
					tmpUpdateBatch = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_DELETEBATCH:
					tmpDeleteBatch = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_UPSERTTEMPORAL:
					tmpUpsertTemporal = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_APPENDATTRSTEMPORAL:
					tmpAppendAttrsTemporal = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_DELETEATTRSTEMPORAL:
					tmpDeleteAttrsTemporal = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_UPDATEATTRSTEMPORAL:
					tmpUpdateAttrsTemporal = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_DELETEATTRINSTANCETEMPORAL:
					tmpDeleteAttrInstanceTemporal = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_DELETETEMPORAL:
					tmpDeleteTemporal = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_MERGEENTITY:
					tmpMergeEntity = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_REPLACEENTITY:
					tmpReplaceEntity = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_REPLACEATTRS:
					tmpReplaceAttrs = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_MERGEBATCH:
					tmpMergeBatch = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_RETRIEVEENTITY:
					tmpRetrieveEntity = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_QUERYENTITY:
					tmpQueryEntity = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_QUERYBATCH:
					tmpQueryBatch = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_RETRIEVETEMPORAL:
					tmpRetrieveTemporal = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_QUERYTEMPORAL:
					tmpQueryTemporal = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_RETRIEVEENTITYTYPES:
					tmpRetrieveEntityTypes = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_RETRIEVEENTITYTYPEDETAILS:
					tmpRetrieveEntityTypeDetails = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_RETRIEVEENTITYTYPEINFO:
					tmpRetrieveEntityTypeInfo = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_RETRIEVEATTRTYPES:
					tmpRetrieveAttrTypes = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_RETRIEVEATTRTYPEDETAILS:
					tmpRetrieveAttrTypeDetails = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_RETRIEVEATTRTYPEINFO:
					tmpRetrieveAttrTypeInfo = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_CREATESUBSCRIPTION:
					tmpCreateSubscription = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_UPDATESUBSCRIPTION:
					tmpUpdateSubscription = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_RETRIEVESUBSCRIPTION:
					tmpRetrieveSubscription = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_QUERYSUBSCRIPTION:
					tmpQuerySubscription = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_DELETESUBSCRIPTION:
					tmpDeleteSubscription = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_ENTITYMAP:
					tmpEntityMap = true;
					break;
				case NGSIConstants.NGSI_LD_REG_OPERATION_CANCOMPRESS:
					tmpCanCompress = true;
					break;
				}
			}
		} else {
			tmpRetrieveEntity = true;
			tmpQueryEntity = true;
			tmpRetrieveEntityTypes = true;
			tmpRetrieveEntityTypeDetails = true;
			tmpRetrieveEntityTypeInfo = true;
			tmpRetrieveAttrTypes = true;
			tmpRetrieveAttrTypeDetails = true;
			tmpRetrieveAttrTypeInfo = true;
			tmpCreateSubscription = true;
			tmpUpdateSubscription = true;
			tmpRetrieveSubscription = true;
			tmpQuerySubscription = true;
			tmpDeleteSubscription = true;
		}
		for (Map<String, Object> infoEntry : (List<Map<String, Object>>) payload
				.get(NGSIConstants.NGSI_LD_INFORMATION)) {
			if (infoEntry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				for (Map<String, Object> entitiesEntry : (List<Map<String, Object>>) infoEntry
						.get(NGSIConstants.NGSI_LD_ENTITIES)) {
					for (String entityType : (List<String>) entitiesEntry.get(NGSIConstants.JSON_LD_TYPE)) {
						String tmpEId = null;
						String tmpEIdp = null;
						if (entitiesEntry.containsKey(NGSIConstants.JSON_LD_ID)) {
							tmpEId = (String) entitiesEntry.get(NGSIConstants.JSON_LD_ID);
						}
						if (entitiesEntry.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)) {
							tmpEIdp = ((List<Map<String, String>>) entitiesEntry.get(NGSIConstants.JSON_LD_ID)).get(0)
									.get(NGSIConstants.JSON_LD_VALUE);
						}

						boolean containsProps = infoEntry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES);
						boolean containsRels = infoEntry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS);
						if (containsProps || containsRels) {
							if (containsProps) {
								for (Map<String, String> prop : (List<Map<String, String>>) infoEntry
										.get(NGSIConstants.NGSI_LD_PROPERTIES)) {
									result.add(new RegistrationEntry(cSourceId, tmpEId, tmpEIdp, entityType,
											prop.get(NGSIConstants.JSON_LD_ID), null, location, scopes, tmpEexpiresAt,
											mode, tmpCreateEntity, tmpUpdateEntity, tmpAppendAttrs, tmpUpdateAttrs,
											tmpDeleteAttrs, tmpDeleteEntity, tmpCreateBatch, tmpUpsertBatch,
											tmpUpdateBatch, tmpDeleteBatch, tmpUpsertTemporal, tmpAppendAttrsTemporal,
											tmpDeleteAttrsTemporal, tmpUpdateAttrsTemporal,
											tmpDeleteAttrInstanceTemporal, tmpDeleteTemporal, tmpMergeEntity,
											tmpReplaceEntity, tmpReplaceAttrs, tmpMergeBatch, tmpRetrieveEntity,
											tmpQueryEntity, tmpQueryBatch, tmpRetrieveTemporal, tmpQueryTemporal,
											tmpRetrieveEntityTypes, tmpRetrieveEntityTypeDetails,
											tmpRetrieveEntityTypeInfo, tmpRetrieveAttrTypes, tmpRetrieveAttrTypeDetails,
											tmpRetrieveAttrTypeInfo, tmpCreateSubscription, tmpUpdateSubscription,
											tmpRetrieveSubscription, tmpQuerySubscription, tmpDeleteSubscription,
											tmpEntityMap, tmpCanCompress, remoteHost));
								}
							}
							if (containsRels) {
								for (Map<String, String> rel : (List<Map<String, String>>) infoEntry
										.get(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
									result.add(new RegistrationEntry(cSourceId, tmpEId, tmpEIdp, entityType, null,
											rel.get(NGSIConstants.JSON_LD_ID), location, scopes, tmpEexpiresAt, mode,
											tmpCreateEntity, tmpUpdateEntity, tmpAppendAttrs, tmpUpdateAttrs,
											tmpDeleteAttrs, tmpDeleteEntity, tmpCreateBatch, tmpUpsertBatch,
											tmpUpdateBatch, tmpDeleteBatch, tmpUpsertTemporal, tmpAppendAttrsTemporal,
											tmpDeleteAttrsTemporal, tmpUpdateAttrsTemporal,
											tmpDeleteAttrInstanceTemporal, tmpDeleteTemporal, tmpMergeEntity,
											tmpReplaceEntity, tmpReplaceAttrs, tmpMergeBatch, tmpRetrieveEntity,
											tmpQueryEntity, tmpQueryBatch, tmpRetrieveTemporal, tmpQueryTemporal,
											tmpRetrieveEntityTypes, tmpRetrieveEntityTypeDetails,
											tmpRetrieveEntityTypeInfo, tmpRetrieveAttrTypes, tmpRetrieveAttrTypeDetails,
											tmpRetrieveAttrTypeInfo, tmpCreateSubscription, tmpUpdateSubscription,
											tmpRetrieveSubscription, tmpQuerySubscription, tmpDeleteSubscription,
											tmpEntityMap, tmpCanCompress, remoteHost));
								}
							}
						} else {
							result.add(new RegistrationEntry(cSourceId, tmpEId, tmpEIdp, entityType, null, null,
									location, scopes, tmpEexpiresAt, mode, tmpCreateEntity, tmpUpdateEntity,
									tmpAppendAttrs, tmpUpdateAttrs, tmpDeleteAttrs, tmpDeleteEntity, tmpCreateBatch,
									tmpUpsertBatch, tmpUpdateBatch, tmpDeleteBatch, tmpUpsertTemporal,
									tmpAppendAttrsTemporal, tmpDeleteAttrsTemporal, tmpUpdateAttrsTemporal,
									tmpDeleteAttrInstanceTemporal, tmpDeleteTemporal, tmpMergeEntity, tmpReplaceEntity,
									tmpReplaceAttrs, tmpMergeBatch, tmpRetrieveEntity, tmpQueryEntity, tmpQueryBatch,
									tmpRetrieveTemporal, tmpQueryTemporal, tmpRetrieveEntityTypes,
									tmpRetrieveEntityTypeDetails, tmpRetrieveEntityTypeInfo, tmpRetrieveAttrTypes,
									tmpRetrieveAttrTypeDetails, tmpRetrieveAttrTypeInfo, tmpCreateSubscription,
									tmpUpdateSubscription, tmpRetrieveSubscription, tmpQuerySubscription,
									tmpDeleteSubscription, tmpEntityMap, tmpCanCompress, remoteHost));
						}

					}
				}
			} else {
				boolean containsProps = infoEntry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES);
				boolean containsRels = infoEntry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS);

				if (containsProps) {
					for (Map<String, String> prop : (List<Map<String, String>>) infoEntry
							.get(NGSIConstants.NGSI_LD_PROPERTIES)) {
						result.add(new RegistrationEntry(cSourceId, null, null, null,
								prop.get(NGSIConstants.JSON_LD_ID), null, location, scopes, tmpEexpiresAt, mode,
								tmpCreateEntity, tmpUpdateEntity, tmpAppendAttrs, tmpUpdateAttrs, tmpDeleteAttrs,
								tmpDeleteEntity, tmpCreateBatch, tmpUpsertBatch, tmpUpdateBatch, tmpDeleteBatch,
								tmpUpsertTemporal, tmpAppendAttrsTemporal, tmpDeleteAttrsTemporal,
								tmpUpdateAttrsTemporal, tmpDeleteAttrInstanceTemporal, tmpDeleteTemporal,
								tmpMergeEntity, tmpReplaceEntity, tmpReplaceAttrs, tmpMergeBatch, tmpRetrieveEntity,
								tmpQueryEntity, tmpQueryBatch, tmpRetrieveTemporal, tmpQueryTemporal,
								tmpRetrieveEntityTypes, tmpRetrieveEntityTypeDetails, tmpRetrieveEntityTypeInfo,
								tmpRetrieveAttrTypes, tmpRetrieveAttrTypeDetails, tmpRetrieveAttrTypeInfo,
								tmpCreateSubscription, tmpUpdateSubscription, tmpRetrieveSubscription,
								tmpQuerySubscription, tmpDeleteSubscription, tmpEntityMap, tmpCanCompress, remoteHost));
					}
				}
				if (containsRels) {
					for (Map<String, String> rel : (List<Map<String, String>>) infoEntry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
						result.add(new RegistrationEntry(cSourceId, null, null, null, null,
								rel.get(NGSIConstants.JSON_LD_ID), location, scopes, tmpEexpiresAt, mode,
								tmpCreateEntity, tmpUpdateEntity, tmpAppendAttrs, tmpUpdateAttrs, tmpDeleteAttrs,
								tmpDeleteEntity, tmpCreateBatch, tmpUpsertBatch, tmpUpdateBatch, tmpDeleteBatch,
								tmpUpsertTemporal, tmpAppendAttrsTemporal, tmpDeleteAttrsTemporal,
								tmpUpdateAttrsTemporal, tmpDeleteAttrInstanceTemporal, tmpDeleteTemporal,
								tmpMergeEntity, tmpReplaceEntity, tmpReplaceAttrs, tmpMergeBatch, tmpRetrieveEntity,
								tmpQueryEntity, tmpQueryBatch, tmpRetrieveTemporal, tmpQueryTemporal,
								tmpRetrieveEntityTypes, tmpRetrieveEntityTypeDetails, tmpRetrieveEntityTypeInfo,
								tmpRetrieveAttrTypes, tmpRetrieveAttrTypeDetails, tmpRetrieveAttrTypeInfo,
								tmpCreateSubscription, tmpUpdateSubscription, tmpRetrieveSubscription,
								tmpQuerySubscription, tmpDeleteSubscription, tmpEntityMap, tmpCanCompress, remoteHost));
					}
				}
			}
		}

		return result;
	}

	private static String[] getScopesFromPayload(Object object) {
		List<Map<String, String>> list = (List<Map<String, String>>) object;
		String[] result = new String[list.size()];
		int i = 0;
		for (Map<String, String> entry : list) {
			result[i] = entry.get(NGSIConstants.JSON_LD_VALUE);
			i++;
		}
		return result;
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
			if (geoQuery.getGeoproperty().equals(NGSIConstants.NGSI_LD_LOCATION)) {
				Shape geoShape = geoQuery.getShape();
				if (location == null) {
					result.setGeo(geoShape);
				} else {
					switch (geoQuery.getGeorel()) {
					case NGSIConstants.GEO_REL_EQUALS:
						result.setGeoOp(NGSIConstants.GEO_REL_EQUALS);
						switch (geoQuery.getGeometry()) {
						case NGSIConstants.GEO_TYPE_POINT:
							if (location instanceof JtsPoint) {
								if (SpatialPredicate.IsEqualTo.evaluate(location, geoShape)) {
									result.setGeo(geoShape);
								} else {
									return null;
								}
							} else {
								if (SpatialPredicate.IsWithin.evaluate(location, geoShape)) {
									result.setGeo(geoShape);
								} else {
									return null;
								}
							}
							break;
						case NGSIConstants.GEO_TYPE_LINESTRING:
						case NGSIConstants.GEO_TYPE_MULTI_LINESTRING:
							if (location instanceof JtsPoint) {
								// point can never be queried for equality with a string
								return null;
							} else {
								if (SpatialPredicate.IsWithin.evaluate(location, geoShape)) {
									result.setGeo(geoShape);
								} else {
									return null;
								}
							}
							break;
						case NGSIConstants.GEO_TYPE_POLYGON:
						case NGSIConstants.GEO_TYPE_MULTI_POLYGON:
							if (location instanceof JtsPoint || ((JtsGeometry) location).getGeom() instanceof LineString
									|| ((JtsGeometry) location).getGeom() instanceof MultiLineString) {
								// point can never be queried for equality with a string
								return null;
							} else {
								if (SpatialPredicate.IsWithin.evaluate(location, geoShape)) {
									result.setGeo(geoShape);
								} else {
									return null;
								}
							}
							break;
						default:
							return null;
						}
						break;
					case NGSIConstants.GEO_REL_NEAR:
						Shape toCheck = geoShape;
						if (geoQuery.getDistanceType().equals(NGSIConstants.GEO_REL_MIN_DISTANCE)) {
							toCheck = new JtsGeometry(((Geometry) toCheck).reverse(), JtsSpatialContext.GEO, true,
									true);
						}
						if (location instanceof JtsPoint) {
							if (SpatialPredicate.IsWithin.evaluate(toCheck, location)) {
								result.setGeo(location);
								result.setGeoOp(NGSIConstants.GEO_REL_EQUALS);
							} else {
								return null;
							}
						} else {
							if (SpatialPredicate.IsWithin.evaluate(toCheck, location)) {
								result.setGeo(geoShape);
								result.setGeoOp(NGSIConstants.GEO_REL_NEAR);
							} else if (SpatialPredicate.IsWithin.evaluate(location, toCheck)) {
								result.setGeo(location);
								result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
							} else if (SpatialPredicate.Intersects.evaluate(location, toCheck)) {
								Geometry geom1 = ((JtsGeometry) toCheck).getGeom();
								Geometry geom2 = ((JtsGeometry) location).getGeom();
								Geometry intersection = geom1.intersection(geom2);
								result.setGeo(new JtsGeometry(intersection, JtsSpatialContext.GEO, true, true));
								result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
							} else {

								return null;
							}
						}
						break;
					case NGSIConstants.GEO_REL_WITHIN:
						if (location instanceof JtsPoint) {
							if (SpatialPredicate.IsWithin.evaluate(geoShape, location)) {
								result.setGeo(location);
								result.setGeoOp(NGSIConstants.GEO_REL_EQUALS);
							} else {
								return null;
							}
						} else {
							if (SpatialPredicate.IsWithin.evaluate(geoShape, location)) {
								result.setGeo(geoShape);
								result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
							} else if (SpatialPredicate.IsWithin.evaluate(location, geoShape)) {
								result.setGeo(location);
								result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
							} else if (SpatialPredicate.Intersects.evaluate(location, geoShape)) {
								Geometry geom1 = ((JtsGeometry) geoShape).getGeom();
								Geometry geom2 = ((JtsGeometry) location).getGeom();
								Geometry intersection = geom1.intersection(geom2);
								result.setGeo(new JtsGeometry(intersection, JtsSpatialContext.GEO, true, true));
								result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
							} else {
								return null;
							}
						}

						break;
					case NGSIConstants.GEO_REL_CONTAINS:
						if (location instanceof JtsPoint) {
							if (SpatialPredicate.Contains.evaluate(geoShape, location)) {
								result.setGeo(location);
								result.setGeoOp(NGSIConstants.GEO_REL_EQUALS);
							} else {
								return null;
							}
						} else {
							if (SpatialPredicate.IsWithin.evaluate(geoShape, location)) {
								result.setGeo(geoShape);
								result.setGeoOp(NGSIConstants.GEO_REL_CONTAINS);
							} else if (SpatialPredicate.IsWithin.evaluate(location, geoShape)) {
								result.setGeo(location);
								result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
							} else if (SpatialPredicate.Intersects.evaluate(location, geoShape)) {
								Geometry geom1 = ((JtsGeometry) geoShape).getGeom();
								Geometry geom2 = ((JtsGeometry) location).getGeom();
								Geometry intersection = geom1.intersection(geom2);
								result.setGeo(new JtsGeometry(intersection, JtsSpatialContext.GEO, true, true));
								result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
							} else {
								return null;
							}
						}
						break;
					case NGSIConstants.GEO_REL_INTERSECTS:
						if (location instanceof JtsPoint) {
							if (SpatialPredicate.Intersects.evaluate(geoShape, location)) {
								result.setGeo(location);
								result.setGeoOp(NGSIConstants.GEO_REL_EQUALS);
							} else {
								return null;
							}
						} else {
							if (SpatialPredicate.IsWithin.evaluate(geoShape, location)) {
								result.setGeo(geoShape);
								result.setGeoOp(NGSIConstants.GEO_REL_INTERSECTS);
							} else if (SpatialPredicate.IsWithin.evaluate(location, geoShape)) {
								result.setGeo(location);
								result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
							} else if (SpatialPredicate.Intersects.evaluate(location, geoShape)) {
								Geometry geom1 = ((JtsGeometry) geoShape).getGeom();
								Geometry geom2 = ((JtsGeometry) location).getGeom();
								Geometry intersection = geom1.intersection(geom2);
								result.setGeo(new JtsGeometry(intersection, JtsSpatialContext.GEO, true, true));
								result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
							} else {
								return null;
							}
						}
						break;
					case NGSIConstants.GEO_REL_DISJOINT:
						if (location instanceof Point) {
							if (SpatialPredicate.IsDisjointTo.evaluate(geoShape, location)) {
								result.setGeo(location);
								result.setGeoOp(NGSIConstants.GEO_REL_EQUALS);
							} else {
								return null;
							}
						} else {
							if (SpatialPredicate.IsDisjointTo.evaluate(geoShape, location)) {
								result.setGeo(location);
								result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
							} else {
								Geometry geom1 = ((JtsGeometry) geoShape).getGeom().reverse();
								Geometry geom2 = ((JtsGeometry) location).getGeom();
								if (geom1.intersects(geom2)) {
									Geometry intersection = geom1.intersection(geom2);
									result.setGeo(new JtsGeometry(intersection, JtsSpatialContext.GEO, true, true));
									result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
								} else {
									return null;
								}

							}
						}
						break;
					case NGSIConstants.GEO_REL_OVERLAPS:
						if (location instanceof Point) {
							if (SpatialPredicate.Intersects.evaluate(geoShape, location)) {
								result.setGeo(location);
								result.setGeoOp(NGSIConstants.GEO_REL_EQUALS);
							} else {
								return null;
							}
						} else {
							if (SpatialPredicate.IsWithin.evaluate(geoShape, location)) {
								result.setGeo(geoShape);
								result.setGeoOp(NGSIConstants.GEO_REL_OVERLAPS);
							} else if (SpatialPredicate.IsWithin.evaluate(location, geoShape)) {
								result.setGeo(location);
								result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
							} else if (SpatialPredicate.Intersects.evaluate(location, geoShape)) {
								Geometry geom1 = ((JtsGeometry) geoShape).getGeom();
								Geometry geom2 = ((JtsGeometry) location).getGeom();
								Geometry intersection = geom1.intersection(geom2);
								result.setGeo(new JtsGeometry(intersection, JtsSpatialContext.GEO, true, true));
								result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
							} else {
								return null;
							}
						}
						break;
					default:
						break;

					}

				}
			}
		} else {
			if (location != null) {
				result.setGeo(location);
			}
		}
		return result;

	}

}
