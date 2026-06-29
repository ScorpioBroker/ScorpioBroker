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

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
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
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.commons.tools.SubscriptionTools;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.core.MultiMap;

@SuppressWarnings("unchecked")
public class RegistrationEntry {
	String cId;
	String eId;
	String eIdp;
	String type;
	String eProp;
	String eRel;
	Shape location;
	String[] scopes;
	long expiresAt;
	int regMode;
	boolean createEntity;
	boolean updateEntity;
	boolean appendAttrs;
	boolean updateAttrs;
	boolean deleteAttrs;
	boolean deleteEntity;
	boolean createBatch;
	boolean upsertBatch;
	boolean updateBatch;
	boolean deleteBatch;
	boolean upsertTemporal;
	boolean appendAttrsTemporal;
	boolean deleteAttrsTemporal;
	boolean updateAttrsTemporal;
	boolean deleteAttrInstanceTemporal;
	boolean deleteTemporal;
	boolean mergeEntity;
	boolean replaceEntity;
	boolean replaceAttrs;
	boolean mergeBatch;
	boolean retrieveEntity;
	boolean queryEntity;
	boolean queryBatch;
	boolean retrieveTemporal;
	boolean queryTemporal;
	boolean retrieveEntityTypes;
	boolean retrieveEntityTypeDetails;
	boolean retrieveEntityTypeInfo;
	boolean retrieveAttrTypes;
	boolean retrieveAttrTypeDetails;
	boolean retrieveAttrTypeInfo;
	boolean createSubscription;
	boolean updateSubscription;
	boolean retrieveSubscription;
	boolean querySubscription;
	boolean deleteSubscription;
	boolean queryEntityMap;
	boolean createEntityMap;
	boolean updateEntityMap;
	boolean deleteEntityMap;
	boolean retrieveEntityMap;
	RemoteHost host;
	Context context;
	Map<String, Object> registration;

	public RegistrationEntry(String cId, String eId, String eIdp, String type, String eProp, String eRel,
			Shape location, String[] scopes, long expiresAt, int regMode, boolean createEntity, boolean updateEntity,
			boolean appendAttrs, boolean updateAttrs, boolean deleteAttrs, boolean deleteEntity, boolean createBatch,
			boolean upsertBatch, boolean updateBatch, boolean deleteBatch, boolean upsertTemporal,
			boolean appendAttrsTemporal, boolean deleteAttrsTemporal, boolean updateAttrsTemporal,
			boolean deleteAttrInstanceTemporal, boolean deleteTemporal, boolean mergeEntity, boolean replaceEntity,
			boolean replaceAttrs, boolean mergeBatch, boolean retrieveEntity, boolean queryEntity, boolean queryBatch,
			boolean retrieveTemporal, boolean queryTemporal, boolean retrieveEntityTypes,
			boolean retrieveEntityTypeDetails, boolean retrieveEntityTypeInfo, boolean retrieveAttrTypes,
			boolean retrieveAttrTypeDetails, boolean retrieveAttrTypeInfo, boolean createSubscription,
			boolean updateSubscription, boolean retrieveSubscription, boolean querySubscription,
			boolean deleteSubscription, boolean queryEntityMap, boolean createEntityMap, boolean updateEntityMap,
			boolean deleteEntityMap, boolean retrieveEntityMap, RemoteHost host, Context context) {
		super();
		this.cId = cId;
		this.eId = eId;
		this.eIdp = eIdp;
		this.type = type;
		this.eProp = eProp;
		this.eRel = eRel;
		this.location = location;
		this.scopes = scopes;
		this.expiresAt = expiresAt;
		this.regMode = regMode;
		this.createEntity = createEntity;
		this.updateEntity = updateEntity;
		this.appendAttrs = appendAttrs;
		this.updateAttrs = updateAttrs;
		this.deleteAttrs = deleteAttrs;
		this.deleteEntity = deleteEntity;
		this.createBatch = createBatch;
		this.upsertBatch = upsertBatch;
		this.updateBatch = updateBatch;
		this.deleteBatch = deleteBatch;
		this.upsertTemporal = upsertTemporal;
		this.appendAttrsTemporal = appendAttrsTemporal;
		this.deleteAttrsTemporal = deleteAttrsTemporal;
		this.updateAttrsTemporal = updateAttrsTemporal;
		this.deleteAttrInstanceTemporal = deleteAttrInstanceTemporal;
		this.deleteTemporal = deleteTemporal;
		this.mergeEntity = mergeEntity;
		this.replaceEntity = replaceEntity;
		this.replaceAttrs = replaceAttrs;
		this.mergeBatch = mergeBatch;
		this.retrieveEntity = retrieveEntity;
		this.queryEntity = queryEntity;
		this.queryBatch = queryBatch;
		this.retrieveTemporal = retrieveTemporal;
		this.queryTemporal = queryTemporal;
		this.retrieveEntityTypes = retrieveEntityTypes;
		this.retrieveEntityTypeDetails = retrieveEntityTypeDetails;
		this.retrieveEntityTypeInfo = retrieveEntityTypeInfo;
		this.retrieveAttrTypes = retrieveAttrTypes;
		this.retrieveAttrTypeDetails = retrieveAttrTypeDetails;
		this.retrieveAttrTypeInfo = retrieveAttrTypeInfo;
		this.createSubscription = createSubscription;
		this.updateSubscription = updateSubscription;
		this.retrieveSubscription = retrieveSubscription;
		this.querySubscription = querySubscription;
		this.deleteSubscription = deleteSubscription;
		this.queryEntityMap = queryEntityMap;
		this.createEntityMap = createEntityMap;
		this.updateEntityMap = updateEntityMap;
		this.deleteEntityMap = deleteEntityMap;
		this.retrieveEntityMap = retrieveEntityMap;
		this.host = host;
		this.context = context;
	}

	public static Uni<List<RegistrationEntry>> fromRegPayload(Map<String, Object> payload, JsonLDService ldService) {
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
		List<Map<String, Object>> csourceInfo = (List<Map<String, Object>>) payload
				.get("https://uri.etsi.org/ngsi-ld/contextSourceInfo");
		MultiMap headers = MultiMap.newInstance(HttpUtils.getHeadersForRemoteCallFromRegUpdate(csourceInfo, tenant));
		String atContextLink = headers.get(NGSIConstants.JSONLD_CONTEXT);
		Uni<Context> ctxUni;
		if (atContextLink == null) {
			ctxUni = Uni.createFrom().nullItem();
		} else {
			headers.remove(NGSIConstants.JSONLD_CONTEXT);
			ctxUni = ldService.parse(atContextLink);
		}
		return ctxUni.onItem().transform(ctx -> {
			Shape tmpLocation;
			if (payload.containsKey(NGSIConstants.NGSI_LD_LOCATION)) {
				try {
					tmpLocation = SubscriptionTools
							.getShape((Map<String, Object>) payload.get(NGSIConstants.NGSI_LD_LOCATION));
				} catch (ResponseException e) {
					tmpLocation = null;
				}
			} else {
				tmpLocation = null;
			}
			Shape location = tmpLocation;
			String[] scopes;
			if (payload.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
				scopes = getScopesFromPayload(payload.get(NGSIConstants.NGSI_LD_SCOPE));
			} else {
				scopes = null;
			}
			int mode;
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
					default:
						mode = 1;
						break;
				}
			} else {
				mode = 1;
			}
			long tmpEexpiresAt;
			if (payload.containsKey(NGSIConstants.NGSI_LD_EXPIRES)) {
				tmpEexpiresAt = SerializationTools
						.date2Long(((List<Map<String, String>>) payload.get(NGSIConstants.NGSI_LD_EXPIRES)).get(0)
								.get(NGSIConstants.JSON_LD_VALUE));
			} else {
				tmpEexpiresAt = -1l;
			}
			String sourceAlias;
			if (payload.containsKey(NGSIConstants.NGSI_LD_SOURCE_ALIAS)) {
				sourceAlias = ((List<Map<String, String>>) payload.get(NGSIConstants.NGSI_LD_SOURCE_ALIAS)).get(0)
						.get(NGSIConstants.JSON_LD_VALUE);
			} else {
				sourceAlias = null;
			}
			RemoteHost remoteHost = new RemoteHost(host, tenant, headers, cSourceId, canDoSingleOp, canDoBatchOp, 0,
					false, false, sourceAlias);

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

			boolean tmpDeleteSubscription = false;
			boolean tmpQuerySubscription = false;
			boolean tmpQueryEntityMap = false;
			boolean tmpCreateEntityMap = false;
			boolean tmpUpdateEntityMap = false;
			boolean tmpDeleteEntityMap = false;
			boolean tmpRetrieveEntityMap = false;
			if (payload.containsKey(NGSIConstants.NGSI_LD_REG_OPERATIONS)) {
				for (Map<String, String> opEntry : (List<Map<String, String>>) payload
						.get(NGSIConstants.NGSI_LD_REG_OPERATIONS)) {
					String operation = opEntry.get(NGSIConstants.JSON_LD_VALUE);
					switch (operation) {
						case NGSIConstants.NGSI_LD_REG_OPERATION_FEDERATION_OPS:
							tmpRetrieveEntity = tmpQueryEntity = tmpRetrieveEntityTypes = tmpRetrieveEntityTypeDetails = tmpRetrieveEntityTypeInfo = tmpRetrieveAttrTypes = tmpRetrieveAttrTypeDetails = tmpRetrieveAttrTypeInfo = tmpCreateSubscription = tmpUpdateSubscription = tmpRetrieveSubscription = tmpQuerySubscription = tmpDeleteSubscription = tmpQueryEntityMap = tmpCreateEntityMap = tmpUpdateEntityMap = tmpDeleteEntityMap = tmpRetrieveEntityMap = true;
							break;
						case NGSIConstants.NGSI_LD_REG_OPERATION_UPDATE_OPS:
							tmpUpdateEntity = tmpUpdateAttrs = tmpReplaceEntity = tmpReplaceAttrs = true;
							break;
						case NGSIConstants.NGSI_LD_REG_OPERATION_RETRIEVE_OPS:
							tmpRetrieveEntity = tmpQueryEntity = true;
							break;
						case NGSIConstants.NGSI_LD_REG_OPERATION_REDIRECTION_OPS:
							tmpCreateEntity = tmpUpdateEntity = tmpAppendAttrs = tmpUpdateAttrs = tmpDeleteAttrs = tmpDeleteEntity = tmpMergeEntity = tmpReplaceEntity = tmpReplaceAttrs = tmpRetrieveEntity = tmpQueryEntity = tmpRetrieveEntityTypes = tmpRetrieveEntityTypeDetails = tmpRetrieveEntityTypeInfo = tmpRetrieveAttrTypes = tmpRetrieveAttrTypeDetails = tmpRetrieveAttrTypeInfo = true;
							break;
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
						case NGSIConstants.NGSI_LD_REG_OPERATION_QUERY_ENTITYMAP:
							tmpQueryEntityMap = true;
							break;
						case NGSIConstants.NGSI_LD_REG_OPERATION_CREATE_ENTITYMAP:
							tmpCreateEntityMap = true;
							break;
						case NGSIConstants.NGSI_LD_REG_OPERATION_UPDATE_ENTITYMAP:
							tmpUpdateEntityMap = true;
							break;
						case NGSIConstants.NGSI_LD_REG_OPERATION_DELETE_ENTITYMAP:
							tmpDeleteEntityMap = true;
							break;
						case NGSIConstants.NGSI_LD_REG_OPERATION_RETRIEVE_ENTITYMAP:
							tmpRetrieveEntityMap = true;
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
				tmpQueryEntityMap = true;
				tmpCreateEntityMap = true;
				tmpUpdateEntityMap = true;
				tmpDeleteEntityMap = true;
				tmpRetrieveEntityMap = true;
			}

			for (Map<String, Object> infoEntry : (List<Map<String, Object>>) payload
					.get(NGSIConstants.NGSI_LD_INFORMATION)) {
				if (infoEntry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
					for (Map<String, Object> entitiesEntry : (List<Map<String, Object>>) infoEntry
							.get(NGSIConstants.NGSI_LD_ENTITIES)) {
						Object typesObj = entitiesEntry.get(NGSIConstants.JSON_LD_TYPE);
						List<String> types;
						if (typesObj != null) {
							types = (List<String>) typesObj;
						} else {
							types = Lists.newArrayList();
							types.add(null);
						}
						for (String entityType : types) {
							String tmpEId = null;
							String tmpEIdp = null;
							if (entitiesEntry.containsKey(NGSIConstants.JSON_LD_ID)) {
								tmpEId = (String) entitiesEntry.get(NGSIConstants.JSON_LD_ID);
							}
							if (entitiesEntry.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)) {
								tmpEIdp = ((List<Map<String, String>>) entitiesEntry.get(NGSIConstants.JSON_LD_ID))
										.get(0).get(NGSIConstants.JSON_LD_VALUE);
							}

							boolean containsProps = infoEntry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES);
							boolean containsRels = infoEntry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS);
							if (containsProps || containsRels) {
								if (containsProps) {
									for (Map<String, String> prop : (List<Map<String, String>>) infoEntry
											.get(NGSIConstants.NGSI_LD_PROPERTIES)) {
										result.add(new RegistrationEntry(cSourceId, tmpEId, tmpEIdp, entityType,
												prop.get(NGSIConstants.JSON_LD_ID), null, location, scopes,
												tmpEexpiresAt, mode, tmpCreateEntity, tmpUpdateEntity, tmpAppendAttrs,
												tmpUpdateAttrs, tmpDeleteAttrs, tmpDeleteEntity, tmpCreateBatch,
												tmpUpsertBatch, tmpUpdateBatch, tmpDeleteBatch, tmpUpsertTemporal,
												tmpAppendAttrsTemporal, tmpDeleteAttrsTemporal, tmpUpdateAttrsTemporal,
												tmpDeleteAttrInstanceTemporal, tmpDeleteTemporal, tmpMergeEntity,
												tmpReplaceEntity, tmpReplaceAttrs, tmpMergeBatch, tmpRetrieveEntity,
												tmpQueryEntity, tmpQueryBatch, tmpRetrieveTemporal, tmpQueryTemporal,
												tmpRetrieveEntityTypes, tmpRetrieveEntityTypeDetails,
												tmpRetrieveEntityTypeInfo, tmpRetrieveAttrTypes,
												tmpRetrieveAttrTypeDetails, tmpRetrieveAttrTypeInfo,
												tmpCreateSubscription, tmpUpdateSubscription, tmpRetrieveSubscription,
												tmpQuerySubscription, tmpDeleteSubscription, tmpQueryEntityMap,
												tmpCreateEntityMap, tmpUpdateEntityMap, tmpDeleteEntityMap,
												tmpRetrieveEntityMap, remoteHost, ctx));
									}
								}
								if (containsRels) {
									for (Map<String, String> rel : (List<Map<String, String>>) infoEntry
											.get(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
										result.add(new RegistrationEntry(cSourceId, tmpEId, tmpEIdp, entityType, null,
												rel.get(NGSIConstants.JSON_LD_ID), location, scopes, tmpEexpiresAt,
												mode, tmpCreateEntity, tmpUpdateEntity, tmpAppendAttrs, tmpUpdateAttrs,
												tmpDeleteAttrs, tmpDeleteEntity, tmpCreateBatch, tmpUpsertBatch,
												tmpUpdateBatch, tmpDeleteBatch, tmpUpsertTemporal,
												tmpAppendAttrsTemporal, tmpDeleteAttrsTemporal, tmpUpdateAttrsTemporal,
												tmpDeleteAttrInstanceTemporal, tmpDeleteTemporal, tmpMergeEntity,
												tmpReplaceEntity, tmpReplaceAttrs, tmpMergeBatch, tmpRetrieveEntity,
												tmpQueryEntity, tmpQueryBatch, tmpRetrieveTemporal, tmpQueryTemporal,
												tmpRetrieveEntityTypes, tmpRetrieveEntityTypeDetails,
												tmpRetrieveEntityTypeInfo, tmpRetrieveAttrTypes,
												tmpRetrieveAttrTypeDetails, tmpRetrieveAttrTypeInfo,
												tmpCreateSubscription, tmpUpdateSubscription, tmpRetrieveSubscription,
												tmpQuerySubscription, tmpDeleteSubscription, tmpQueryEntityMap,
												tmpCreateEntityMap, tmpUpdateEntityMap, tmpDeleteEntityMap,
												tmpRetrieveEntityMap, remoteHost, ctx));
									}
								}
							} else {
								result.add(new RegistrationEntry(cSourceId, tmpEId, tmpEIdp, entityType, null, null,
										location, scopes, tmpEexpiresAt, mode, tmpCreateEntity, tmpUpdateEntity,
										tmpAppendAttrs, tmpUpdateAttrs, tmpDeleteAttrs, tmpDeleteEntity, tmpCreateBatch,
										tmpUpsertBatch, tmpUpdateBatch, tmpDeleteBatch, tmpUpsertTemporal,
										tmpAppendAttrsTemporal, tmpDeleteAttrsTemporal, tmpUpdateAttrsTemporal,
										tmpDeleteAttrInstanceTemporal, tmpDeleteTemporal, tmpMergeEntity,
										tmpReplaceEntity, tmpReplaceAttrs, tmpMergeBatch, tmpRetrieveEntity,
										tmpQueryEntity, tmpQueryBatch, tmpRetrieveTemporal, tmpQueryTemporal,
										tmpRetrieveEntityTypes, tmpRetrieveEntityTypeDetails, tmpRetrieveEntityTypeInfo,
										tmpRetrieveAttrTypes, tmpRetrieveAttrTypeDetails, tmpRetrieveAttrTypeInfo,
										tmpCreateSubscription, tmpUpdateSubscription, tmpRetrieveSubscription,
										tmpQuerySubscription, tmpDeleteSubscription, tmpQueryEntityMap,
										tmpCreateEntityMap, tmpUpdateEntityMap, tmpDeleteEntityMap,
										tmpRetrieveEntityMap, remoteHost, ctx));
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
									tmpQuerySubscription, tmpDeleteSubscription, tmpQueryEntityMap, tmpCreateEntityMap,
									tmpUpdateEntityMap, tmpDeleteEntityMap, tmpRetrieveEntityMap, remoteHost, ctx));
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
									tmpQuerySubscription, tmpDeleteSubscription, tmpQueryEntityMap, tmpCreateEntityMap,
									tmpUpdateEntityMap, tmpDeleteEntityMap, tmpRetrieveEntityMap, remoteHost, ctx));
						}
					}
				}
			}

			// keep a reference to the full expanded registration so a context source
			// filter (csf) can be evaluated against the registration's descriptive
			// properties in memory. all entries of one registration share the payload.
			for (RegistrationEntry regEntry : result) {
				regEntry.registration = payload;
			}
			return result;
		});
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

	/**
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
		if (prop != null && (eRel != null || eProp != null && !prop.equals(eProp))) {
			return null;
		}
		if (rel != null && (eProp != null || eRel != null && !rel.equals(eRel))) {
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
		if (originalScopes == null) {
			if (this.scopes == null) {
				return null;
			}
			originalScopes = Lists.newArrayList();
		}
		for (Map<String, String> scopeEntry : originalScopes) {
			String scope = scopeEntry.get(NGSIConstants.JSON_LD_VALUE);
			if (this.scopes == null || ArrayUtils.contains(this.scopes, scope)) {
				result.add(scope);
			}
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
				result.setGeoOp(geoQuery.getGeorel());
				result.setGeoQuery(geoQuery);
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
									if (location instanceof JtsPoint
											|| ((JtsGeometry) location).getGeom() instanceof LineString
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
				result.setGeoOp(NGSIConstants.GEO_REL_WITHIN);
				result.setGeo(location);
			}
		}
		result.setqQuery(qQuery);
		return result;

	}

	public String cId() {
		return cId;
	}

	public void setcId(String cId) {
		this.cId = cId;
	}

	public String eId() {
		return eId;
	}

	public void seteId(String eId) {
		this.eId = eId;
	}

	public String eIdp() {
		return eIdp;
	}

	public void seteIdp(String eIdp) {
		this.eIdp = eIdp;
	}

	public String type() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String eProp() {
		return eProp;
	}

	public void seteProp(String eProp) {
		this.eProp = eProp;
	}

	public String eRel() {
		return eRel;
	}

	public void seteRel(String eRel) {
		this.eRel = eRel;
	}

	public Shape location() {
		return location;
	}

	public void setLocation(Shape location) {
		this.location = location;
	}

	public String[] scopes() {
		return scopes;
	}

	public void setScopes(String[] scopes) {
		this.scopes = scopes;
	}

	public long expiresAt() {
		return expiresAt;
	}

	public void setExpiresAt(long expiresAt) {
		this.expiresAt = expiresAt;
	}

	public int regMode() {
		return regMode;
	}

	public void setRegMode(int regMode) {
		this.regMode = regMode;
	}

	public boolean createEntity() {
		return createEntity;
	}

	public void setCreateEntity(boolean createEntity) {
		this.createEntity = createEntity;
	}

	public boolean updateEntity() {
		return updateEntity;
	}

	public void setUpdateEntity(boolean updateEntity) {
		this.updateEntity = updateEntity;
	}

	public boolean appendAttrs() {
		return appendAttrs;
	}

	public void setAppendAttrs(boolean appendAttrs) {
		this.appendAttrs = appendAttrs;
	}

	public boolean updateAttrs() {
		return updateAttrs;
	}

	public void setUpdateAttrs(boolean updateAttrs) {
		this.updateAttrs = updateAttrs;
	}

	public boolean deleteAttrs() {
		return deleteAttrs;
	}

	public void setDeleteAttrs(boolean deleteAttrs) {
		this.deleteAttrs = deleteAttrs;
	}

	public boolean deleteEntity() {
		return deleteEntity;
	}

	public void setDeleteEntity(boolean deleteEntity) {
		this.deleteEntity = deleteEntity;
	}

	public boolean createBatch() {
		return createBatch;
	}

	public void setCreateBatch(boolean createBatch) {
		this.createBatch = createBatch;
	}

	public boolean upsertBatch() {
		return upsertBatch;
	}

	public void setUpsertBatch(boolean upsertBatch) {
		this.upsertBatch = upsertBatch;
	}

	public boolean updateBatch() {
		return updateBatch;
	}

	public void setUpdateBatch(boolean updateBatch) {
		this.updateBatch = updateBatch;
	}

	public boolean deleteBatch() {
		return deleteBatch;
	}

	public void setDeleteBatch(boolean deleteBatch) {
		this.deleteBatch = deleteBatch;
	}

	public boolean upsertTemporal() {
		return upsertTemporal;
	}

	public void setUpsertTemporal(boolean upsertTemporal) {
		this.upsertTemporal = upsertTemporal;
	}

	public boolean appendAttrsTemporal() {
		return appendAttrsTemporal;
	}

	public void setAppendAttrsTemporal(boolean appendAttrsTemporal) {
		this.appendAttrsTemporal = appendAttrsTemporal;
	}

	public boolean deleteAttrsTemporal() {
		return deleteAttrsTemporal;
	}

	public void setDeleteAttrsTemporal(boolean deleteAttrsTemporal) {
		this.deleteAttrsTemporal = deleteAttrsTemporal;
	}

	public boolean updateAttrsTemporal() {
		return updateAttrsTemporal;
	}

	public void setUpdateAttrsTemporal(boolean updateAttrsTemporal) {
		this.updateAttrsTemporal = updateAttrsTemporal;
	}

	public boolean deleteAttrInstanceTemporal() {
		return deleteAttrInstanceTemporal;
	}

	public void setDeleteAttrInstanceTemporal(boolean deleteAttrInstanceTemporal) {
		this.deleteAttrInstanceTemporal = deleteAttrInstanceTemporal;
	}

	public boolean deleteTemporal() {
		return deleteTemporal;
	}

	public void setDeleteTemporal(boolean deleteTemporal) {
		this.deleteTemporal = deleteTemporal;
	}

	public boolean mergeEntity() {
		return mergeEntity;
	}

	public void setMergeEntity(boolean mergeEntity) {
		this.mergeEntity = mergeEntity;
	}

	public boolean replaceEntity() {
		return replaceEntity;
	}

	public void setReplaceEntity(boolean replaceEntity) {
		this.replaceEntity = replaceEntity;
	}

	public boolean replaceAttrs() {
		return replaceAttrs;
	}

	public void setReplaceAttrs(boolean replaceAttrs) {
		this.replaceAttrs = replaceAttrs;
	}

	public boolean mergeBatch() {
		return mergeBatch;
	}

	public void setMergeBatch(boolean mergeBatch) {
		this.mergeBatch = mergeBatch;
	}

	public boolean retrieveEntity() {
		return retrieveEntity;
	}

	public void setRetrieveEntity(boolean retrieveEntity) {
		this.retrieveEntity = retrieveEntity;
	}

	public boolean queryEntity() {
		return queryEntity;
	}

	public void setQueryEntity(boolean queryEntity) {
		this.queryEntity = queryEntity;
	}

	public boolean queryBatch() {
		return queryBatch;
	}

	public void setQueryBatch(boolean queryBatch) {
		this.queryBatch = queryBatch;
	}

	public boolean retrieveTemporal() {
		return retrieveTemporal;
	}

	public void setRetrieveTemporal(boolean retrieveTemporal) {
		this.retrieveTemporal = retrieveTemporal;
	}

	public boolean queryTemporal() {
		return queryTemporal;
	}

	public void setQueryTemporal(boolean queryTemporal) {
		this.queryTemporal = queryTemporal;
	}

	public boolean retrieveEntityTypes() {
		return retrieveEntityTypes;
	}

	public void setRetrieveEntityTypes(boolean retrieveEntityTypes) {
		this.retrieveEntityTypes = retrieveEntityTypes;
	}

	public boolean retrieveEntityTypeDetails() {
		return retrieveEntityTypeDetails;
	}

	public void setRetrieveEntityTypeDetails(boolean retrieveEntityTypeDetails) {
		this.retrieveEntityTypeDetails = retrieveEntityTypeDetails;
	}

	public boolean retrieveEntityTypeInfo() {
		return retrieveEntityTypeInfo;
	}

	public void setRetrieveEntityTypeInfo(boolean retrieveEntityTypeInfo) {
		this.retrieveEntityTypeInfo = retrieveEntityTypeInfo;
	}

	public boolean retrieveAttrTypes() {
		return retrieveAttrTypes;
	}

	public void setRetrieveAttrTypes(boolean retrieveAttrTypes) {
		this.retrieveAttrTypes = retrieveAttrTypes;
	}

	public boolean retrieveAttrTypeDetails() {
		return retrieveAttrTypeDetails;
	}

	public void setRetrieveAttrTypeDetails(boolean retrieveAttrTypeDetails) {
		this.retrieveAttrTypeDetails = retrieveAttrTypeDetails;
	}

	public boolean retrieveAttrTypeInfo() {
		return retrieveAttrTypeInfo;
	}

	public void setRetrieveAttrTypeInfo(boolean retrieveAttrTypeInfo) {
		this.retrieveAttrTypeInfo = retrieveAttrTypeInfo;
	}

	public boolean createSubscription() {
		return createSubscription;
	}

	public void setCreateSubscription(boolean createSubscription) {
		this.createSubscription = createSubscription;
	}

	public boolean updateSubscription() {
		return updateSubscription;
	}

	public void setUpdateSubscription(boolean updateSubscription) {
		this.updateSubscription = updateSubscription;
	}

	public boolean retrieveSubscription() {
		return retrieveSubscription;
	}

	public void setRetrieveSubscription(boolean retrieveSubscription) {
		this.retrieveSubscription = retrieveSubscription;
	}

	public boolean querySubscription() {
		return querySubscription;
	}

	public void setQuerySubscription(boolean querySubscription) {
		this.querySubscription = querySubscription;
	}

	public boolean deleteSubscription() {
		return deleteSubscription;
	}

	public void setDeleteSubscription(boolean deleteSubscription) {
		this.deleteSubscription = deleteSubscription;
	}

	public boolean queryEntityMap() {
		return queryEntityMap;
	}

	public void setQueryEntityMap(boolean queryEntityMap) {
		this.queryEntityMap = queryEntityMap;
	}

	public boolean createEntityMap() {
		return createEntityMap;
	}

	public void setCreateEntityMap(boolean createEntityMap) {
		this.createEntityMap = createEntityMap;
	}

	public boolean updateEntityMap() {
		return updateEntityMap;
	}

	public void setUpdateEntityMap(boolean updateEntityMap) {
		this.updateEntityMap = updateEntityMap;
	}

	public boolean deleteEntityMap() {
		return deleteEntityMap;
	}

	public void setDeleteEntityMap(boolean deleteEntityMap) {
		this.deleteEntityMap = deleteEntityMap;
	}

	public boolean retrieveEntityMap() {
		return retrieveEntityMap;
	}

	public void setRetrieveEntityMap(boolean retrieveEntityMap) {
		this.retrieveEntityMap = retrieveEntityMap;
	}

	public RemoteHost host() {
		return host;
	}

	public void setHost(RemoteHost host) {
		this.host = host;
	}

	public Context context() {
		return context;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public Map<String, Object> registration() {
		return registration;
	}

}
