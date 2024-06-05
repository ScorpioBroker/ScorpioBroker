package eu.neclab.ngsildbroker.queryhandler.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityCache;
import eu.neclab.ngsildbroker.commons.datatypes.EntityMap;
import eu.neclab.ngsildbroker.commons.datatypes.EntityMapEntry;
import eu.neclab.ngsildbroker.commons.datatypes.QueryInfos;
import eu.neclab.ngsildbroker.commons.datatypes.QueryRemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.QueryServiceInterface;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class QueryService implements QueryServiceInterface {

	private static Logger logger = LoggerFactory.getLogger(QueryService.class);

	@Inject
	QueryDAO queryDAO;

	@Inject
	Vertx vertx;

	@Inject
	JsonLDService ldService;

	private WebClient webClient;

	protected JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	private Table<String, String, List<RegistrationEntry>> tenant2CId2RegEntries = HashBasedTable.create();

	@ConfigProperty(name = "scorpio.entitymap.cleanup.ttl", defaultValue = "30 sec")
	String entityMapTTL;

	@ConfigProperty(name = "scorpio.fed.timeout", defaultValue = "20000")
	long timeout;

	@PostConstruct
	void setup() {
		webClient = WebClient.create(vertx);
		queryDAO.getAllRegistries().onItem().transform(t -> {
			tenant2CId2RegEntries = t;
			return null;
		}).await().indefinitely();
	}

	// This is needed so that @postconstruct runs on the startup thread and not on a
	// worker thread later on
	void startup(@Observes StartupEvent event) {
	}

	public Uni<QueryResult> query(String tenant, String qToken, boolean tokenProvided, String[] ids,
			TypeQueryTerm typeQuery, String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf,
			GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, int limit, int offSet,
			boolean count, boolean localOnly, Context context, io.vertx.core.MultiMap headersFromReq,
			boolean doNotCompact, Set<String> jsonKeys, DataSetIdTerm dataSetIdTerm, String join, int joinLevel,
			boolean entityDist, PickTerm pickTerm, OmitTerm omitTerm) {
		if (localOnly) {
			return localQueryLevel1(tenant, ids, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery,
					langQuery, limit, offSet, count, dataSetIdTerm, join, joinLevel, pickTerm, omitTerm);
		}
		if (!tokenProvided) {
			return getAndStoreEntityIdList(tenant, ids, idPattern, qToken, typeQuery, attrsQuery, geoQuery, qQuery,
					scopeQuery, langQuery, limit, offSet, context, headersFromReq, doNotCompact, dataSetIdTerm, join,
					joinLevel, entityDist, pickTerm, omitTerm).onItem().transformToUni(t -> {
						return handleEntityMap(t.getItem2(), t.getItem1(), tenant, ids, typeQuery, idPattern,
								attrsQuery, qQuery, geoQuery, scopeQuery, langQuery, limit, offSet, count,
								dataSetIdTerm, join, joinLevel, context, entityDist, jsonKeys, headersFromReq, pickTerm,
								omitTerm);

					});
		} else {
			return getEntityMapAndEntitiesAndUpdateExpires(tenant, ids, typeQuery, idPattern, attrsQuery, qQuery,
					geoQuery, scopeQuery, context, limit, offSet, dataSetIdTerm, join, joinLevel, qToken, pickTerm,
					omitTerm).onItem().transformToUni(t -> {
						return handleEntityMap(t.getItem2(), t.getItem1(), tenant, ids, typeQuery, idPattern,
								attrsQuery, qQuery, geoQuery, scopeQuery, langQuery, limit, offSet, count,
								dataSetIdTerm, join, joinLevel, context, entityDist, jsonKeys, headersFromReq, pickTerm,
								omitTerm);

					});
		}
	}

	private Uni<Tuple2<EntityCache, EntityMap>> getEntityMapAndEntitiesAndUpdateExpires(String tenant, String[] ids,
			TypeQueryTerm typeQuery, String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery,
			GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, Context context, int limit, int offset,
			DataSetIdTerm dataSetIdTerm, String join, int joinLevel, String qToken, PickTerm pickTerm,
			OmitTerm omitTerm) {
		Uni<Tuple2<EntityCache, EntityMap>> entityCacheAndEntityMap;
		if (tenant2CId2RegEntries.isEmpty()) {
			entityCacheAndEntityMap = queryDAO.queryForEntitiesAndEntityMapNoRegEntry(tenant, attrsQuery, limit, offset,
					dataSetIdTerm, join, joinLevel, qToken, pickTerm, omitTerm);
		} else {
			entityCacheAndEntityMap = queryDAO.queryForEntitiesAndEntityMap(tenant, attrsQuery, limit, offset,
					dataSetIdTerm, join, joinLevel, qToken, pickTerm, omitTerm);
		}
		return entityCacheAndEntityMap;
	}

	private Uni<QueryResult> handleEntityMap(EntityMap entityMap, EntityCache entityCache, String tenant, String[] id,
			TypeQueryTerm typeQuery, String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery,
			GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, int limit, int offSet,
			boolean count, DataSetIdTerm dataSetIdTerm, String join, int joinLevel, Context context,
			boolean onlyFullEntities, Set<String> jsonKeys, io.vertx.core.MultiMap headersFromReq, PickTerm pickTerm,
			OmitTerm omitTerm) {
		QueryResult result = new QueryResult();

		List<Map<String, Object>> resultData = Lists.newArrayList();
		result.setData(resultData);
		result.setCount(entityMap.size());
		result.setqToken(entityMap.getId());
		result.setLimit(limit);
		result.setOffset(offSet);

		long leftAfter = entityMap.size() - (offSet + limit);
		if (leftAfter < 0) {
			leftAfter = 0;
		}
		result.setResultsLeftAfter(leftAfter);
		result.setResultsLeftBefore((long) offSet);
		List<EntityMapEntry> subMap = entityMap.getSubMap(offSet, offSet + limit);
		Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2EntityAndHost = entityCache
				.getAllIds2EntityAndHosts();
		// no registry entries just push out the result
		if (entityMap.isRegEmpty()
				|| (entityMap.isNoRootLevelRegEntry() && ((join == null || joinLevel <= 0) && !qQuery.hasLinkedQ()))) {
			boolean doInline = NGSIConstants.INLINE.equals(join);
			for (EntityMapEntry entityMapEntry : subMap) {
				if (!doInline) {
					resultData.add(ids2EntityAndHost.remove(entityMapEntry.getEntityId()).getItem1());
				} else {
					resultData.add(ids2EntityAndHost.get(entityMapEntry.getEntityId()).getItem1());
				}
			}
			if (join != null && joinLevel > 0) {
				if (join.equals(NGSIConstants.FLAT)) {
					for (Entry<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> entityEntry : ids2EntityAndHost
							.entrySet()) {
						resultData.add(entityEntry.getValue().getItem1());
					}
				} else if (join.equals(NGSIConstants.INLINE)) {
					resultData.forEach(entity -> {
						inlineEntity(entity, entityCache, 1, joinLevel, false);
					});

				}
			}
			// run pick and omit again in case of linked projection ... this should be also
			// in the DB once my head is ready for sql again
			EntityTools.evaluateFilterQueries(resultData, null, null, null, null, pickTerm, omitTerm, null, null, null);
			return Uni.createFrom().item(result);
		} else if (entityMap.isNoRootLevelRegEntry()) {
			for (EntityMapEntry entityMapEntry : subMap) {
				resultData.add(ids2EntityAndHost.get(entityMapEntry.getEntityId()).getItem1());
			}
			if (qQuery.hasLinkedQ()) {
				return retrieveJoins(tenant, resultData, entityCache, context, qQuery, onlyFullEntities, 0, -1,
						qQuery.getMaxJoinLevel()).onItem().transformToUni(updatedEntityCache -> {
							Map<String, Map<String, Object>> deleted = EntityTools.evaluateFilterQueries(resultData,
									qQuery, null, null, attrsQuery, pickTerm, omitTerm, dataSetIdTerm,
									updatedEntityCache, jsonKeys);
							if (deleted.isEmpty()) {
								if (entityMap.isChanged()) {
									return queryDAO.storeEntityMap(tenant, entityMap.getId(), entityMap, true).onItem()
											.transformToUni(v -> {
												return doJoinIfNeeded(tenant, result, updatedEntityCache, context, join,
														joinLevel, onlyFullEntities);
											});
								}
								return doJoinIfNeeded(tenant, result, updatedEntityCache, context, join, joinLevel,
										onlyFullEntities);
							} else {
								return updateEntityMapAndRepull(deleted, entityMap, updatedEntityCache, tenant, id,
										typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, langQuery,
										limit, offSet, count, dataSetIdTerm, join, joinLevel, context, onlyFullEntities,
										jsonKeys, headersFromReq, pickTerm, omitTerm);
							}
						});
			} else {
				if (attrsQuery != null) {
					attrsQuery.calculateQuery(resultData);
				}
				return doJoinIfNeeded(tenant, result, entityCache, context, join, joinLevel, onlyFullEntities);
			}
		} else {
			return fillCacheFromEntityMap(subMap, entityCache, context, headersFromReq, false, tenant).onItem()
					.transformToUni(updatedEntityCache -> {
						if ((join == null || joinLevel < 0) && qQuery.hasLinkedQ()) {
							for (EntityMapEntry entityMapEntry : subMap) {
								resultData.add(ids2EntityAndHost.get(entityMapEntry.getEntityId()).getItem1());
							}
							if (onlyFullEntities) {
								return Uni.createFrom().item(result);
							} else {
								Map<String, Map<String, Object>> deleted = EntityTools.evaluateFilterQueries(resultData,
										qQuery, scopeQuery, geoQuery, attrsQuery, pickTerm, omitTerm, dataSetIdTerm,
										updatedEntityCache, jsonKeys);
								if (deleted.isEmpty()) {
									if (entityMap.isChanged()) {
										return queryDAO.storeEntityMap(tenant, entityMap.getId(), entityMap, true)
												.onItem().transformToUni(v -> {
													return doJoinIfNeeded(tenant, result, updatedEntityCache, context,
															join, joinLevel, onlyFullEntities);
												});
									}
									return doJoinIfNeeded(tenant, result, updatedEntityCache, context, join, joinLevel,
											onlyFullEntities);
								} else {
									return updateEntityMapAndRepull(deleted, entityMap, updatedEntityCache, tenant, id,
											typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, langQuery,
											limit, offSet, count, dataSetIdTerm, join, joinLevel, context,
											onlyFullEntities, jsonKeys, headersFromReq, pickTerm, omitTerm);
								}
							}

						} else {
							if (onlyFullEntities) {
								if (qQuery.hasLinkedQ()) {
									return retrieveJoins(tenant, resultData, entityCache, context, qQuery,
											onlyFullEntities, 0, -1, qQuery.getMaxJoinLevel()).onItem()
											.transformToUni(updatedEntityCache2 -> {
												Map<String, Map<String, Object>> deleted = EntityTools
														.evaluateFilterQueries(resultData, qQuery, null, null,
																attrsQuery, pickTerm, omitTerm, null, entityCache,
																jsonKeys);
												if (deleted.isEmpty()) {
													if (entityMap.isChanged()) {
														return queryDAO.storeEntityMap(tenant, entityMap.getId(),
																entityMap, true).onItem().transformToUni(v -> {
																	return doJoinIfNeeded(tenant, result,
																			updatedEntityCache, context, join,
																			joinLevel, onlyFullEntities);
																});
													}
													return doJoinIfNeeded(tenant, result, updatedEntityCache2, context,
															join, joinLevel, onlyFullEntities);
												} else {
													return updateEntityMapAndRepull(deleted, entityMap,
															updatedEntityCache2, tenant, id, typeQuery, idPattern,
															attrsQuery, qQuery, geoQuery, scopeQuery, langQuery, limit,
															offSet, count, dataSetIdTerm, join, joinLevel, context,
															onlyFullEntities, jsonKeys, headersFromReq, pickTerm,
															omitTerm);
												}
											});
								} else {
									if (attrsQuery != null) {
										attrsQuery.calculateQuery(resultData);
									}
									if (entityMap.isChanged()) {
										return queryDAO.storeEntityMap(tenant, entityMap.getId(), entityMap, true)
												.onItem().transformToUni(v -> {
													return doJoinIfNeeded(tenant, result, updatedEntityCache, context,
															join, joinLevel, onlyFullEntities);
												});
									}
									return doJoinIfNeeded(tenant, result, entityCache, context, join, joinLevel,
											onlyFullEntities);
								}
							} else {
								Map<String, Map<String, Object>> deleted = EntityTools.evaluateFilterQueries(resultData,
										qQuery, scopeQuery, geoQuery, attrsQuery, pickTerm, omitTerm, dataSetIdTerm,
										entityCache, jsonKeys);
								if (deleted.isEmpty()) {
									if (entityMap.isChanged()) {
										return queryDAO.storeEntityMap(tenant, entityMap.getId(), entityMap, true)
												.onItem().transformToUni(v -> {
													return doJoinIfNeeded(tenant, result, updatedEntityCache, context,
															join, joinLevel, onlyFullEntities);
												});
									}
									return doJoinIfNeeded(tenant, result, updatedEntityCache, context, join, joinLevel,
											onlyFullEntities);
								} else {
									return updateEntityMapAndRepull(deleted, entityMap, updatedEntityCache, tenant, id,
											typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, langQuery,
											limit, offSet, count, dataSetIdTerm, join, joinLevel, context,
											onlyFullEntities, jsonKeys, headersFromReq, pickTerm, omitTerm);
								}
							}
						}

					});

		}

	}

	private Uni<EntityCache> fillCacheFromEntityMap(List<EntityMapEntry> subMap, EntityCache entityCache,
			Context context, io.vertx.core.MultiMap headersFromReq, boolean callDB, String tenant) {
		Map<QueryRemoteHost, Set<String>> remoteHost2Ids = Maps.newHashMap();
		Set<String> idsForDBCall = Sets.newHashSet();
		for (EntityMapEntry mapEntry : subMap) {
			List<QueryRemoteHost> hosts = mapEntry.getRemoteHosts();
			String entityId = mapEntry.getEntityId();
			for (QueryRemoteHost host : hosts) {
				if (host.host().equals(AppConstants.INTERNAL_NULL_KEY)) {
					if (callDB) {
						Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> cacheEntry = entityCache
								.getAllIds2EntityAndHosts().get(entityId);
						if (cacheEntry == null || !cacheEntry.getItem2().containsKey(AppConstants.INTERNAL_NULL_KEY)) {
							idsForDBCall.add(entityId);
						}
					} else {
						continue;
					}

				}
				Set<String> ids = remoteHost2Ids.get(host);
				if (ids == null) {
					ids = Sets.newHashSet();
					remoteHost2Ids.put(host, ids);
				}
				ids.add(entityId);
			}
		}
		List<Uni<Tuple2<Object, QueryRemoteHost>>> unis = Lists.newArrayList();
		for (Entry<QueryRemoteHost, Set<String>> entry : remoteHost2Ids.entrySet()) {
			QueryRemoteHost host = entry.getKey();
			host.updatedIds(entry.getValue());
			unis.add(getFullQueryResult(host, headersFromReq, context));
		}
		if (!idsForDBCall.isEmpty()) {
			unis.add(0, queryDAO.queryForEntities(tenant, idsForDBCall));
		}
		return Uni.combine().all().unis(unis).combinedWith(l -> {
			for (Object o : l) {
				@SuppressWarnings("unchecked")
				Tuple2<Object, QueryRemoteHost> t = (Tuple2<Object, QueryRemoteHost>) o;
				entityCache.putEntitiesIntoEntityCacheAndEntityMap((List<?>) t.getItem1(), t.getItem2(), null);
			}
			return entityCache;
		});
	}

	private Uni<QueryResult> doJoinIfNeeded(String tenant, QueryResult result, EntityCache entityCache, Context context,
			String join, int joinLevel, boolean onlyFullEntities) {
		if (join != null && joinLevel > 0) {
			List<Map<String, Object>> resultData = result.getData();
			return retrieveJoins(tenant, resultData, entityCache, context, null, onlyFullEntities, 0, joinLevel,
					joinLevel).onItem().transformToUni(updatedCache2 -> {
						if (NGSIConstants.FLAT.equals(join)) {
							Map<String, Map<String, Object>> toAdd = Maps.newHashMap();
							for (Map<String, Object> entity : resultData) {
								flatAddEntity(entity, entityCache, 0, joinLevel, toAdd, false);
							}
							resultData.addAll(toAdd.values());
						} else if (NGSIConstants.INLINE.equals(join)) {
							for (Map<String, Object> entity : resultData) {
								inlineEntity(entity, entityCache, 0, joinLevel, false);
							}
						}
						return Uni.createFrom().item(result);
					});
		}
		return Uni.createFrom().item(result);
	}

	private void flatAddEntity(Map<String, Object> entity, EntityCache entityCache, int currentJoinLevel, int joinLevel,
			Map<String, Map<String, Object>> toAdd, boolean localOnly) {
		for (String attribName : entity.keySet()) {
			if (NGSIConstants.ENTITY_BASE_PROPS.contains(attribName)) {
				continue;
			}
			Object attribObj = entity.get(attribName);
			if (attribObj instanceof List<?> list) {
				for (Object entry : list) {
					flatAddAttrib(entry, entityCache, currentJoinLevel, joinLevel, toAdd, localOnly);
				}
			} else {
				flatAddAttrib(attribObj, entityCache, currentJoinLevel, joinLevel, toAdd, localOnly);
			}

		}

	}

	@SuppressWarnings("unchecked")
	private void flatAddAttrib(Object attribObj, EntityCache entityCache, int currentJoinLevel, int joinLevel,
			Map<String, Map<String, Object>> toAdd, boolean localOnly) {
		if (attribObj instanceof Map<?, ?> attribMap) {
			Object typeObj = attribMap.get(NGSIConstants.JSON_LD_TYPE);
			if (typeObj != null && typeObj instanceof List<?> typeList) {
				if (typeList.contains(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
					List<Map<String, String>> hasObject = (List<Map<String, String>>) attribMap
							.get(NGSIConstants.NGSI_LD_HAS_OBJECT);

					if (localOnly) {
						Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2EntityAndHost = entityCache
								.getAllIds2EntityAndHosts();
						for (Map<String, String> idEntry : hasObject) {
							String entityId = idEntry.get(NGSIConstants.JSON_LD_ID);
							Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> entityAndHosts = ids2EntityAndHost
									.get(entityId);
							if (entityAndHosts != null) {
								Map<String, Object> ogEntity = entityAndHosts.getItem1();
								if (ogEntity != null) {
									toAdd.put((String) ogEntity.get(NGSIConstants.JSON_LD_ID), ogEntity);
									if (currentJoinLevel + 1 <= joinLevel) {
										flatAddEntity(ogEntity, entityCache, currentJoinLevel + 1, joinLevel, toAdd,
												localOnly);
									}
								}
							}
						}
					} else if (attribMap.containsKey(NGSIConstants.OBJECT_TYPE)) {
						List<Object> linkedTypes = (List<Object>) attribMap.get(NGSIConstants.OBJECT_TYPE);
						for (Object linkedTypeObj : linkedTypes) {
							if (linkedTypeObj instanceof Map<?, ?> linkedTypeMap
									&& linkedTypeMap.containsKey(NGSIConstants.JSON_LD_ID)) {
								String linkedType = (String) linkedTypeMap.get(NGSIConstants.JSON_LD_ID);
								Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2EntityAndHost = entityCache
										.getByType(linkedType);
								if (ids2EntityAndHost != null) {
									for (Map<String, String> idEntry : hasObject) {
										String entityId = idEntry.get(NGSIConstants.JSON_LD_ID);
										Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> entityAndHosts = ids2EntityAndHost
												.get(entityId);
										if (entityAndHosts != null) {
											Map<String, Object> ogEntity = entityAndHosts.getItem1();
											if (ogEntity != null
													&& ((List<String>) ogEntity.get(NGSIConstants.JSON_LD_TYPE))
															.contains(linkedType)) {
												toAdd.put((String) ogEntity.get(NGSIConstants.JSON_LD_ID), ogEntity);
												if (currentJoinLevel + 1 <= joinLevel) {
													flatAddEntity(ogEntity, entityCache, currentJoinLevel + 1,
															joinLevel, toAdd, localOnly);
												}
											}
										}
									}
								}
							}
						}
					}

				} else if (typeList.contains(NGSIConstants.NGSI_LD_LISTRELATIONSHIP)
						&& attribMap.containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST)) {

					List<Map<String, List<Map<String, List<Map<String, String>>>>>> hasObjectList = (List<Map<String, List<Map<String, List<Map<String, String>>>>>>) attribMap
							.get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);

					if (localOnly) {
						Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2EntityAndHost = entityCache
								.getAllIds2EntityAndHosts();
						for (Map<String, List<Map<String, List<Map<String, String>>>>> atListEntry : hasObjectList) {
							List<Map<String, List<Map<String, String>>>> atList = atListEntry
									.get(NGSIConstants.JSON_LD_LIST);
							for (Map<String, List<Map<String, String>>> hasObjectEntry : atList) {
								List<Map<String, String>> objectList = hasObjectEntry
										.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
								for (Map<String, String> objectEntry : objectList) {
									String entityId = objectEntry.get(NGSIConstants.JSON_LD_ID);
									Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> entityAndHosts = ids2EntityAndHost
											.get(entityId);
									if (entityAndHosts != null) {
										Map<String, Object> ogEntity = entityAndHosts.getItem1();
										if (ogEntity != null) {
											toAdd.put((String) ogEntity.get(NGSIConstants.JSON_LD_ID), ogEntity);
											if (currentJoinLevel + 1 <= joinLevel) {
												flatAddEntity(ogEntity, entityCache, currentJoinLevel + 1, joinLevel,
														toAdd, localOnly);
											}
										}
									}
								}

							}

						}
					} else if (attribMap.containsKey(NGSIConstants.OBJECT_TYPE)) {
						List<Object> linkedTypes = (List<Object>) attribMap.get(NGSIConstants.OBJECT_TYPE);
						for (Object linkedTypeObj : linkedTypes) {
							if (linkedTypeObj instanceof Map<?, ?> linkedTypeMap
									&& linkedTypeMap.containsKey(NGSIConstants.JSON_LD_ID)) {
								String linkedType = (String) linkedTypeMap.get(NGSIConstants.JSON_LD_ID);
								Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2EntityAndHost = entityCache
										.getByType(linkedType);
								if (ids2EntityAndHost != null) {
									for (Map<String, List<Map<String, List<Map<String, String>>>>> atListEntry : hasObjectList) {
										List<Map<String, List<Map<String, String>>>> atList = atListEntry
												.get(NGSIConstants.JSON_LD_LIST);
										for (Map<String, List<Map<String, String>>> hasObjectEntry : atList) {
											List<Map<String, String>> objectList = hasObjectEntry
													.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
											for (Map<String, String> objectEntry : objectList) {
												String entityId = objectEntry.get(NGSIConstants.JSON_LD_ID);
												Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> entityAndHosts = ids2EntityAndHost
														.get(entityId);
												if (entityAndHosts != null) {
													Map<String, Object> ogEntity = entityAndHosts.getItem1();
													if (ogEntity != null
															&& ((List<String>) ogEntity.get(NGSIConstants.JSON_LD_TYPE))
																	.contains(linkedType)) {
														toAdd.put((String) ogEntity.get(NGSIConstants.JSON_LD_ID),
																ogEntity);
														if (currentJoinLevel + 1 <= joinLevel) {
															flatAddEntity(ogEntity, entityCache, currentJoinLevel + 1,
																	joinLevel, toAdd, localOnly);
														}
													}
												}
											}

										}

									}
								}
							}
						}
					}
				}
			}
		}

	}

	private Uni<QueryResult> updateEntityMapAndRepull(Map<String, Map<String, Object>> deleted, EntityMap entityMap,
			EntityCache entityCache, String tenant, String[] id, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery,
			LanguageQueryTerm langQuery, int limit, int offSet, boolean count, DataSetIdTerm dataSetIdTerm, String join,
			int joinLevel, Context context, boolean onlyFullEntities, Set<String> jsonKeys,
			io.vertx.core.MultiMap headersFromReq, PickTerm pickTerm, OmitTerm omitTerm) {
		entityMap.removeEntries(deleted.keySet());
		List<EntityMapEntry> subMap = entityMap.getSubMap(offSet, limit + (deleted.size() * 3));
		entityMap.setChanged(true);
		return fillCacheFromEntityMap(subMap, entityCache, context, headersFromReq, true, tenant).onItem()
				.transformToUni(updatedCache -> {
					return handleEntityMap(entityMap, entityCache, tenant, id, typeQuery, idPattern, attrsQuery, qQuery,
							geoQuery, scopeQuery, langQuery, limit, offSet, count, dataSetIdTerm, join, joinLevel,
							context, onlyFullEntities, jsonKeys, headersFromReq, pickTerm, omitTerm);
				});

	}

	private Uni<EntityCache> retrieveJoins(String tenant, List<Map<String, Object>> currentLevel,
			EntityCache entityCache, Context context, QQueryTerm qQuery, boolean onlyFullEntities, int currentJoinLevel,
			int joinLevel, int maxJoinLevel) {
		Map<Set<String>, Set<String>> types2EntityIds;
		if (currentJoinLevel < joinLevel) {
			types2EntityIds = getAllTypesAndIds(currentLevel);
		} else {
			Map<String, Set<String>> attribAndTypes = qQuery.getJoinLevel2AttribAndTypes().get(currentJoinLevel);
			if (attribAndTypes == null) {
				return Uni.createFrom().item(entityCache);
			}
			types2EntityIds = getAllTypesAndIds(currentLevel, attribAndTypes);
		}
		return getEntitiesFromUncalledHosts(tenant, types2EntityIds, entityCache, context, qQuery, onlyFullEntities)
				.onItem().transformToUni(t -> {
					int nextLevel = currentJoinLevel + 1;
					if (nextLevel < maxJoinLevel) {
						return retrieveJoins(tenant, t.getItem2(), entityCache, context, qQuery, onlyFullEntities,
								nextLevel, joinLevel, maxJoinLevel);
					}
					return Uni.createFrom().item(t.getItem1());
				});

	}

	private Map<Set<String>, Set<String>> getAllTypesAndIds(List<Map<String, Object>> currentLevel,
			Map<String, Set<String>> attribAndTypes) {
		Map<Set<String>, Set<String>> result = Maps.newHashMap();
		for (Map<String, Object> entity : currentLevel) {
			for (Entry<String, Set<String>> attrib2Types : attribAndTypes.entrySet()) {
				Object attrValueListObj = entity.get(attrib2Types.getKey());
				addTypes2IdsFromAttr(attrValueListObj, result, attrib2Types.getValue());
			}
		}
		return result;
	}

	private Map<Set<String>, Set<String>> getAllTypesAndIds(List<Map<String, Object>> currentLevel) {
		Map<Set<String>, Set<String>> result = Maps.newHashMap();
		for (Map<String, Object> entity : currentLevel) {
			for (Entry<String, Object> attrEntry : entity.entrySet()) {
				Object attrValueListObj = attrEntry.getValue();
				addTypes2IdsFromAttr(attrValueListObj, result, null);
			}
		}
		return result;

	}

	private void addTypes2IdsFromAttr(Object attrValueListObj, Map<Set<String>, Set<String>> result,
			Set<String> typeFilter) {
		if (attrValueListObj != null && attrValueListObj instanceof List<?> l) {
			for (Object attrValueObj : l) {
				if (attrValueObj instanceof Map<?, ?> map) {
					List<String> attrType = (List<String>) map.get(NGSIConstants.JSON_LD_TYPE);
					if (attrType.contains(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
						Object objectTypeObj = map.get(NGSIConstants.NGSI_LD_OBJECT_TYPE);
						if (objectTypeObj != null && objectTypeObj instanceof List<?> objectTypeList) {
							Set<String> types = new HashSet<>(objectTypeList.size());
							for (Object objectType : objectTypeList) {
								types.add(((Map<String, String>) objectType).get(NGSIConstants.JSON_LD_ID));
							}
							if (typeFilter == null || !Collections.disjoint(types, typeFilter)) {
								Object hasObjectObj = map.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
								if (hasObjectObj != null && hasObjectObj instanceof List<?> objectList) {
									Set<String> ids = new HashSet<>(objectTypeList.size());
									for (Object objectObj : objectList) {
										ids.add(((Map<String, String>) objectObj).get(NGSIConstants.JSON_LD_ID));
									}
									if (!ids.isEmpty() && !types.isEmpty()) {
										result.put(types, ids);
									}
								}
							}
						}

					} else if (attrType.contains(NGSIConstants.NGSI_LD_LISTRELATIONSHIP)) {
						Object objectTypeObj = map.get(NGSIConstants.NGSI_LD_OBJECT_TYPE);
						if (objectTypeObj != null && objectTypeObj instanceof List<?> objectTypeList) {
							Set<String> types = new HashSet<>(objectTypeList.size());
							for (Object objectType : objectTypeList) {
								types.add(((Map<String, String>) objectType).get(NGSIConstants.JSON_LD_ID));
							}
							if (typeFilter == null || !Collections.disjoint(types, typeFilter)) {
								Object hasObjectListObj = map.get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
								if (hasObjectListObj != null && hasObjectListObj instanceof List<?> hasObjectList) {
									Object atListObj = hasObjectList.get(0);
									if (atListObj != null && atListObj instanceof Map<?, ?> atListMap) {
										Object atList = atListMap.get(NGSIConstants.JSON_LD_LIST);
										if (atList != null && atList instanceof List<?> hasObjects) {
											for (Object hasObjectObj : hasObjects) {
												if (hasObjectObj != null
														&& hasObjectObj instanceof List<?> objectList) {
													Set<String> ids = new HashSet<>(objectTypeList.size());
													for (Object objectObj : objectList) {
														ids.add(((Map<String, String>) objectObj)
																.get(NGSIConstants.JSON_LD_ID));
													}
													if (!ids.isEmpty() && !types.isEmpty()) {
														result.put(types, ids);
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

	}

	private void inlineEntity(Map<String, Object> entity, EntityCache entityCache, int currentJoinLevel, int joinLevel,
			boolean localOnly) {
		for (String attribName : entity.keySet()) {
			if (NGSIConstants.ENTITY_BASE_PROPS.contains(attribName)) {
				continue;
			}
			Object attribObj = entity.get(attribName);
			if (attribObj instanceof List list) {
				for (Object entry : list) {
					inlineAttrib(entry, entityCache, currentJoinLevel, joinLevel, localOnly);
				}
			} else {
				inlineAttrib(attribObj, entityCache, currentJoinLevel, joinLevel, localOnly);
			}

		}

	}

	private void inlineAttrib(Object attribObj, EntityCache entityCache, int currentJoinLevel, int joinLevel,
			boolean localOnly) {
		if (attribObj instanceof Map attribMap) {
			Object typeObj = attribMap.get(NGSIConstants.JSON_LD_TYPE);
			if (typeObj != null && typeObj instanceof List typeList) {
				if (typeList.contains(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
					List<Map<String, String>> hasObject = (List<Map<String, String>>) attribMap
							.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
					List<Map<String, Object>> entities = new ArrayList<>(hasObject.size());

					if (localOnly) {
						Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2EntityAndHost = entityCache
								.getAllIds2EntityAndHosts();
						for (Map<String, String> idEntry : hasObject) {
							String entityId = idEntry.get(NGSIConstants.JSON_LD_ID);
							Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> entityAndHosts = ids2EntityAndHost
									.get(entityId);
							if (entityAndHosts != null) {
								Map<String, Object> ogEntity = entityAndHosts.getItem1();
								if (ogEntity != null) {
									Map<String, Object> entity = MicroServiceUtils.deepCopyMap(ogEntity);
									entities.add(entity);
									if (currentJoinLevel + 1 <= joinLevel) {
										inlineEntity(entity, entityCache, currentJoinLevel + 1, joinLevel, localOnly);
									}
								}
							}
						}
					} else if (attribMap.containsKey(NGSIConstants.OBJECT_TYPE)) {
						List<Object> linkedTypes = (List<Object>) attribMap.get(NGSIConstants.OBJECT_TYPE);
						for (Object linkedTypeObj : linkedTypes) {
							if (linkedTypeObj instanceof Map linkedTypeMap
									&& linkedTypeMap.containsKey(NGSIConstants.JSON_LD_ID)) {
								String linkedType = (String) linkedTypeMap.get(NGSIConstants.JSON_LD_ID);
								Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2EntityAndHost = entityCache
										.getByType(linkedType);
								if (ids2EntityAndHost != null) {
									for (Map<String, String> idEntry : hasObject) {
										String entityId = idEntry.get(NGSIConstants.JSON_LD_ID);
										Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> entityAndHosts = ids2EntityAndHost
												.get(entityId);
										if (entityAndHosts != null) {
											Map<String, Object> ogEntity = entityAndHosts.getItem1();
											if (ogEntity != null
													&& ((List<String>) ogEntity.get(NGSIConstants.JSON_LD_TYPE))
															.contains(linkedType)) {
												Map<String, Object> entity = MicroServiceUtils.deepCopyMap(ogEntity);
												entities.add(entity);
												if (currentJoinLevel + 1 <= joinLevel) {
													inlineEntity(entity, entityCache, currentJoinLevel + 1, joinLevel,
															localOnly);
												}
											}
										}
									}
								}
							}
						}
					}

					if (!entities.isEmpty()) {
						attribMap.put(NGSIConstants.NGSI_LD_ENTITY, entities);
					}
				} else if (typeList.contains(NGSIConstants.NGSI_LD_LISTRELATIONSHIP)
						&& attribMap.containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST)) {

					List<Map<String, List<Map<String, List<Map<String, String>>>>>> hasObjectList = (List<Map<String, List<Map<String, List<Map<String, String>>>>>>) attribMap
							.get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
					List<Map<String, Object>> entities = new ArrayList<>(hasObjectList.size());

					if (localOnly) {
						Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2EntityAndHost = entityCache
								.getAllIds2EntityAndHosts();
						for (Map<String, List<Map<String, List<Map<String, String>>>>> atListEntry : hasObjectList) {
							List<Map<String, List<Map<String, String>>>> atList = atListEntry
									.get(NGSIConstants.JSON_LD_LIST);
							for (Map<String, List<Map<String, String>>> hasObjectEntry : atList) {
								List<Map<String, String>> objectList = hasObjectEntry
										.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
								for (Map<String, String> objectEntry : objectList) {
									String entityId = objectEntry.get(NGSIConstants.JSON_LD_ID);
									Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> entityAndHosts = ids2EntityAndHost
											.get(entityId);
									if (entityAndHosts != null) {
										Map<String, Object> ogEntity = entityAndHosts.getItem1();
										if (ogEntity != null) {
											Map<String, Object> entity = MicroServiceUtils.deepCopyMap(ogEntity);
											entities.add(entity);
											if (currentJoinLevel + 1 <= joinLevel) {
												inlineEntity(entity, entityCache, currentJoinLevel + 1, joinLevel,
														localOnly);
											}
										}
									}
								}

							}

						}
					} else if (attribMap.containsKey(NGSIConstants.OBJECT_TYPE)) {
						List<Object> linkedTypes = (List<Object>) attribMap.get(NGSIConstants.OBJECT_TYPE);
						for (Object linkedTypeObj : linkedTypes) {
							if (linkedTypeObj instanceof Map linkedTypeMap
									&& linkedTypeMap.containsKey(NGSIConstants.JSON_LD_ID)) {
								String linkedType = (String) linkedTypeMap.get(NGSIConstants.JSON_LD_ID);
								Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2EntityAndHost = entityCache
										.getByType(linkedType);
								if (ids2EntityAndHost != null) {
									for (Map<String, List<Map<String, List<Map<String, String>>>>> atListEntry : hasObjectList) {
										List<Map<String, List<Map<String, String>>>> atList = atListEntry
												.get(NGSIConstants.JSON_LD_LIST);
										for (Map<String, List<Map<String, String>>> hasObjectEntry : atList) {
											List<Map<String, String>> objectList = hasObjectEntry
													.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
											for (Map<String, String> objectEntry : objectList) {
												String entityId = objectEntry.get(NGSIConstants.JSON_LD_ID);
												Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> entityAndHosts = ids2EntityAndHost
														.get(entityId);
												if (entityAndHosts != null) {
													Map<String, Object> ogEntity = entityAndHosts.getItem1();
													if (ogEntity != null
															&& ((List<String>) ogEntity.get(NGSIConstants.JSON_LD_TYPE))
																	.contains(linkedType)) {
														Map<String, Object> entity = MicroServiceUtils
																.deepCopyMap(ogEntity);
														entities.add(entity);
														if (currentJoinLevel + 1 <= joinLevel) {
															inlineEntity(entity, entityCache, currentJoinLevel + 1,
																	joinLevel, localOnly);
														}
													}
												}
											}

										}

									}
								}
							}
						}
					}
					if (!entities.isEmpty()) {
						attribMap.put(NGSIConstants.NGSI_LD_ENTITY_LIST,
								List.of(Map.of(NGSIConstants.JSON_LD_LIST, entities)));
					}

				}
			}
		}
	}

	public Uni<EntityMap> getEntityMap(String tenant, String qToken) {
		return queryDAO.getEntityMap(tenant, qToken);
	}

	private Uni<Map<String, Map<String, Object>>> handle414(int baseLength, QueryRemoteHost remoteHost, String linkHead,
			String idsString, List<Object> contextLinks) {
		int maxLengthForIds = 2000 - baseLength;
		if (maxLengthForIds <= idsString.indexOf(",")) {
			logger.warn("failed to split up query");
			return Uni.createFrom().item(new HashMap<String, Map<String, Object>>());
		}
		int index;
		List<Uni<Map<String, Map<String, Object>>>> backUpUnis = Lists.newArrayList();

		while (!idsString.isEmpty()) {

			index = idsString.lastIndexOf(",", maxLengthForIds);
			String toUseIds = idsString.substring(0, index);

			backUpUnis.add(webClient
					.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "?id=" + toUseIds
							+ "&options=sysAttrs&limit=1000")
					.putHeaders(remoteHost.headers()).putHeader("Link", linkHead).send().onItem()
					.transformToUni(response -> {
						if (response != null && response.statusCode() == 200) {
							return ldService.expand(contextLinks, response.bodyAsJsonArray().getList(), opts,
									AppConstants.QUERY_PAYLOAD, false).onItem().transform(expanded -> {
										Map<String, Map<String, Object>> result = Maps.newHashMap();
										expanded.forEach(obj -> {
											Map<String, Object> entity = (Map<String, Object>) obj;
											entity.put(EntityTools.REG_MODE_KEY, remoteHost.regMode());
											result.put((String) entity.get(NGSIConstants.JSON_LD_ID), entity);
										});
										return result;
									});
						}
						logger.warn("failed to query remote host: " + remoteHost.host()
								+ NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "?id=" + toUseIds
								+ "&options=sysAttrs&limit=1000");
						if (response != null) {
							logger.debug("response code: " + response.statusCode());
							logger.debug("response : " + response.bodyAsString());
						} else {
							logger.debug("null response");
						}

						return Uni.createFrom().item(new HashMap<String, Map<String, Object>>(0));
					}).onFailure().recoverWithUni(e -> {
						logger.warn("failed to query with error " + e.getMessage());
						return Uni.createFrom().item(new HashMap<String, Map<String, Object>>(0));
					}));

			idsString = idsString.substring(index + 1);
			if (idsString.length() <= maxLengthForIds) {
				backUpUnis.add(webClient
						.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "?id=" + idsString
								+ "&options=sysAttrs&limit=1000")
						.putHeaders(remoteHost.headers()).putHeader("Link", linkHead).send().onItem()
						.transformToUni(response -> {
							if (response != null && response.statusCode() == 200) {
								return ldService.expand(contextLinks, response.bodyAsJsonArray().getList(), opts,
										AppConstants.QUERY_PAYLOAD, false).onItem().transform(expanded -> {
											Map<String, Map<String, Object>> result = Maps.newHashMap();
											expanded.forEach(obj -> {
												Map<String, Object> entity = (Map<String, Object>) obj;
												entity.put(EntityTools.REG_MODE_KEY, remoteHost.regMode());
												result.put((String) entity.get(NGSIConstants.JSON_LD_ID), entity);
											});
											return result;
										});
							}
							logger.warn("Failed to query entity list from remote host" + remoteHost.toString());
							if (response != null) {
								logger.debug("response code: " + response.statusCode());
								logger.debug("response : " + response.bodyAsString());
							} else {
								logger.debug("null response");
							}

							return Uni.createFrom().item(new HashMap<String, Map<String, Object>>(0));
						}).onFailure().recoverWithUni(e -> {
							logger.warn("failed to query with error " + e.getMessage());
							return Uni.createFrom().item(new HashMap<String, Map<String, Object>>(0));
						}));
				idsString = "";
			}
		}
		return Uni.combine().all().unis(backUpUnis).combinedWith(l1 -> {
			Map<String, Map<String, Object>> result = Maps.newHashMap();
			for (Object obj1 : l1) {
				Map<String, Map<String, Object>> m1 = (Map<String, Map<String, Object>>) obj1;
				result.putAll(m1);
			}
			return result;
		});

	}

	private void mergeEntity(String entityId, Map<String, Object> entity,
			Map<String, Map<String, Map<String, Map<String, Object>>>> entityId2AttrName2DatasetId2AttrValue,
			Map<String, Set<String>> entityId2Types, Map<String, Set<String>> entityId2Scopes,
			Map<String, Long> entityId2YoungestModified, Map<String, Long> entityId2OldestCreatedAt,
			Map<String, Map<String, Integer>> entityId2AttrDatasetId2CurrentRegMode) {
		int regMode = 1;
		logger.debug("first level merge for entityId: " + entityId);
		Map<String, Map<String, Map<String, Object>>> result = entityId2AttrName2DatasetId2AttrValue.get(entityId);
		if (result == null) {
			result = Maps.newHashMap();
			entityId2AttrName2DatasetId2AttrValue.put(entityId, result);
		}

		Map<String, Integer> attsDataset2CurrentRegMode = entityId2AttrDatasetId2CurrentRegMode.get(entityId);
		if (attsDataset2CurrentRegMode == null) {
			attsDataset2CurrentRegMode = Maps.newHashMap();
			entityId2AttrDatasetId2CurrentRegMode.put(entityId, attsDataset2CurrentRegMode);
		}
		if (entity.containsKey(EntityTools.REG_MODE_KEY)) {
			regMode = (Integer) entity.remove(EntityTools.REG_MODE_KEY);
		}
		Set<String> types = entityId2Types.get(entityId);
		if (types == null) {
			types = Sets.newHashSet();
			entityId2Types.put(entityId, types);
		}
		Set<String> scopes = entityId2Scopes.get(entityId);
		if (scopes == null) {
			scopes = Sets.newHashSet();
			entityId2Scopes.put(entityId, scopes);
		}
		long youngestModifiedAt = Long.MIN_VALUE;
		long oldestCreatedAt = Long.MAX_VALUE;
		if (entityId2YoungestModified.containsKey(entityId)) {
			youngestModifiedAt = entityId2YoungestModified.get(entityId);
		}
		if (entityId2OldestCreatedAt.containsKey(entityId)) {
			oldestCreatedAt = entityId2OldestCreatedAt.get(entityId);
		}

		for (Entry<String, Object> attrib : entity.entrySet()) {
			String key = attrib.getKey();
			if (key.equals(NGSIConstants.JSON_LD_ID)) {
				continue;
			} else if (key.equals(NGSIConstants.JSON_LD_TYPE)) {
				types.addAll((List<String>) attrib.getValue());
			} else if (key.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				Long modifiedAt = SerializationTools.date2Long(
						((List<Map<String, String>>) attrib.getValue()).get(0).get(NGSIConstants.JSON_LD_VALUE));
				if (modifiedAt > youngestModifiedAt) {
					entityId2YoungestModified.put(entityId, modifiedAt);
				}
			} else if (key.equals(NGSIConstants.NGSI_LD_CREATED_AT)) {
				Long createdAt = SerializationTools.date2Long(
						((List<Map<String, String>>) attrib.getValue()).get(0).get(NGSIConstants.JSON_LD_VALUE));
				if (createdAt < oldestCreatedAt) {
					entityId2OldestCreatedAt.put(entityId, createdAt);
				}
			} else if (key.equals(NGSIConstants.NGSI_LD_SCOPE)) {
				List<Map<String, String>> tmpList = (List<Map<String, String>>) attrib.getValue();
				for (Map<String, String> scope : tmpList) {
					scopes.add(scope.get(NGSIConstants.JSON_LD_VALUE));
				}
			} else {
				mergeAttr(key, (List<Map<String, Object>>) attrib.getValue(), result, regMode,
						attsDataset2CurrentRegMode);
			}

		}

	}

	private Uni<QueryResult> localQueryLevel1(String tenant, String[] id, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery,
			LanguageQueryTerm langQuery, int limit, int offSet, boolean count, DataSetIdTerm dataSetIdTerm, String join,
			int joinLevel, PickTerm pickTerm, OmitTerm omitTerm) {
		return queryDAO
				.queryLocalOnly(tenant, id, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, langQuery,
						limit, offSet, count, dataSetIdTerm, join, joinLevel, pickTerm, omitTerm)
				.onItem().transform(rows -> {
					QueryResult result = new QueryResult();
					if (limit == 0 && count) {
						result.setCount(rows.iterator().next().getLong(0));
					} else {
						RowIterator<Row> it = rows.iterator();
						Row next = null;

						List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>(rows.size());
						Map<String, Object> entity;
						while (it.hasNext()) {
							next = it.next();
							entity = next.getJsonObject(0).getMap();
							resultData.add(entity);

						}
						if (count) {
							Long resultCount = next.getLong(1);
							result.setCount(resultCount);
							long leftAfter = resultCount - (offSet + limit);
							if (leftAfter < 0) {
								leftAfter = 0;
							}
							result.setResultsLeftAfter(leftAfter);
						} else {
							if (resultData.size() < limit) {
								result.setResultsLeftAfter(0l);
							} else {
								result.setResultsLeftAfter((long) limit);
							}

						}
						long leftBefore = offSet;

						result.setResultsLeftBefore(leftBefore);
						result.setLimit(limit);
						result.setOffset(offSet);
						result.setData(resultData);
					}

					return result;
				});
	}

	public Uni<List<Map<String, Object>>> getTypesWithDetail(String tenant, boolean localOnly) {
		Uni<List<Map<String, Object>>> local = queryDAO.getTypesWithDetails(tenant);
		if (localOnly) {
			return local;
		}
		Uni<Map<String, Set<String>>> queryRemoteTypes = Uni.combine().all()
				.unis(queryDAO.getRemoteSourcesForTypesWithDetails(tenant),
						queryDAO.getRemoteTypesWithDetailsForRegWithoutTypeSupport(tenant))
				.asTuple().onItem().transformToUni(t -> {
					Map<String, Set<String>> currentType2Attrib = t.getItem2();
					RowSet<Row> rows = t.getItem1();
					if (rows.size() > 0) {
						List<Uni<List<Object>>> unis = getRemoteCalls(rows,
								NGSIConstants.NGSI_LD_TYPES_ENDPOINT + "?details=true");
						return Uni.combine().all().unis(unis).combinedWith(list -> {
							for (Object entry : list) {
								if (((List) entry).isEmpty()) {
									continue;
								}
								List<Map<String, Object>> typeList = (List<Map<String, Object>>) entry;

								mergeTypeListWithDetails(typeList, currentType2Attrib);

							}
							return currentType2Attrib;
						});
					} else {
						return Uni.createFrom().item(currentType2Attrib);
					}
				});

		return Uni.combine().all().unis(local, queryRemoteTypes).asTuple().onItem().transform(t -> {
			List<Map<String, Object>> localResult = t.getItem1();
			Map<String, Set<String>> remoteResults = t.getItem2();
			if (!remoteResults.isEmpty()) {
				mergeTypeListWithDetails(localResult, remoteResults);
				localResult.clear();
				for (Entry<String, Set<String>> entry : remoteResults.entrySet()) {
					Map<String, Object> resultEntry = Maps.newHashMap();
					String type = entry.getKey();
					resultEntry.put(NGSIConstants.JSON_LD_ID, type);
					List<Map<String, String>> typeName = Lists.newArrayList();
					Map<String, String> typeEntry = Maps.newHashMap();
					typeEntry.put(NGSIConstants.JSON_LD_ID, type);
					resultEntry.put(NGSIConstants.NGSI_LD_TYPE_NAME, typeName);
					resultEntry.put(NGSIConstants.JSON_LD_TYPE, Lists.newArrayList(NGSIConstants.NGSI_LD_ENTITY_TYPE));
					List<Map<String, String>> attribList = Lists.newArrayList();
					for (String attrib : entry.getValue()) {
						Map<String, String> attribValue = Maps.newHashMap();
						attribValue.put(NGSIConstants.JSON_LD_ID, attrib);
						attribList.add(attribValue);
					}
					resultEntry.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAMES, attribList);
					localResult.add(resultEntry);
				}
			}
			return localResult;
		});
	}

	private void mergeTypeListWithDetails(List<Map<String, Object>> typeList,
			Map<String, Set<String>> currentType2Attrib) {
		String type;
		List<Map<String, String>> attribs;
		for (Map<String, Object> entry : typeList) {
			type = (String) entry.get(NGSIConstants.JSON_LD_ID);
			attribs = (List<Map<String, String>>) entry.get(NGSIConstants.NGSI_LD_ATTRIBUTE_NAMES);
			Set<String> currentAttribs;
			if (currentType2Attrib.containsKey(type)) {
				currentAttribs = currentType2Attrib.get(type);
			} else {
				currentAttribs = Sets.newHashSet();
				currentType2Attrib.put(type, currentAttribs);
			}
			for (Map<String, String> attrib : attribs) {
				currentAttribs.add(attrib.get(NGSIConstants.JSON_LD_ID));
			}
		}

	}

	public Uni<Map<String, Object>> getTypes(String tenant, boolean localOnly) {
		Uni<Map<String, Object>> local = queryDAO.getTypes(tenant);
		if (localOnly) {
			return local;
		}

		Uni<Set<String>> remoteTypes = Uni.combine().all()
				.unis(queryDAO.getRemoteSourcesForTypes(tenant),
						queryDAO.getRemoteTypesForRegWithoutTypesSupport(tenant))
				.asTuple().onItem().transformToUni(t -> {
					RowSet<Row> rows = t.getItem1();
					Set<String> currentTypes = Sets.newHashSet(t.getItem2());
					if (rows.size() > 0) {
						List<Uni<List<Object>>> unis = getRemoteCalls(rows, NGSIConstants.NGSI_LD_TYPES_ENDPOINT);
						return Uni.combine().all().unis(unis).combinedWith(list -> {
							for (Object entry : list) {
								if (!((List) entry).isEmpty()) {
									Map<String, Object> typeMap = ((List<Map<String, Object>>) entry).get(0);
									mergeTypeList(typeMap.get(NGSIConstants.NGSI_LD_TYPE_LIST), currentTypes);
								}
							}
							return currentTypes;
						});
					} else {
						return Uni.createFrom().item(currentTypes);
					}

				});

		return Uni.combine().all().unis(local, remoteTypes).asTuple().onItem().transform(t -> {
			Map<String, Object> localResult = t.getItem1();
			Set<String> remoteResult = t.getItem2();
			if (!remoteResult.isEmpty()) {
				mergeTypeList(localResult.get(NGSIConstants.NGSI_LD_TYPE_LIST), remoteResult);
				List<Map<String, String>> newTypeList = Lists.newArrayList();
				for (String type : remoteResult) {
					Map<String, String> tmp = Maps.newHashMap();
					tmp.put(NGSIConstants.JSON_LD_ID, type);
					newTypeList.add(tmp);
				}
				localResult.put(NGSIConstants.NGSI_LD_TYPE_LIST, newTypeList);
			}
			return localResult;
		});
	}

	private void mergeTypeList(Object newTypeListObj, Set<String> currentTypes) {
		if (newTypeListObj == null) {
			return;
		}
		List<Map<String, String>> newTypeList = ((List<Map<String, String>>) newTypeListObj);
		for (Map<String, String> entry : newTypeList) {
			currentTypes.add(entry.get(NGSIConstants.JSON_LD_ID));

		}

	}

	public Uni<Map<String, Object>> getType(String tenant, String type, boolean localOnly) {
		Uni<Map<String, Object>> local = queryDAO.getType(tenant, type);
		if (localOnly) {
			return local;
		}
		Uni<Tuple2<Map<String, Set<String>>, Long>> remoteAttribs2AttribTypeAndCount = Uni.combine().all()
				.unis(queryDAO.getRemoteSourcesForType(tenant, type),
						queryDAO.getRemoteTypeInfoForRegWithOutTypeSupport(tenant, type))
				.asTuple().onItem().transformToUni(t -> {

					Map<String, Set<String>> attribId2AttribType = t.getItem2();
					RowSet<Row> rows = t.getItem1();
					if (rows.size() > 0) {
						List<Uni<List<Object>>> remoteResults = getRemoteCalls(rows,
								NGSIConstants.NGSI_LD_TYPES_ENDPOINT + "/" + type);
						return Uni.combine().all().unis(remoteResults).combinedWith(list -> {
							long count = 0;
							for (Object obj : list) {
								if (!((List) obj).isEmpty()) {
									Map<String, Object> typeInfo = ((List<Map<String, Object>>) obj).get(0);
									count += ((List<Map<String, Long>>) typeInfo
											.get(NGSIConstants.NGSI_LD_ENTITY_COUNT)).get(0)
											.get(NGSIConstants.JSON_LD_VALUE);
									List<Map<String, Object>> attributeDetails = (List<Map<String, Object>>) typeInfo
											.get(NGSIConstants.NGSI_LD_ATTRIBUTE_DETAILS);
									for (Map<String, Object> attrDetail : attributeDetails) {
										String attrName = (String) attrDetail.get(NGSIConstants.JSON_LD_ID);
										Set<String> types = attribId2AttribType.get(attrName);
										if (types == null) {
											types = Sets.newHashSet();
											attribId2AttribType.put(attrName, types);
										}
										List<Map<String, String>> attrTypes = (List<Map<String, String>>) attrDetail
												.get(NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES);
										for (Map<String, String> typeEntry : attrTypes) {
											types.add(typeEntry.get(NGSIConstants.JSON_LD_ID));
										}
									}
								}

							}
							return Tuple2.of(attribId2AttribType, count);
						});
					} else {
						return Uni.createFrom().item(Tuple2.of(attribId2AttribType, 0l));
					}
				});
		return Uni.combine().all().unis(local, remoteAttribs2AttribTypeAndCount).asTuple().onItem().transform(t -> {
			Map<String, Object> localResult = t.getItem1();
			Tuple2<Map<String, Set<String>>, Long> remoteResult = t.getItem2();
			Long remoteCount = remoteResult.getItem2();
			Map<String, Set<String>> attribId2AttribType = remoteResult.getItem1();
			if (!attribId2AttribType.isEmpty()) {
				List<Map<String, Object>> attributeDetails = (List<Map<String, Object>>) localResult
						.get(NGSIConstants.NGSI_LD_ATTRIBUTE_DETAILS);
				for (Map<String, Object> attrDetail : attributeDetails) {
					String attrName = (String) attrDetail.get(NGSIConstants.JSON_LD_ID);
					Set<String> types = attribId2AttribType.get(attrName);
					if (types == null) {
						types = Sets.newHashSet();
						attribId2AttribType.put(attrName, types);
					}
					List<Map<String, String>> attrTypes = (List<Map<String, String>>) attrDetail
							.get(NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES);
					for (Map<String, String> typeEntry : attrTypes) {
						types.add(typeEntry.get(NGSIConstants.JSON_LD_ID));
					}
				}
				remoteCount += ((List<Map<String, Long>>) localResult.get(NGSIConstants.NGSI_LD_ENTITY_COUNT)).get(0)
						.get(NGSIConstants.JSON_LD_VALUE);
				((List<Map<String, Long>>) localResult.get(NGSIConstants.NGSI_LD_ENTITY_COUNT)).get(0)
						.put(NGSIConstants.JSON_LD_VALUE, remoteCount);
				List<Map<String, Object>> newAttribDetails = Lists.newArrayList();
				for (Entry<String, Set<String>> attr2Type : attribId2AttribType.entrySet()) {
					Map<String, Object> attrEntry = Maps.newHashMap();
					attrEntry.put(NGSIConstants.JSON_LD_ID, attr2Type.getKey());
					attrEntry.put(NGSIConstants.JSON_LD_TYPE, Lists.newArrayList(NGSIConstants.NGSI_LD_ATTRIBUTE));
					Map<String, String> tmp = Maps.newHashMap();
					tmp.put(NGSIConstants.JSON_LD_ID, attr2Type.getKey());
					attrEntry.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAME, Lists.newArrayList(tmp));
					List<Map<String, String>> attrTypes = Lists.newArrayList();
					for (String attrType : attr2Type.getValue()) {
						tmp = Maps.newHashMap();
						tmp.put(NGSIConstants.JSON_LD_ID, attrType);
						attrTypes.add(tmp);
					}
					attrEntry.put(NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES, attrTypes);
					newAttribDetails.add(attrEntry);
				}
				localResult.put(NGSIConstants.NGSI_LD_ATTRIBUTE_DETAILS, newAttribDetails);
			}
			return localResult;
		});
	}

	public Uni<List<Map<String, Object>>> getAttribsWithDetails(String tenant, boolean localOnly) {
		Uni<List<Map<String, Object>>> local = queryDAO.getAttributesDetail(tenant);
		if (localOnly) {
			return local;
		}
		Uni<Map<String, Set<String>>> remoteAttribs = Uni.combine().all()
				.unis(queryDAO.getRemoteSourcesForAttribsWithDetails(tenant),
						queryDAO.getRemoteAttribsWithDetailsForRegWithoutAttribSupport(tenant))
				.asTuple().onItem().transformToUni(t -> {
					RowSet<Row> rows = t.getItem1();
					Map<String, Set<String>> attrib2EntityTypes = t.getItem2();
					if (rows.size() > 0) {
						List<Uni<List<Object>>> remoteResults = getRemoteCalls(rows,
								NGSIConstants.NGSI_LD_ATTRIBUTES_ENDPOINT + "?details=true");
						return Uni.combine().all().unis(remoteResults).combinedWith(list -> {
							for (Object obj : list) {
								if (((List) obj).isEmpty()) {
									continue;
								}
								List<Map<String, Object>> attribList = ((List<Map<String, Object>>) obj);
								for (Map<String, Object> entry : attribList) {
									List<Map<String, String>> entryEntityTypes = (List<Map<String, String>>) entry
											.get(NGSIConstants.NGSI_LD_TYPE_NAMES);
									for (Map<String, String> typeEntry : entryEntityTypes) {
										Set<String> entityTypes = attrib2EntityTypes
												.get((String) entry.get(NGSIConstants.JSON_LD_ID));
										if (entityTypes == null) {
											entityTypes = Sets.newHashSet();
											attrib2EntityTypes.put((String) entry.get(NGSIConstants.JSON_LD_ID),
													entityTypes);
										}
										entityTypes.add(typeEntry.get(NGSIConstants.JSON_LD_ID));
									}
								}

							}
							return attrib2EntityTypes;
						});
					} else {
						return Uni.createFrom().item(attrib2EntityTypes);
					}
				});
		return Uni.combine().all().unis(local, remoteAttribs).asTuple().onItem().transform(t -> {
			List<Map<String, Object>> localResult = t.getItem1();
			Map<String, Set<String>> remoteResult = t.getItem2();
			if (!remoteResult.isEmpty()) {
				for (Map<String, Object> entry : localResult) {
					List<Map<String, String>> entryEntityTypes = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_TYPE_NAMES);
					for (Map<String, String> typeEntry : entryEntityTypes) {
						Set<String> entityTypes = remoteResult.get((String) entry.get(NGSIConstants.JSON_LD_ID));
						if (entityTypes == null) {
							entityTypes = Sets.newHashSet();
							remoteResult.put((String) entry.get(NGSIConstants.JSON_LD_ID), entityTypes);
						}
						entityTypes.add(typeEntry.get(NGSIConstants.JSON_LD_ID));
					}

				}
				localResult = Lists.newArrayList();
				for (Entry<String, Set<String>> entry : remoteResult.entrySet()) {
					List<Map<String, String>> types = Lists.newArrayList();
					for (String type : entry.getValue()) {
						types.add(Map.of(NGSIConstants.JSON_LD_ID, type));
					}
					localResult.add(Map.of(NGSIConstants.JSON_LD_ID, entry.getKey(), NGSIConstants.JSON_LD_TYPE,
							List.of(NGSIConstants.NGSI_LD_ATTRIBUTE), NGSIConstants.NGSI_LD_ATTRIBUTE_NAME,
							List.of(Map.of(NGSIConstants.JSON_LD_ID, entry.getKey())), NGSIConstants.NGSI_LD_TYPE_NAMES,
							types));
				}
			}
			return localResult;
		});

	}

	public Uni<Map<String, Object>> getAttribs(String tenant, boolean localOnly) {
		Uni<Map<String, Object>> local = queryDAO.getAttributeList(tenant);
		if (localOnly) {
			return local;
		}
		Uni<Set<String>> remoteAttribs = Uni.combine().all()
				.unis(queryDAO.getRemoteSourcesForAttribs(tenant),
						queryDAO.getRemoteAttribsForRegWithoutAttribSupport(tenant))
				.asTuple().onItem().transformToUni(t -> {
					RowSet<Row> rows = t.getItem1();
					Set<String> attribIds = t.getItem2();
					if (rows.size() > 0) {
						List<Uni<List<Object>>> remoteResults = getRemoteCalls(rows,
								NGSIConstants.NGSI_LD_ATTRIBUTES_ENDPOINT);
						return Uni.combine().all().unis(remoteResults).combinedWith(list -> {
							for (Object obj : list) {
								if (((List) obj).isEmpty()) {
									continue;
								}
								Map<String, Object> payload = ((List<Map<String, Object>>) obj).get(0);
								List<Map<String, String>> entryAttribIds = (List<Map<String, String>>) payload
										.get(NGSIConstants.NGSI_LD_ATTRIBUTE_LIST_ATTRIBUTE_KEY);

								for (Map<String, String> entry : entryAttribIds) {
									attribIds.add(entry.get(NGSIConstants.JSON_LD_ID));
								}
							}
							return attribIds;
						});
					} else {
						return Uni.createFrom().item(attribIds);
					}
				});

		return Uni.combine().all().unis(local, remoteAttribs).asTuple().onItem().transform(t -> {
			Map<String, Object> localResult = t.getItem1();
			Set<String> remoteAttribIds = t.getItem2();
			if (!remoteAttribIds.isEmpty()) {
				List<Map<String, String>> entryAttribIds = (List<Map<String, String>>) localResult
						.get(NGSIConstants.NGSI_LD_ATTRIBUTE_LIST_ATTRIBUTE_KEY);

				for (Map<String, String> entry : entryAttribIds) {
					remoteAttribIds.add(entry.get(NGSIConstants.JSON_LD_ID));
				}
				List<Map<String, String>> newAttribIds = Lists.newArrayList();
				for (String entry : remoteAttribIds) {
					newAttribIds.add(Map.of(NGSIConstants.JSON_LD_ID, entry));
				}
				localResult.put(NGSIConstants.NGSI_LD_ATTRIBUTE_LIST_ATTRIBUTE_KEY, newAttribIds);
			}
			return localResult;
		});
	}

	public Uni<Map<String, Object>> getAttrib(String tenant, String attrib, boolean localOnly) {
		Uni<Map<String, Object>> local = queryDAO.getAttributeDetail(tenant, attrib);
		if (localOnly) {
			return local;
		}
		Uni<Tuple3<Long, Set<String>, Set<String>>> remoteAttrib = Uni.combine().all()
				.unis(queryDAO.getRemoteSourcesForAttrib(tenant, attrib),
						queryDAO.getRemoteAttribForRegWithoutAttribSupport(tenant, attrib))
				.asTuple().onItem().transformToUni(t -> {
					RowSet<Row> rows = t.getItem1();
					Tuple2<Set<String>, Set<String>> t2 = t.getItem2();
					Set<String> attribTypes = t2.getItem1();
					Set<String> entityTypes = t2.getItem2();
					if (rows.size() > 0) {
						List<Uni<List<Object>>> remoteResults = getRemoteCalls(rows,
								NGSIConstants.NGSI_LD_ATTRIBUTES_ENDPOINT + "/" + attrib);
						return Uni.combine().all().unis(remoteResults).combinedWith(list -> {
							long count = 0;
							for (Object obj : list) {
								if (((List) obj).isEmpty()) {
									continue;
								}
								Map<String, Object> payload = ((List<Map<String, Object>>) obj).get(0);
								count += ((List<Map<String, Long>>) payload.get(NGSIConstants.NGSI_LD_ATTRIBUTE_COUNT))
										.get(0).get(NGSIConstants.JSON_LD_VALUE);
								List<Map<String, String>> entryAttribTypes = (List<Map<String, String>>) payload
										.get(NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES);
								List<Map<String, String>> entryEntityTypes = (List<Map<String, String>>) payload
										.get(NGSIConstants.NGSI_LD_TYPE_LIST);
								for (Map<String, String> entry : entryAttribTypes) {
									attribTypes.add(entry.get(NGSIConstants.JSON_LD_ID));
								}
								for (Map<String, String> entry : entryEntityTypes) {
									entityTypes.add(entry.get(NGSIConstants.JSON_LD_ID));
								}
							}
							return Tuple3.of(count, entityTypes, attribTypes);
						});
					} else {
						return Uni.createFrom().item(Tuple3.of(0l, entityTypes, attribTypes));
					}
				});
		return Uni.combine().all().unis(local, remoteAttrib).asTuple().onItem().transform(t -> {
			Map<String, Object> localResult = t.getItem1();
			Tuple3<Long, Set<String>, Set<String>> remoteResult = t.getItem2();
			return mergeAttribDetails(localResult, remoteResult, attrib);

		});
	}

	private Map<String, Object> mergeAttribDetails(Map<String, Object> localResult,
			Tuple3<Long, Set<String>, Set<String>> remoteResult, String attrib) {
		if (!remoteResult.getItem2().isEmpty()) {
			Set<String> entityTypes = remoteResult.getItem2();
			Set<String> attribTypes = remoteResult.getItem3();
			long count = remoteResult.getItem1()
					+ ((List<Map<String, Long>>) localResult.get(NGSIConstants.NGSI_LD_ATTRIBUTE_COUNT)).get(0)
							.get(NGSIConstants.JSON_LD_VALUE);
			((List<Map<String, Long>>) localResult.get(NGSIConstants.NGSI_LD_ATTRIBUTE_COUNT)).get(0)
					.put(NGSIConstants.JSON_LD_VALUE, count);

			List<Map<String, String>> entryAttribTypes = (List<Map<String, String>>) localResult
					.get(NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES);
			List<Map<String, String>> entryEntityTypes = (List<Map<String, String>>) localResult
					.get(NGSIConstants.NGSI_LD_TYPE_LIST);
			for (Map<String, String> entry : entryAttribTypes) {
				attribTypes.add(entry.get(NGSIConstants.JSON_LD_ID));
			}
			for (Map<String, String> entry : entryEntityTypes) {
				entityTypes.add(entry.get(NGSIConstants.JSON_LD_ID));
			}
			List<Map<String, String>> newEntryAttribTypes = Lists.newArrayList();
			List<Map<String, String>> newEntryEntityTypes = Lists.newArrayList();
			attribTypes.forEach(attribType -> {
				newEntryAttribTypes.add(Map.of(NGSIConstants.JSON_LD_ID, attribType));
			});
			entityTypes.forEach(entityType -> {
				newEntryEntityTypes.add(Map.of(NGSIConstants.JSON_LD_ID, entityType));
			});
			localResult.put(NGSIConstants.NGSI_LD_TYPE_LIST, newEntryEntityTypes);
			localResult.put(NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES, newEntryAttribTypes);
			localResult.put(NGSIConstants.JSON_LD_ID, attrib);
			localResult.put(NGSIConstants.JSON_LD_TYPE, List.of(NGSIConstants.NGSI_LD_ATTRIBUTE));
		}
		return localResult;
	}

	private List<Uni<List<Object>>> getRemoteCalls(RowSet<Row> rows, String endpoint) {
		List<Uni<List<Object>>> unis = Lists.newArrayList();
		rows.forEach(row -> {
			// C.endpoint C.tenant_id, c.headers, c.reg_mode
			String host = row.getString(0);
			MultiMap remoteHeaders = MultiMap
					.newInstance(HttpUtils.getHeadersForRemoteCall(row.getJsonArray(2), row.getString(1)));
			unis.add(webClient.getAbs(host + endpoint).putHeaders(remoteHeaders).send().onFailure().recoverWithNull()
					.onItem().transformToUni(response -> {
						String responseTypes;
						if (response == null || response.statusCode() != 200) {
							return Uni.createFrom().item(Lists.newArrayList());
						} else {
							responseTypes = response.bodyAsString();
							try {
								return JsonUtils.fromString(responseTypes).onItem().transformToUni(json -> {
									return ldService.expand(HttpUtils.getContextFromHeader(remoteHeaders), json, opts,
											-1, false);
								});
							} catch (Exception e) {
								logger.debug("failed to handle response from host " + host + " : " + responseTypes, e);
								return Uni.createFrom().item(Lists.newArrayList());
							}
						}

					}).onFailure().recoverWithItem(e -> Lists.newArrayList()));
		});
		return unis;
	}

	public Uni<Map<String, Object>> retrieveEntity(Context context, String tenant, String entityId, String attrs,
			LanguageQueryTerm lang, boolean localOnly, String containedBy, String join, boolean idsOnly,
			int joinLevel) {
		AttrsQueryTerm attrsQuery;
		try {
			attrsQuery = QueryParser.parseAttrs(attrs, context);
		} catch (ResponseException e) {
			throw new RuntimeException(e);
		}
		Map<String, Object> attrsMap = QueryParser.parseInput(attrs);
		Uni<Map<String, Object>> entity;
		if (idsOnly) {
			entity = idsOnly(
					getEntityFlat(context, tenant, entityId, attrsMap, lang, localOnly, containedBy, joinLevel, null));
		} else if (join == null) {
			entity = retrieveEntity(context, tenant, entityId, attrsQuery, lang, localOnly);
		} else if (join.equals(NGSIConstants.FLAT)) {
			entity = getEntityFlat(context, tenant, entityId, attrsMap, lang, localOnly, containedBy, joinLevel, null);
		} else if (join.equals(NGSIConstants.INLINE)) {
			entity = getEntityInline(context, tenant, entityId, attrsMap, lang, localOnly, containedBy, joinLevel);
		} else {
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData));
		}
		return entity;
	}

	public Uni<Map<String, Object>> retrieveEntity(Context context, String tenant, String entityId,
			AttrsQueryTerm attrsQuery, LanguageQueryTerm lang, boolean localOnly) {
		Uni<Map<String, Object>> local;
		local = queryDAO.getEntity(entityId, tenant, attrsQuery, null);
		if (localOnly) {
			return local;
		}
		Set<QueryRemoteHost> remoteHosts = Sets
				.newHashSet(getRemoteHostsForRetrieve(tenant, entityId, attrsQuery, lang, context));
		if (remoteHosts.isEmpty()) {
			return local.onItem().transformToUni(item -> {
				if (item.isEmpty()) {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.NotFound, entityId + " was not found"));
				}
				return Uni.createFrom().item(item);
			});
		}
		List<Uni<Map<String, Object>>> unis = new ArrayList<>(remoteHosts.size() + 1);
		unis.add(local);
		for (QueryRemoteHost remoteHost : remoteHosts) {
			String queryString;
			if (remoteHost.queryString() == null || remoteHost.queryString().isBlank()) {
				queryString = "?options=sysAttrs";
			} else {
				queryString = remoteHost.queryString() + "&options=sysAttrs";
			}
			unis.add(webClient
					.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId + queryString)
					.putHeaders(remoteHost.headers()).send().onItem().transformToUni(response -> {
						if (response == null || response.statusCode() != 200) {
							return Uni.createFrom().item(new HashMap<String, Object>());
						}
						Map<String, Object> result = response.bodyAsJsonObject().getMap();
						return ldService
								.expand(HttpUtils.getContextFromHeader(remoteHost.headers()), result, opts, -1, false)
								.onItem().transform(expanded -> {
									Map<String, Object> myResult = (Map<String, Object>) expanded.get(0);
									myResult.put(EntityTools.REG_MODE_KEY, remoteHost.regMode());
									return myResult;
								}).onFailure().recoverWithItem(e -> {
									logger.warn("Failed to expand body from remote source", e);
									return new HashMap<String, Object>();
								});
					}).onFailure().recoverWithItem(e -> {
						logger.warn("Failed to retrieve infos from host " + remoteHost, e);
						return new HashMap<String, Object>();
					}));
		}

		return Uni.combine().all().unis(unis).combinedWith(list -> {
			Map<String, Map<String, Map<String, Object>>> result = Maps.newHashMap();
			Set<String> types = Sets.newHashSet();
			Set<String> scopes = Sets.newHashSet();
			long oldestCreatedAt = Long.MAX_VALUE;
			long youngestModifiedAt = Long.MIN_VALUE;
			String id = null;
			Map<String, Integer> attsDataset2CurrentRegMode = Maps.newHashMap();
			for (Object obj : list) {
				// regmode 2 is redirect meaning it is expected to be something we is merged by
				// normal rules
				int regMode = 1;
				Map<String, Object> tmpEntity = (Map<String, Object>) obj;
				if (tmpEntity.containsKey(EntityTools.REG_MODE_KEY)) {
					regMode = (int) tmpEntity.remove(EntityTools.REG_MODE_KEY);
				}

				for (Entry<String, Object> attrib : tmpEntity.entrySet()) {
					String key = attrib.getKey();
					if (key.equals(NGSIConstants.JSON_LD_ID)) {
						id = (String) attrib.getValue();
					} else if (key.equals(NGSIConstants.JSON_LD_TYPE)) {
						types.addAll((List<String>) attrib.getValue());
					} else if (key.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
						Long modifiedAt = SerializationTools.date2Long(((List<Map<String, String>>) attrib.getValue())
								.get(0).get(NGSIConstants.JSON_LD_VALUE));
						if (modifiedAt > youngestModifiedAt) {
							youngestModifiedAt = modifiedAt;
						}
					} else if (key.equals(NGSIConstants.NGSI_LD_CREATED_AT)) {
						Long createdAt = SerializationTools.date2Long(((List<Map<String, String>>) attrib.getValue())
								.get(0).get(NGSIConstants.JSON_LD_VALUE));
						if (createdAt < oldestCreatedAt) {
							oldestCreatedAt = createdAt;
						}
					} else if (key.equals(NGSIConstants.NGSI_LD_SCOPE)) {
						List<Map<String, String>> tmpList = (List<Map<String, String>>) attrib.getValue();
						for (Map<String, String> scope : tmpList) {
							scopes.add(scope.get(NGSIConstants.JSON_LD_VALUE));
						}
					} else {
						mergeAttr(key, (List<Map<String, Object>>) attrib.getValue(), result, regMode,
								attsDataset2CurrentRegMode);
					}

				}

			}
			Map<String, Object> realResult = Maps.newHashMap();
			if (id != null) {
				realResult.put(NGSIConstants.JSON_LD_ID, id);
				realResult.put(NGSIConstants.NGSI_LD_CREATED_AT,
						List.of(Map.of(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME,
								NGSIConstants.JSON_LD_VALUE, SerializationTools.toDateTimeString(oldestCreatedAt))));
				realResult.put(NGSIConstants.NGSI_LD_MODIFIED_AT,
						List.of(Map.of(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME,
								NGSIConstants.JSON_LD_VALUE, SerializationTools.toDateTimeString(youngestModifiedAt))));
			}
			if (!types.isEmpty()) {
				realResult.put(NGSIConstants.JSON_LD_TYPE, Lists.newArrayList(types));
			}
			if (!scopes.isEmpty()) {
				List<Map<String, String>> scopesResult = new ArrayList<>(scopes.size());
				for (String scope : scopes) {
					scopesResult.add(Map.of(NGSIConstants.JSON_LD_VALUE, scope));
				}
				realResult.put(NGSIConstants.NGSI_LD_SCOPE, scopesResult);
			}
			for (Entry<String, Map<String, Map<String, Object>>> attrEntry : result.entrySet()) {
				realResult.put(attrEntry.getKey(), Lists.newArrayList(attrEntry.getValue().values()));
			}
			return realResult;
		}).onItem().transformToUni(result -> {
			if (result.isEmpty()) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, entityId + " was not found"));
			}
			return Uni.createFrom().item(result);
		});
	}

	private void mergeAttr(String key, List<Map<String, Object>> value,
			Map<String, Map<String, Map<String, Object>>> result, int regMode,
			Map<String, Integer> attsDataset2CurrentRegMode) {

		if (!result.containsKey(key)) {
			Map<String, Map<String, Object>> attribMap = new HashMap<>(value.size());
			result.put(key, attribMap);
			for (Map<String, Object> attrEntry : value) {
				if (attrEntry.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
					String datasetId = ((List<Map<String, String>>) attrEntry.get(NGSIConstants.NGSI_LD_DATA_SET_ID))
							.get(0).get(NGSIConstants.JSON_LD_ID);
					attsDataset2CurrentRegMode.put(key + datasetId, regMode);
					attribMap.put(datasetId, attrEntry);
				} else {
					attsDataset2CurrentRegMode.put(key + NGSIConstants.DEFAULT_DATA_SET_ID, regMode);
					attribMap.put(NGSIConstants.DEFAULT_DATA_SET_ID, attrEntry);
				}
			}
		} else {
			Map<String, Map<String, Object>> attribMap = result.get(key);
			for (Map<String, Object> attrEntry : value) {
				String datasetId;
				if (attrEntry.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
					datasetId = ((List<Map<String, String>>) attrEntry.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0)
							.get(NGSIConstants.JSON_LD_ID);
				} else {
					datasetId = NGSIConstants.DEFAULT_DATA_SET_ID;
				}
				if (attribMap.containsKey(datasetId)) {
					Integer currentRegMode = attsDataset2CurrentRegMode.get(key + datasetId);
					if (regMode == 3 || currentRegMode == 0) {
						attribMap.put(datasetId, attrEntry);
						attsDataset2CurrentRegMode.put(key + datasetId, regMode);
						continue;
					}
					if (currentRegMode == 3 || regMode == 0) {
						continue;
					}
					Map<String, Object> currentValue = attribMap.get(datasetId);
					Long currentModifiedDate = SerializationTools
							.date2Long(((List<Map<String, String>>) currentValue.get(NGSIConstants.NGSI_LD_MODIFIED_AT))
									.get(0).get(NGSIConstants.JSON_LD_VALUE));
					Long newModifiedDate = SerializationTools
							.date2Long(((List<Map<String, String>>) attrEntry.get(NGSIConstants.NGSI_LD_MODIFIED_AT))
									.get(0).get(NGSIConstants.JSON_LD_VALUE));
					if (newModifiedDate > currentModifiedDate) {
						attribMap.put(datasetId, attrEntry);
						attsDataset2CurrentRegMode.put(key + datasetId, regMode);
					}

				} else {
					attribMap.put(datasetId, attrEntry);
				}
			}
		}

	}

	private List<QueryRemoteHost> getRemoteHostsForRetrieve(String tenant, String entityId, AttrsQueryTerm attrsQuery,
			LanguageQueryTerm lang, Context context) {
		Iterator<List<RegistrationEntry>> regEntries = tenant2CId2RegEntries.row(tenant).values().iterator();

		Map<RemoteHost, QueryInfos> host2QueryInfo = Maps.newHashMap();
		List<QueryRemoteHost> result = Lists.newArrayList();

		while (regEntries.hasNext()) {
			Iterator<RegistrationEntry> it = regEntries.next().iterator();
			while (it.hasNext()) {
				RegistrationEntry regEntry = it.next();
				if (!regEntry.retrieveEntity()) {
					continue;
				}
				QueryInfos tmp = regEntry.matches(new String[] { entityId }, null, null, attrsQuery, null, null, null);
				if (tmp == null) {
					continue;
				}
				QueryInfos resultQueryInfo = host2QueryInfo.get(regEntry.host());
				if (resultQueryInfo == null) {
					resultQueryInfo = tmp;
					host2QueryInfo.put(regEntry.host(), tmp);
				} else {
					resultQueryInfo.merge(tmp);
				}

			}
		}
		for (Entry<RemoteHost, QueryInfos> entry : host2QueryInfo.entrySet()) {
			RemoteHost remoteHost = entry.getKey();
			result.add(QueryRemoteHost.fromRemoteHost(remoteHost,
					entry.getValue().toQueryString(context, null, null, lang, true, null, null),
					remoteHost.canDoEntityId(), remoteHost.canDoZip(), null));
		}
		return result;
	}

	public Uni<Void> handleRegistryChange(BaseRequest req) {
		List<RegistrationEntry> newRegs = Lists.newArrayList();
		tenant2CId2RegEntries.remove(req.getTenant(), req.getId());
		if (req.getRequestType() != AppConstants.DELETE_REQUEST) {
			for (RegistrationEntry regEntry : RegistrationEntry.fromRegPayload(req.getPayload())) {
				if (regEntry.retrieveEntity() || regEntry.queryEntity() || regEntry.queryBatch()) {
					newRegs.add(regEntry);
				}
			}
			tenant2CId2RegEntries.put(req.getTenant(), req.getId(), newRegs);
		}
		return Uni.createFrom().voidItem();
	}

	@SuppressWarnings("unchecked")
	/**
	 * 
	 * @param remoteHost
	 * @param headersFromReq
	 * @param context
	 * @return a Tuple of Object and RemoteHost to fit in the initial entitymap
	 *         creation algorithm return actually a List<Map<String, Object>> in the
	 *         Object
	 */
	private Uni<Tuple2<Object, QueryRemoteHost>> getFullQueryResult(QueryRemoteHost remoteHost,
			io.vertx.core.MultiMap headersFromReq, Context context) {
		String linkHead;
		if (!remoteHost.headers().contains("Link") && headersFromReq.contains("Link")) {
			linkHead = headersFromReq.get("Link");
		} else if (remoteHost.headers().contains("Link")) {
			linkHead = remoteHost.headers().get("Link");
		} else {
			linkHead = "";
		}
		return webClient
				.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + remoteHost.queryString()
						+ "&limit=1000")
				.putHeaders(remoteHost.headers()).putHeader("Link", linkHead).timeout(timeout).send().onItem()
				.transformToUni(response -> {

					if (response != null && response.statusCode() == 200) {
						List<?> tmpList = response.bodyAsJsonArray().getList();
						return ldService.expand(context, tmpList, opts, -1, false).onItem().transformToUni(expanded -> {

							if (response.headers().contains("Next")) {
								QueryRemoteHost updatedHost = remoteHost
										.updatedDuplicate(response.headers().get("Next"));
								return getFullQueryResult(updatedHost, headersFromReq, context).onItem()
										.transform(nextResult -> {
											Object nextEntities = nextResult.getItem1();
											if (nextEntities != null) {
												expanded.addAll((List<Object>) nextEntities);
											}

											return Tuple2.of((Object) expanded, remoteHost);
										});
							}
							return Uni.createFrom().item(Tuple2.of((Object) expanded, remoteHost));
						});
					} else {
						return Uni.createFrom().item(Tuple2.of(null, remoteHost));
					}

				}).onFailure().recoverWithUni(e -> {
					logger.warn("Failed to query remote host" + remoteHost.toString());

					return Uni.createFrom().item(Tuple2.of(null, remoteHost));
				});
	}

	public Uni<Tuple2<EntityCache, EntityMap>> getAndStoreEntityIdList(String tenant, String[] ids, String idPattern,
			String qToken, TypeQueryTerm typeQuery, AttrsQueryTerm attrsQuery, GeoQueryTerm geoQuery, QQueryTerm qQuery,
			ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, int limit, int offset, Context context,
			io.vertx.core.MultiMap headersFromReq, boolean doNotCompact, DataSetIdTerm dataSetIdTerm, String join,
			int joinLevel, boolean onlyFullEntitiesDistributed, PickTerm pickTerm, OmitTerm omitTerm) {
		// we got no registry entries
		Uni<Tuple2<EntityCache, EntityMap>> entityCacheAndEntityMap;
		if (tenant2CId2RegEntries.isEmpty()) {
			entityCacheAndEntityMap = queryDAO.queryForEntityIdsAndEntitiesRegEmpty(tenant, ids, typeQuery, idPattern,
					attrsQuery, qQuery, geoQuery, scopeQuery, context, limit, offset, dataSetIdTerm, join, joinLevel,
					qToken, pickTerm, omitTerm);
		} else {
			EntityCache fullEntityCache = new EntityCache();
			Map<QueryRemoteHost, Set<String>> remoteHost2Query = EntityTools.getRemoteQueries(tenant, ids, typeQuery,
					idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, langQuery, tenant2CId2RegEntries, context,
					fullEntityCache, onlyFullEntitiesDistributed);
			if (remoteHost2Query.isEmpty()) {
				if ((join == null || joinLevel <= 0) && (qQuery == null || !qQuery.hasLinkedQ())) {
					entityCacheAndEntityMap = queryDAO.queryForEntityIdsAndEntitiesRegEmpty(tenant, ids, typeQuery,
							idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, context, limit, offset, dataSetIdTerm,
							join, joinLevel, qToken, pickTerm, omitTerm).onItem().transform(t -> {
								EntityMap entityMap = t.getItem2();
								entityMap.setRegEmpty(false);
								entityMap.setOnlyFullEntitiesDistributed(onlyFullEntitiesDistributed);
								return t;
							});
				} else {
					entityCacheAndEntityMap = queryDAO.queryForEntityIdsAndEntitiesRegNotEmpty(tenant, ids, typeQuery,
							idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, limit, offset, qToken,
							onlyFullEntitiesDistributed);
				}
			} else {
				Uni<Tuple2<EntityCache, EntityMap>> localEntityCacheAndEntityMap;
				if (onlyFullEntitiesDistributed) {
					// We got registry entries but the query assumes that only full entities are
					// present in the broker
					localEntityCacheAndEntityMap = queryDAO.queryForEntityIdsAndEntitiesRegNotEmpty(tenant, ids,
							typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, limit, offset, qToken,
							onlyFullEntitiesDistributed);
				} else {
					// Can't assume anything! We got reg entries and the entities can be fully
					// distributed
					localEntityCacheAndEntityMap = queryDAO.queryForEntityIdsAndEntitiesRegNotEmptyExpectDistEntities(
							tenant, ids, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, join, joinLevel, qToken);
				}

				List<Uni<Tuple2<Object, QueryRemoteHost>>> unis = Lists.newArrayList();

				for (Entry<QueryRemoteHost, Set<String>> remoteHostEntry : remoteHost2Query.entrySet()) {
					QueryRemoteHost remoteHost = remoteHostEntry.getKey();
					String linkHead;
					if (!remoteHost.headers().contains("Link") && headersFromReq.contains("Link")) {
						linkHead = headersFromReq.get("Link");
					} else if (remoteHost.headers().contains("Link")) {
						linkHead = remoteHost.headers().get("Link");
					} else {
						linkHead = "";
					}
					if (remoteHost.canDoEntityMap()) {

						unis.add(webClient
								.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITY_MAP_ENDPOINT
										+ remoteHost.queryString())
								.putHeaders(remoteHost.headers()).putHeader("Link", linkHead).timeout(timeout).send()
								.onItem().transformToUni(response -> {

									Object result = null;
									if (response != null && response.statusCode() == 200) {
										result = response.bodyAsJsonObject().getMap();

									}
									logger.debug("from remote host: " + remoteHost.host()
											+ NGSIConstants.NGSI_LD_ENTITY_MAP_ENDPOINT + remoteHost.queryString());
									return Uni.createFrom().item(Tuple2.of(result, remoteHost));
								}).onFailure().recoverWithItem(e -> {
									logger.warn("Failed to query entity list from remote host" + remoteHost.toString());
									return null;
								}));
					} else {
						unis.add(getFullQueryResult(remoteHost, headersFromReq, context));
					}
				}
				Uni<Tuple2<EntityCache, EntityMap>> remoteResults = Uni.combine().all().unis(unis)
						.combinedWith(list -> {
							EntityCache result = new EntityCache();
							EntityMap entityMap = new EntityMap(null, onlyFullEntitiesDistributed, false, false);
							for (Object obj : list) {
								if (obj != null) {
									Tuple2<Object, QueryRemoteHost> t = (Tuple2<Object, QueryRemoteHost>) obj;
									Object resultObj = t.getItem1();
									if (resultObj != null) {
										QueryRemoteHost host = t.getItem2();
										if (host.canDoEntityMap()) {
											Map<String, Object> responseMap = (Map<String, Object>) resultObj;
											Map<String, Object> entityMapEntry = (Map<String, Object>) responseMap
													.get(NGSIConstants.NGSI_LD_ENTITY_MAP_SHORT);
											String id = (String) responseMap.get(NGSIConstants.ID);

											entityMapEntry.keySet()
													.forEach(entry -> entityMap.getEntry(entry).addRemoteHost(host));
											entityMap.addLinkedMap(host.cSourceId(), id);
										} else {
											List<Object> responseEntities = (List<Object>) resultObj;
											result.putEntitiesIntoEntityCacheAndEntityMap(responseEntities, host,
													entityMap);
										}
									}
								}
							}
							return Tuple2.of(result, entityMap);
						});
				entityCacheAndEntityMap = Uni.combine().all().unis(localEntityCacheAndEntityMap, remoteResults)
						.asTuple().onItem().transform(t -> {
							Tuple2<EntityCache, EntityMap> localT = t.getItem1();
							Tuple2<EntityCache, EntityMap> remoteT = t.getItem2();
							mergeLocalAndRemoteCacheAndMap(localT, remoteT);
							return localT;
						});
			}

		}
		return entityCacheAndEntityMap.onItem().transform(tuple -> {
			EntityMap entityMap = tuple.getItem2();
			if (!doNotCompact) {
				queryDAO.storeEntityMap(tenant, qToken, entityMap, false).subscribe().with(t -> {
					logger.debug("Stored entity map " + qToken);
				});
			}

			return tuple;
		});
	}

	private void mergeLocalAndRemoteCacheAndMap(Tuple2<EntityCache, EntityMap> localT,
			Tuple2<EntityCache, EntityMap> remoteT) {

		localT.getItem1().mergeCache(remoteT.getItem1());
		mergeLocalAndRemoteEntityMap(localT.getItem2(), remoteT.getItem2());
	}

	private void mergeLocalAndRemoteEntityMap(EntityMap localMap, EntityMap remoteMap) {
		localMap.setLinkedMaps(remoteMap.getLinkedMaps());
		for (EntityMapEntry entry : localMap.getEntityList()) {
			entry.getRemoteHosts().forEach(remoteHost -> {
				localMap.getEntry(entry.getEntityId()).addRemoteHost(remoteHost);
			});
		}

	}

	private Uni<Tuple2<QueryRemoteHost, Map<String, Map<String, Object>>>> handle414IdQuery(int baseLength,
			QueryRemoteHost remoteHost, String linkHead, boolean entityMap) {
		int start = remoteHost.queryString().indexOf("id=");
		if (start == -1) {
			logger.warn("failed to split up query");
			return Uni.createFrom().item(Tuple2.of(remoteHost, Maps.newHashMap()));
		}
		int end = remoteHost.queryString().indexOf("&", start);
		String idsString;
		if (end == -1) {
			idsString = remoteHost.queryString().substring(start + 3);
		} else {
			idsString = remoteHost.queryString().substring(start + 3, end);
		}
		// assuming 2000kb for this ... since ... internet explorer set this as a
		// minimum
		int maxLengthForIds = 2000 - (baseLength + (remoteHost.queryString().length() - (idsString.length() + 4)));
		if (maxLengthForIds <= idsString.indexOf(",")) {
			logger.warn("failed to split up query");
			return Uni.createFrom().item(Tuple2.of(remoteHost, Maps.newHashMap()));
		}
		int index;
		List<Uni<Tuple2<QueryRemoteHost, Map<String, Map<String, Object>>>>> backUpUnis = Lists.newArrayList();
		String baseQueryString;
		if (end == -1) {
			baseQueryString = remoteHost.queryString().substring(0, start + 1);
		} else {
			baseQueryString = remoteHost.queryString().substring(0, start + 1)
					+ remoteHost.queryString().substring(end);
		}
		if (baseQueryString.isEmpty()) {
			baseQueryString = "?";
		} else {
			baseQueryString += "&";
		}
		while (!idsString.isEmpty()) {

			index = idsString.lastIndexOf(",", maxLengthForIds);
			String toUseIds = idsString.substring(0, index);
			backUpUnis.add(webClient
					.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + baseQueryString + "id="
							+ toUseIds + "&limit=1000")
					.putHeaders(remoteHost.headers()).putHeader("Link", linkHead).timeout(timeout).send().onItem()
					.transformToUni(backupResponse -> {
						Map<String, Map<String, Object>> backupResult = Maps.newHashMap();
						if (backupResponse != null && backupResponse.statusCode() == 200) {
							List tmpList = backupResponse.bodyAsJsonArray().getList();
							for (Object obj : tmpList) {
								Map<String, Object> entity = (Map<String, Object>) obj;
								backupResult.put((String) entity.get(NGSIConstants.ID), entity);
							}
							logger.debug("retrieved entity list: " + backupResult.toString());
						}
						return Uni.createFrom().item(Tuple2.of(remoteHost, backupResult));
					}).onFailure().recoverWithItem(e1 -> null));
			idsString = idsString.substring(index + 1);
			if (idsString.length() <= maxLengthForIds) {
				backUpUnis.add(webClient
						.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + baseQueryString + "id="
								+ idsString + "&limit=1000")
						.putHeaders(remoteHost.headers()).putHeader("Link", linkHead).timeout(timeout).send().onItem()
						.transformToUni(backupResponse -> {
							Map<String, Map<String, Object>> backupResult = Maps.newHashMap();
							if (backupResponse != null && backupResponse.statusCode() == 200) {
								List tmpList = backupResponse.bodyAsJsonArray().getList();
								for (Object obj : tmpList) {
									Map<String, Object> entity = (Map<String, Object>) obj;
									backupResult.put((String) entity.get(NGSIConstants.ID), entity);
								}
								logger.debug("retrieved entity list: " + backupResult.toString());
							}
							return Uni.createFrom().item(Tuple2.of(remoteHost, backupResult));
						}).onFailure().recoverWithItem(e1 -> null));
				idsString = "";
			}
		}
		return Uni.combine().all().unis(backUpUnis).combinedWith(l1 -> {
			Map<String, Map<String, Object>> allIds = Maps.newHashMap();
			QueryRemoteHost qrh = null;
			for (Object obj1 : l1) {
				Tuple2<QueryRemoteHost, Map<String, Map<String, Object>>> t1 = (Tuple2<QueryRemoteHost, Map<String, Map<String, Object>>>) obj1;
				qrh = t1.getItem1();
				allIds.putAll(t1.getItem2());
			}
			return Tuple2.of(qrh, allIds);
		});

	}

	@Scheduled(every = "${scorpio.entitymap.cleanup.schedule}", delayed = "${scorpio.startupdelay}")
	public Uni<Void> scheduleEntityMapCleanUp() {
		return queryDAO.runEntityMapCleanup(entityMapTTL);
	}

	public Uni<Map<String, Object>> getEntityFlat(Context context, String tenant, String entityId,
			Map<String, Object> attrsMap, LanguageQueryTerm lang, boolean localOnly, String containedBy, int joinLevel,
			List<Map<String, Object>> relResult) {
		if (relResult == null) {
			relResult = new ArrayList<>();
		}
		List<Map<String, Object>> finalRelResult = relResult;
		AttrsQueryTerm attrsQuery = null;
		if (attrsMap != null && !attrsMap.isEmpty()) {
			try {
				attrsQuery = QueryParser.parseAttrs(String.join(",", attrsMap.keySet()), context);
			} catch (ResponseException e) {
				throw new RuntimeException(e);
			}
		}
		AtomicInteger joinLvl = new AtomicInteger(joinLevel);
		return retrieveEntity(context, tenant, entityId, attrsQuery, lang, localOnly).onItem().transformToUni(ent -> {
			if (ent.isEmpty()) {
				return Uni.createFrom().failure(
						new ResponseException(ErrorType.NotFound, "Entity with ID " + entityId + " was not found"));
			}
			if (containedBy.contains((String) ent.get(JsonLdConsts.ID)) || joinLvl.get() == 0) {
				return Uni.createFrom().item(Map.of(JsonLdConsts.GRAPH, finalRelResult));
			}
			joinLvl.set(joinLvl.decrementAndGet());
			finalRelResult.add(ent);
			List<Uni<Map<String, Object>>> unisOfMaps = new ArrayList<>();
			for (String key : ent.keySet()) {
				if (ent.get(key) instanceof List<?> attrib && attrib.get(0) instanceof Map<?, ?>
						&& ((Map<String, Object>) attrib.get(0)).containsKey(NGSIConstants.NGSI_LD_OBJECT_TYPE)
						&& (!containedBy.contains((String) ent.get(JsonLdConsts.ID)))) {
					Map<String, Object> finalAttrs = new HashMap<>();
					if (attrsMap != null && !attrsMap.isEmpty()) {
						String toRemove = attrsMap.keySet().iterator().next();
						finalAttrs = (Map<String, Object>) attrsMap.getOrDefault(toRemove, new HashMap<>());
						attrsMap.remove(toRemove);
					}
					if (((Map<String, Object>) attrib.get(0)).containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST)) {
						for (Map<String, Object> idsMap : ((Map<String, List<Map<String, List<Map<String, Object>>>>>) attrib
								.get(0)).get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST).get(0).get(JsonLdConsts.LIST)) {
							unisOfMaps.add(getEntityFlat(context, tenant, (String) idsMap.get(JsonLdConsts.VALUE),
									finalAttrs, lang, localOnly, containedBy + ent.get(JsonLdConsts.ID), joinLvl.get(),
									finalRelResult));
						}
					} else {
						for (Map<String, Object> idMap : ((Map<String, List<Map<String, Object>>>) attrib.get(0))
								.get(NGSIConstants.NGSI_LD_HAS_OBJECT)) {
							unisOfMaps.add(getEntityFlat(context, tenant, (String) idMap.get(JsonLdConsts.ID),
									finalAttrs, lang, localOnly, containedBy + ent.get(JsonLdConsts.ID), joinLvl.get(),
									finalRelResult));
						}
					}
				}
			}
			if (!unisOfMaps.isEmpty()) {
				return Uni.combine().all().unis(unisOfMaps).collectFailures().combinedWith(x -> x).onItemOrFailure()
						.transformToUni((list, fail) -> {
							return Uni.createFrom().item(Map.of(JsonLdConsts.GRAPH, finalRelResult));
						});
			}
			return Uni.createFrom().item(Map.of(JsonLdConsts.GRAPH, finalRelResult));
		});
	}

	public Uni<Map<String, Object>> getEntityInline(Context context, String tenant, String entityId,
			Map<String, Object> attrsMap, LanguageQueryTerm lang, boolean localOnly, String containedBy,
			int joinLevel) {
		AttrsQueryTerm attrsQuery = null;
		if (attrsMap != null && !attrsMap.isEmpty()) {
			try {
				attrsQuery = QueryParser.parseAttrs(String.join(",", attrsMap.keySet()), context);
			} catch (ResponseException e) {
				throw new RuntimeException(e);
			}
		}
		AtomicInteger joinLvl = new AtomicInteger(joinLevel);
		return retrieveEntity(context, tenant, entityId, attrsQuery, lang, localOnly).onItem().transformToUni(ent -> {
			if (ent.isEmpty()) {
				return Uni.createFrom().failure(
						new ResponseException(ErrorType.NotFound, "Entity with ID " + entityId + " was not found"));
			}
			if (containedBy.contains((String) ent.get(JsonLdConsts.ID)) || joinLvl.get() == 0) {
				return Uni.createFrom().failure(new Throwable());
			}
			joinLvl.set(joinLvl.decrementAndGet());
			List<Uni<Map<String, Object>>> unisOfMaps = new ArrayList<>();
			for (String key : ent.keySet()) {
				if (ent.get(key) instanceof List<?> attrib && attrib.get(0) instanceof Map<?, ?>
						&& ((Map<String, Object>) attrib.get(0)).containsKey(NGSIConstants.NGSI_LD_OBJECT_TYPE)
						&& (!containedBy.contains((String) ent.get(JsonLdConsts.ID)))) {
					Map<String, Object> finalAttrsMap = new HashMap<>();
					if (attrsMap != null && !attrsMap.isEmpty()) {
						String toRemove = attrsMap.keySet().iterator().next();
						finalAttrsMap = (Map<String, Object>) attrsMap.getOrDefault(toRemove, new HashMap<>());
						attrsMap.remove(toRemove);
					}
					List<Object> flatResult = new ArrayList<>();
					if (((Map<String, Object>) attrib.get(0)).containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST)) {
						for (Map<String, Object> idsMap : ((Map<String, List<Map<String, List<Map<String, Object>>>>>) attrib
								.get(0)).get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST).get(0).get(JsonLdConsts.LIST)) {
							unisOfMaps.add(getEntityInline(context, tenant, (String) idsMap.get(JsonLdConsts.VALUE),
									finalAttrsMap, lang, localOnly, containedBy + ent.get(JsonLdConsts.ID),
									joinLvl.get()).onItem().transform(map -> {
										flatResult.add(map);
										((List<Map<String, Object>>) ent.get(key)).get(0).put("entity", flatResult);
										return ent;
									}));
						}

					} else {
						for (Map<String, Object> idMap : ((Map<String, List<Map<String, Object>>>) attrib.get(0))
								.get(NGSIConstants.NGSI_LD_HAS_OBJECT)) {
							unisOfMaps.add(
									getEntityInline(context, tenant, (String) idMap.get(JsonLdConsts.ID), finalAttrsMap,
											lang, localOnly, containedBy + ent.get(JsonLdConsts.ID), joinLvl.get())
											.onItem().transform(map -> {
												flatResult.add(map);
												((List<Map<String, Object>>) ent.get(key)).get(0).put("entity",
														flatResult);
												return ent;
											}));
						}

					}
				}
			}
			if (!unisOfMaps.isEmpty()) {
				return Uni.combine().all().unis(unisOfMaps).collectFailures().combinedWith(x -> x).onItemOrFailure()
						.transformToUni((list, fail) -> {
							return Uni.createFrom().item(ent);
						});
			}
			return Uni.createFrom().item(ent);
		});
	}

	public Uni<Map<String, Object>> idsOnly(Uni<Map<String, Object>> uniMap) {
		return uniMap.onItem().transform(map -> {
			Map<String, Object> result = new HashMap<>();
			List<Map<String, Object>> list = new ArrayList<>();
			result.put(JsonLdConsts.GRAPH, list);
			List<Map<String, Object>> mapList = (List<Map<String, Object>>) map.get(JsonLdConsts.GRAPH);
			for (Map<String, Object> item : mapList) {
				list.add(Map.of(JsonLdConsts.ID, item.get(JsonLdConsts.ID)));
			}
			return result;
		});
	}

	@Override
	public Uni<Tuple2<EntityCache, List<Map<String, Object>>>> getEntitiesFromUncalledHosts(String tenant,
			Map<Set<String>, Set<String>> types2EntityIds, EntityCache fullEntityCache, Context linkHeaders,
			QQueryTerm linkedQ, boolean expectFullEntities) {
		TypeQueryTerm typeQueryTerm = new TypeQueryTerm(linkHeaders);
		TypeQueryTerm currentTypeQuery = typeQueryTerm;
		Map<Set<String>, Set<String>> types2EntityIdsForDB = Maps.newHashMap();
		// List<Uni<>>
		List<Uni<Tuple3<List<Map<String, Object>>, QueryRemoteHost, Map<Set<String>, Set<String>>>>> unis = Lists
				.newArrayList();
		for (Entry<Set<String>, Set<String>> types2EntityIdsEntry : types2EntityIds.entrySet()) {
			String[] entityIds = types2EntityIdsEntry.getValue().toArray(new String[0]);

			Set<String> types = types2EntityIdsEntry.getKey();
			Set<String> idsForDB = Sets.newHashSet();
			typeQueryTerm.setAllTypes(types);

			for (String type : types) {
				currentTypeQuery.setType(type);
				currentTypeQuery.setNextAnd(false);
				TypeQueryTerm next = new TypeQueryTerm(linkHeaders);
				currentTypeQuery.setNext(next);
				currentTypeQuery = next;
				Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> cacheIds = fullEntityCache
						.getByType(type);
				for (String id : entityIds) {
					if (cacheIds.containsKey(id)) {
						Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> entityAndHosts = cacheIds.get(id);
						Map<String, QueryRemoteHost> hostName2Host = entityAndHosts.getItem2();
						if (hostName2Host == null) {
							idsForDB.add(id);
						} else {
							if (!hostName2Host.containsKey(AppConstants.INTERNAL_NULL_KEY)) {
								idsForDB.add(id);
							}
						}
					} else {
						idsForDB.add(id);
					}
				}

			}
			if (!idsForDB.isEmpty()) {
				types2EntityIdsForDB.put(types, idsForDB);
			}
			Map<QueryRemoteHost, Set<String>> remoteQueries = EntityTools.getRemoteQueries(entityIds, typeQueryTerm,
					null, null, linkedQ, null, null, null, tenant2CId2RegEntries.row(tenant).values().iterator(),
					linkHeaders, fullEntityCache);
			for (Entry<QueryRemoteHost, Set<String>> remoteHostEntry : remoteQueries.entrySet()) {
				QueryRemoteHost remoteQuery = remoteHostEntry.getKey();
				unis.add(EntityTools.getRemoteEntities(remoteQuery, webClient).onItem().transform(l -> Tuple3.of(l,
						remoteQuery, Map.of(typeQueryTerm.getAllTypes(), remoteHostEntry.getValue()))));
			}

		}
		unis.add(0, queryDAO.getEntities(tenant, types2EntityIdsForDB, linkedQ).onItem()
				.transform(entities -> Tuple3.of(entities, AppConstants.DB_REMOTE_HOST, types2EntityIdsForDB)));
		return Uni.combine().all().unis(unis).combinedWith(l -> {
			List<Map<String, Object>> result = Lists.newArrayList();
			for (Object obj : l) {
				Tuple3<List<Map<String, Object>>, QueryRemoteHost, Map<Set<String>, Set<String>>> entry = (Tuple3<List<Map<String, Object>>, QueryRemoteHost, Map<Set<String>, Set<String>>>) obj;
				List<Map<String, Object>> entities = entry.getItem1();
				result.addAll(entities);
				QueryRemoteHost host = entry.getItem2();
				Map<Set<String>, Set<String>> types2Ids = entry.getItem3();
				fullEntityCache.putEntitiesIntoEntityCacheAndEntityMap(entities, host, null);
			}
			return Tuple2.of(fullEntityCache, result);
		});
	}

}
