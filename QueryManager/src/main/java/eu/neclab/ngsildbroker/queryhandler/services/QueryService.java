package eu.neclab.ngsildbroker.queryhandler.services;

import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.jsonldjava.core.JsonLdConsts;
import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.apache.http.util.EntityUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
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
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.QueryServiceInterface;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;
import eu.neclab.ngsildbroker.queryhandler.repository.entityMap;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.UniAndGroup;
import io.smallrye.mutiny.groups.UniAndGroup2;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;
import static eu.neclab.ngsildbroker.commons.tools.HttpUtils.parseLinkHeaderNoUni;

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
			boolean entityDist) {
		if (localOnly) {
			return localQueryLevel1(tenant, ids, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery,
					langQuery, limit, offSet, count, dataSetIdTerm, join, joinLevel);
		}
		if (!tokenProvided) {
			return getAndStoreEntityIdList(tenant, ids, idPattern, qToken, typeQuery, attrsQuery, geoQuery, qQuery,
					scopeQuery, langQuery, limit, offSet, context, headersFromReq, doNotCompact, dataSetIdTerm, join,
					joinLevel, entityDist).onItem().transformToUni(t -> {

						return handleEntityMap(t.getItem2(), t.getItem1(), tenant, ids, typeQuery, idPattern,
								attrsQuery, qQuery, geoQuery, scopeQuery, langQuery, limit, offSet, count,
								dataSetIdTerm, join, joinLevel);
					});
		} else {
			return getEntityMapAndEntitiesAndUpdateExpires(tenant, ids, typeQuery, idPattern, attrsQuery, qQuery,
					geoQuery, scopeQuery, context, limit, offSet, dataSetIdTerm, join, joinLevel, qToken).onItem()
					.transformToUni(t -> {
						return handleEntityMap(t.getItem2(), t.getItem1(), tenant, ids, typeQuery, idPattern,
								attrsQuery, qQuery, geoQuery, scopeQuery, langQuery, limit, offSet, count,
								dataSetIdTerm, join, joinLevel);

					});
		}
	}

	private Uni<Tuple2<Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>>, EntityMap>> getEntityMapAndEntitiesAndUpdateExpires(
			String tenant, String[] ids, TypeQueryTerm typeQuery, String idPattern, AttrsQueryTerm attrsQuery,
			QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, Context context, int limit, int offSet,
			DataSetIdTerm dataSetIdTerm, String join, int joinLevel, String qToken) {
		// TODO Auto-generated method stub
		return null;
	}

	private Uni<QueryResult> handleEntityMap(EntityMap entityMap,
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> entityCache,
			String tenant, String[] id, TypeQueryTerm typeQuery, String idPattern, AttrsQueryTerm attrsQuery,
			QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, int limit,
			int offSet, boolean count, DataSetIdTerm dataSetIdTerm, String join, int joinLevel) {
		QueryResult result = new QueryResult();

		List<Map<String, Object>> resultData = Lists.newArrayList();
		result.setData(resultData);
		List<EntityMapEntry> subMap = entityMap.getSubMap(offSet, offSet + limit);
		Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2EntityAndHost = entityCache.get("*");
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

		} else if (entityMap.isNoRootLevelRegEntry()) {
			// no reg entry for the entities but reg entries exist but no join requrired so
			// just push
			if ((join == null || joinLevel <= 0) && !qQuery.hasLinkedQ()) {
				resultData.addAll(localResults.values());
			} else {
				Map<String, Map<String, Object>> fullEntityCache = Maps.newHashMap(localResults);
				if (linkedEntities != null) {
					fullEntityCache.putAll(linkedEntities);
				}
				if (qQuery.hasLinkedQ()) {
					qQuery.calculate(localResults, fullEntityCache, tenant2CId2RegEntries.row(tenant));
				}
			}
		} else if (entityMap.onlyFullEntitiesDistributed()) {

		} else {

		}

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
		return Uni.createFrom().item(result);
	}

	private void inlineEntity(Map<String, Object> entity,
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> entityCache,
			int currentJoinLevel, int joinLevel, boolean localOnly) {
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

	private void inlineAttrib(Object attribObj,
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> entityCache,
			int currentJoinLevel, int joinLevel, boolean localOnly) {
		if (attribObj instanceof Map attribMap) {
			Object typeObj = attribMap.get(NGSIConstants.JSON_LD_TYPE);
			if (typeObj != null && typeObj instanceof List typeList) {
				if (typeList.contains(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
					List<Map<String, String>> hasObject = (List<Map<String, String>>) attribMap
							.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
					List<Map<String, Object>> entities = new ArrayList<>(hasObject.size());

					if (localOnly) {
						Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2EntityAndHost = entityCache
								.get("*");
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
										.get(linkedType);
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
								.get("*");
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
										.get(linkedType);
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

	private Uni<QueryResult> handleEntityMap(Long resultCount, List<EntityMapEntry> resultEntityMap,
			AttrsQueryTerm attrsQuery, Map<String, Map<String, Object>> localResults, boolean count, int limit,
			int offSet, String qToken, io.vertx.core.MultiMap headersFromReq, QQueryTerm queryTerm,
			Set<String> jsonKeys, boolean isDist) {
		Map<QueryRemoteHost, List<String>> remoteHost2EntityIds = Maps.newHashMap();
		// has to be linked. We want to keep order here
		Map<String, Map<String, Map<String, Map<String, Object>>>> entityId2AttrName2DatasetId2AttrValue = Maps
				.newLinkedHashMap();
		for (EntityMapEntry entry : resultEntityMap) {
			List<QueryRemoteHost> remoteHosts = entry.getRemoteHosts();
			for (QueryRemoteHost remoteHost : remoteHosts) {
				List<String> tmp = remoteHost2EntityIds.computeIfAbsent(remoteHost, k -> Lists.newArrayList());
				logger.debug("adding entityid: " + entry.getEntityId() + " for remote host " + remoteHost.host());
				tmp.add(URLEncoder.encode(entry.getEntityId(), StandardCharsets.UTF_8));
			}

			entityId2AttrName2DatasetId2AttrValue.put(entry.getEntityId(), new HashMap<>(0));
		}
		List<Uni<Map<String, Map<String, Object>>>> unis = Lists.newArrayList();
		for (Entry<QueryRemoteHost, List<String>> entry : remoteHost2EntityIds.entrySet()) {
			QueryRemoteHost remoteHost = entry.getKey();
			if (remoteHost.isLocal()) {
				// Already here now
				// unis.add(queryDAO.getEntities(conn, entry.getValue(), attrsQuery));
			} else {
				List<Object> contextLinks;
				String linkHead;
				if (!remoteHost.headers().contains("Link")) {
					linkHead = headersFromReq.get("Link");
					contextLinks = parseLinkHeaderNoUni(headersFromReq.getAll("Link"),
							NGSIConstants.HEADER_REL_LDCONTEXT);
				} else {
					linkHead = remoteHost.headers().get("Link");
					contextLinks = parseLinkHeaderNoUni(remoteHost.headers().getAll("Link"),
							NGSIConstants.HEADER_REL_LDCONTEXT);
				}

				String idList = String.join(",", entry.getValue());
				logger.debug("calling: " + remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "?id=" + idList
						+ "&options=sysAttrs&limit=1000");

				unis.add(webClient
						.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "?id=" + idList
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
							logger.warn("failed to query remote host.");
							logger.warn("Attempting split");
							// request to long split it up
							if (response != null && response.statusCode() == 414) {
								int baseLength = (remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "?id="
										+ "&options=sysAttrs&limit=1000").length();
								return handle414(baseLength, remoteHost, linkHead, String.join(",", entry.getValue()),
										contextLinks);
							}
							if (response != null) {
								logger.debug("response code: " + response.statusCode());
								logger.debug("response : " + response.bodyAsString());
							} else {
								logger.debug("null response");
							}

							return Uni.createFrom().item(new HashMap<String, Map<String, Object>>(0));
						}).onFailure().recoverWithUni(e -> {
							logger.debug("failed to query with error " + e.getMessage());
							return Uni.createFrom().item(new HashMap<String, Map<String, Object>>(0));
						}));
			}
		}
		if (!localResults.isEmpty()) {
			unis.add(Uni.createFrom().item(localResults));
		}

		if (unis.isEmpty()) {
			QueryResult q = new QueryResult();
			if (count) {
				q.setCount(resultCount);
			}
			q.setData(new ArrayList<Map<String, Object>>());
			return Uni.createFrom().item(q);
		}

		return Uni.combine().all().unis(unis).combinedWith(list -> {
			QueryResult result = new QueryResult();
			Map<String, Set<String>> entityId2Types = Maps.newHashMap();
			Map<String, Set<String>> entityId2Scopes = Maps.newHashMap();
			Map<String, Long> entityId2YoungestModified = Maps.newHashMap();
			Map<String, Long> entityId2OldestCreatedAt = Maps.newHashMap();
			Map<String, Map<String, Integer>> entityId2AttrDatasetId2CurrentRegMode = Maps.newHashMap();
			for (Object obj : list) {
				Map<String, Map<String, Object>> entityId2Entity = (Map<String, Map<String, Object>>) obj;
				for (Entry<String, Map<String, Object>> entry : entityId2Entity.entrySet()) {
					mergeEntity(entry.getKey(), entry.getValue(), entityId2AttrName2DatasetId2AttrValue, entityId2Types,
							entityId2Scopes, entityId2YoungestModified, entityId2OldestCreatedAt,
							entityId2AttrDatasetId2CurrentRegMode);
				}
			}
			List<Map<String, Object>> resultData = Lists.newArrayList();
			for (Entry<String, Map<String, Map<String, Map<String, Object>>>> entry : entityId2AttrName2DatasetId2AttrValue
					.entrySet()) {
				String entityId = entry.getKey();
				Map<String, Map<String, Map<String, Object>>> attribMap = entry.getValue();
				Map<String, Object> entity = new HashMap<>(attribMap.size() + 5);
				entity.put(NGSIConstants.JSON_LD_ID, entityId);
				entity.put(NGSIConstants.JSON_LD_TYPE, Lists.newArrayList(entityId2Types.get(entityId)));
				if (entityId2Scopes.containsKey(entityId)) {
					Set<String> scopesSet = entityId2Scopes.get(entityId);
					if (!scopesSet.isEmpty()) {
						List<Map<String, String>> scopes = Lists.newArrayList();
						for (String scope : scopesSet) {
							scopes.add(Map.of(NGSIConstants.JSON_LD_VALUE, scope));
						}
						entity.put(NGSIConstants.NGSI_LD_SCOPE, scopes);
					}
				}
				if (entityId2OldestCreatedAt.get(entityId) != null) {
					entity.put(NGSIConstants.NGSI_LD_CREATED_AT,
							List.of(Map.of(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME,
									NGSIConstants.JSON_LD_VALUE,
									SerializationTools.toDateTimeString(entityId2OldestCreatedAt.get(entityId)))));
				}
				if (entityId2YoungestModified.get(entityId) != null) {
					entity.put(NGSIConstants.NGSI_LD_MODIFIED_AT,
							List.of(Map.of(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME,
									NGSIConstants.JSON_LD_VALUE,
									SerializationTools.toDateTimeString(entityId2YoungestModified.get(entityId)))));
				}
				for (Entry<String, Map<String, Map<String, Object>>> attribEntry : attribMap.entrySet()) {
					entity.put(attribEntry.getKey(), Lists.newArrayList(attribEntry.getValue().values()));
				}
				if (queryTerm != null) {
					try {
						if (queryTerm.calculate(EntityTools.getBaseProperties(entity))
								|| queryTerm.calculate(entity, jsonKeys)) {
							resultData.add(entity);
						}
					} catch (Exception e) {
						// Handling any exceptions occurred during calculation
						if (queryTerm.calculate(entity, jsonKeys)) {
							resultData.add(entity);
						}
					}
				} else {
					resultData.add(entity);
				}

			}
			result.setData(resultData);
			result.setLimit(limit);
			result.setOffset(offSet);
			if (count) {
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
			result.setqToken(qToken);
			return result;
		});

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
			int joinLevel) {
		return queryDAO.queryLocalOnly(tenant, id, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery,
				langQuery, limit, offSet, count, dataSetIdTerm, join, joinLevel).onItem().transform(rows -> {
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
		local = queryDAO.getEntity(entityId, tenant, attrsQuery);
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
					entry.getValue().toQueryString(context, null, null, lang, true), remoteHost.canDoEntityId(),
					remoteHost.canDoZip(), null));
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

	private Uni<Tuple2<Object, QueryRemoteHost>> getFullQueryResult(QueryRemoteHost remoteHost,
			io.vertx.core.MultiMap headersFromReq, boolean onlyFullEntitiesDistributed, Context context) {
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
						List tmpList = response.bodyAsJsonArray().getList();
						return ldService.expand(context, tmpList, opts, -1, false).onItem().transformToUni(expanded -> {

							if (response.headers().contains("Next")) {
								QueryRemoteHost updatedHost = remoteHost
										.updatedDuplicate(response.headers().get("Next"));
								return getFullQueryResult(updatedHost, headersFromReq, onlyFullEntitiesDistributed,
										context).onItem().transform(nextResult -> {
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

	private void mergeEntityMap(EntityMap entityMap, EntityMap nextEntityMap) {
		// TODO Auto-generated method stub

	}

	private void mergeEntityCache(
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> result,
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> nextEntityCache) {
		// TODO Auto-generated method stub

	}

	public Uni<Tuple2<Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>>, EntityMap>> getAndStoreEntityIdList(
			String tenant, String[] ids, String idPattern, String qToken, TypeQueryTerm typeQuery,
			AttrsQueryTerm attrsQuery, GeoQueryTerm geoQuery, QQueryTerm qQuery, ScopeQueryTerm scopeQuery,
			LanguageQueryTerm langQuery, int limit, int offset, Context context, io.vertx.core.MultiMap headersFromReq,
			boolean doNotCompact, DataSetIdTerm dataSetIdTerm, String join, int joinLevel,
			boolean onlyFullEntitiesDistributed) {
		// we got no registry entries
		Uni<Tuple2<Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>>, EntityMap>> entityCacheAndEntityMap;
		if (tenant2CId2RegEntries.isEmpty()) {
			entityCacheAndEntityMap = queryDAO.queryForEntityIdsAndEntitiesRegEmpty(tenant, ids, typeQuery, idPattern,
					attrsQuery, qQuery, geoQuery, scopeQuery, context, limit, offset, dataSetIdTerm, join, joinLevel,
					qToken);
		} else {
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> fullEntityCache = Maps
					.newHashMap();
			Map<QueryRemoteHost, Set<String>> remoteHost2Query = EntityTools.getRemoteQueries(tenant, ids, typeQuery,
					idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, langQuery, tenant2CId2RegEntries, context,
					fullEntityCache, onlyFullEntitiesDistributed);
			if (remoteHost2Query.isEmpty()) {
				if ((join == null || joinLevel <= 0) && (qQuery == null || !qQuery.hasLinkedQ())) {
					entityCacheAndEntityMap = queryDAO.queryForEntityIdsAndEntitiesRegEmpty(tenant, ids, typeQuery,
							idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, context, limit, offset, dataSetIdTerm,
							join, joinLevel, qToken).onItem().transform(t -> {
								EntityMap entityMap = t.getItem2();
								entityMap.setRegEmpty(false);
								entityMap.setOnlyFullEntitiesDistributed(onlyFullEntitiesDistributed);
								return t;
							});
				} else {
					entityCacheAndEntityMap = queryDAO.queryForEntityIdsAndEntitiesRegNotEmpty(tenant, ids, typeQuery,
							idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, context, limit, offset, qToken,
							onlyFullEntitiesDistributed);
				}
			} else {
				Uni<Tuple2<Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>>, EntityMap>> localEntityCacheAndEntityMap;
				if (onlyFullEntitiesDistributed) {
					// We got registry entries but the query assumes that only full entities are
					// present in the broker
					localEntityCacheAndEntityMap = queryDAO.queryForEntityIdsAndEntitiesRegNotEmpty(tenant, ids,
							typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, context, limit, offset,
							qToken, onlyFullEntitiesDistributed);
				} else {
					// Can't assume anything! We got reg entries and the entities can be fully
					// distributed
					localEntityCacheAndEntityMap = queryDAO.queryForEntityIdsAndEntitiesRegNotEmptyExpectDistEntities(
							tenant, ids, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, context, qToken);
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
						unis.add(getFullQueryResult(remoteHost, headersFromReq, onlyFullEntitiesDistributed, context));
					}
				}
				Uni<Tuple2<Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>>, EntityMap>> remoteResults = Uni
						.combine().all().unis(unis).combinedWith(list -> {
							Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> result = Maps
									.newHashMap();
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
											putEntitiesIntoEntityCacheAndEntityMap(responseEntities, result, host,
													entityMap);
										}
									}
								}
							}
							return Tuple2.of(result, entityMap);
						});
				entityCacheAndEntityMap = Uni.combine().all().unis(localEntityCacheAndEntityMap, remoteResults)
						.asTuple().onItem().transform(t -> {
							Tuple2<Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>>, EntityMap> localT = t
									.getItem1();
							Tuple2<Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>>, EntityMap> remoteT = t
									.getItem2();
							mergeLocalAndRemoteCacheAndMap(localT, remoteT);
							return localT;
						});
			}

		}
		return entityCacheAndEntityMap.onItem().transform(tuple -> {
			EntityMap entityMap = tuple.getItem2();
			if (!doNotCompact) {
				queryDAO.storeEntityMap(tenant, qToken, entityMap).subscribe().with(t -> {
					logger.debug("Stored entity map " + qToken);
				});
			}

			return tuple;
		});
	}

	private void mergeLocalAndRemoteCacheAndMap(
			Tuple2<Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>>, EntityMap> localT,
			Tuple2<Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>>, EntityMap> remoteT) {

		mergeLocalAndRemoteCache(localT.getItem1(), remoteT.getItem1());
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

	private void mergeLocalAndRemoteCache(
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> localCache,
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> remoteCache) {
		for (Entry<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> remoteEntry : remoteCache
				.entrySet()) {
			String rType = remoteEntry.getKey();
			Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> rId2EntityAndHosts = remoteEntry
					.getValue();
			Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> lId2EntityAndHost = localCache
					.get(rType);
			if (lId2EntityAndHost == null) {
				lId2EntityAndHost = Maps.newHashMap();
				localCache.put(rType, lId2EntityAndHost);
			}
			for (Entry<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> rId2EntityAndHostEntry : rId2EntityAndHosts
					.entrySet()) {
				String rId = rId2EntityAndHostEntry.getKey();
				Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> lEntityAndHost = lId2EntityAndHost.get(rId);
				Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> rEntityAndHost = rId2EntityAndHostEntry
						.getValue();
				Map<String, Object> rEntity = rEntityAndHost.getItem1();
				Map<String, QueryRemoteHost> rRemoteHosts = rEntityAndHost.getItem2();
				if (lEntityAndHost == null) {
					lEntityAndHost = Tuple2.of(rEntity, rRemoteHosts);
					lId2EntityAndHost.put(rId, lEntityAndHost);
				} else {
					Map<String, Object> lEntity = lEntityAndHost.getItem1();
					Map<String, QueryRemoteHost> lRemoteHosts = lEntityAndHost.getItem2();
					lRemoteHosts.putAll(rRemoteHosts);
					if (lEntity == null) {
						lEntityAndHost = Tuple2.of(rEntity, lRemoteHosts);
						lId2EntityAndHost.put(rId, lEntityAndHost);
					} else {
						mergeEntity(lEntity, rEntity);
					}
				}
			}
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
	public Uni<Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>>> getEntitiesFromUncalledHosts(
			String tenant, Map<Set<String>, Set<String>> types2EntityIds,
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> fullEntityCache,
			Context linkHeaders, QQueryTerm linkedQ, boolean expectFullEntities) {
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
						.get(type);
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
			for (Object obj : l) {
				Tuple3<List<Map<String, Object>>, QueryRemoteHost, Map<Set<String>, Set<String>>> entry = (Tuple3<List<Map<String, Object>>, QueryRemoteHost, Map<Set<String>, Set<String>>>) obj;
				List<Map<String, Object>> entities = entry.getItem1();
				QueryRemoteHost host = entry.getItem2();
				Map<Set<String>, Set<String>> types2Ids = entry.getItem3();
				putEntitiesIntoEntityCacheAndEntityMap(entities, fullEntityCache, host, null);
			}
			return fullEntityCache;
		});
	}

	private void putEntitiesIntoEntityCacheAndEntityMap(List entities,
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> fullEntityCache,
			QueryRemoteHost host, EntityMap entityMap) {
		for (Object entityObj : entities) {
			Map<String, Object> entity = (Map<String, Object>) entityObj;
			List<String> types = (List<String>) entity.get(NGSIConstants.JSON_LD_TYPE);
			String id = (String) entity.get(NGSIConstants.JSON_LD_ID);
			if (entityMap != null) {
				entityMap.getEntry(id).addRemoteHost(host);
			}
			for (String type : types) {

				Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>> ids2Entity2RemoteHost = fullEntityCache
						.get(type);
				if (ids2Entity2RemoteHost == null) {
					ids2Entity2RemoteHost = Maps.newHashMap();
					fullEntityCache.put(type, ids2Entity2RemoteHost);
				}
				Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>> entityAndRemoteHost = ids2Entity2RemoteHost
						.get(id);
				if (entityAndRemoteHost == null) {
					Map<String, QueryRemoteHost> hostName2RemoteHost = Maps.newHashMap();
					hostName2RemoteHost.put(host.host(), host);
					entityAndRemoteHost = Tuple2.of(entity, hostName2RemoteHost);
				} else {
					Map<String, QueryRemoteHost> hostName2RemoteHost = entityAndRemoteHost.getItem2();
					hostName2RemoteHost.put(host.host(), host);
					Map<String, Object> currentEntity = entityAndRemoteHost.getItem1();
					if (currentEntity == null) {
						entityAndRemoteHost = Tuple2.of(entity, hostName2RemoteHost);
					} else {
						entityAndRemoteHost = Tuple2.of(mergeEntity(currentEntity, entity), hostName2RemoteHost);
					}
				}
				ids2Entity2RemoteHost.put(id, entityAndRemoteHost);

			}
		}
	}

	private Map<String, Object> mergeEntity(Map<String, Object> currentEntity, Map<String, Object> entity) {
		// TODO Auto-generated method stub
		return null;
	}

}
