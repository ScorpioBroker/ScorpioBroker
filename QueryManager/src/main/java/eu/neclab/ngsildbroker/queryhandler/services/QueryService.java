package eu.neclab.ngsildbroker.queryhandler.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
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
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityCache;
import eu.neclab.ngsildbroker.commons.datatypes.EntityMap;
import eu.neclab.ngsildbroker.commons.datatypes.QueryRemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.Query;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.CSourceHandler;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
@SuppressWarnings("unchecked")
public class QueryService implements CSourceHandler {

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
	int timeout;
	@Inject
	MicroServiceUtils microServiceUtils;

	@PostConstruct
	void setup() {
		webClient = WebClient.create(vertx);
		queryDAO.getAllRegistries().onItem().transform(t -> {
			tenant2CId2RegEntries = t;
			return null;
		}).await().indefinitely();
		this.microServiceUtils.registerCSourceReceiver(this);
	}

	// This is needed so that @postconstruct runs on the startup thread and not on a
	// worker thread later on
	void startup(@Observes StartupEvent event) {
	}

	public Uni<QueryResult> query(String tenant, String qToken, boolean tokenProvided,
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPattern, AttrsQueryTerm attrsQuery,
			QQueryTerm qQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery,
			LanguageQueryTerm langQuery, int limit, int offSet, boolean count, boolean localOnly, Context context,
			io.vertx.core.MultiMap headersFromReq, boolean doNotCompact, Set<String> jsonKeys,
			DataSetIdTerm dataSetIdTerm, String join, int joinLevel, boolean entityDist, PickTerm pickTerm,
			OmitTerm omitTerm, String checkSum, ViaHeaders viaHeaders, String typePattern) {
		if (!tokenProvided) {
			return getAndStoreEntityMap(tenant, qToken, idsAndTypeQueryAndIdPattern, attrsQuery, geoQuery, qQuery,
					scopeQuery, langQuery, limit, offSet, context, headersFromReq, doNotCompact, dataSetIdTerm, join,
					joinLevel, entityDist, pickTerm, omitTerm, checkSum, viaHeaders, typePattern, localOnly).onItem().transformToUni(t -> {
						return handleEntityMap(t.getItem2(), t.getItem1(), tenant, idsAndTypeQueryAndIdPattern,
								attrsQuery, qQuery, geoQuery, scopeQuery, langQuery, limit, offSet, count,
								dataSetIdTerm, join, joinLevel, context, jsonKeys, headersFromReq, pickTerm, omitTerm,
								viaHeaders);

					});
		} else {
			return getEntityMapAndEntitiesAndUpdateExpires(tenant, idsAndTypeQueryAndIdPattern, limit, offSet, qToken,
					checkSum).onItem().transformToUni(t -> {
						EntityMap entityMap = t.getItem2();
						if (entityMap == null) {
							if (idsAndTypeQueryAndIdPattern == null && attrsQuery == null && geoQuery == null
									&& qQuery == null && scopeQuery == null && langQuery == null && pickTerm == null
									&& omitTerm == null) {
								return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
										"Unknow entitymap and no query details provided please requery with the original query params"));
							}
							return getAndStoreEntityMap(tenant, qToken, idsAndTypeQueryAndIdPattern, attrsQuery,
									geoQuery, qQuery, scopeQuery, langQuery, limit, offSet, context, headersFromReq,
									doNotCompact, dataSetIdTerm, join, joinLevel, entityDist, pickTerm, omitTerm,
									checkSum, viaHeaders, typePattern, localOnly).onItem().transformToUni(t2 -> {
										return handleEntityMap(t2.getItem2(), t2.getItem1(), tenant,
												idsAndTypeQueryAndIdPattern, attrsQuery, qQuery, geoQuery, scopeQuery,
												langQuery, limit, offSet, count, dataSetIdTerm, join, joinLevel,
												context, jsonKeys, headersFromReq, pickTerm, omitTerm, viaHeaders);

									});
						}

						List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPatternTBU = idsAndTypeQueryAndIdPattern;
						AttrsQueryTerm attrsQueryTBU = attrsQuery;
						QQueryTerm qQueryTBU = qQuery;
						GeoQueryTerm geoQueryTBU = geoQuery;
						ScopeQueryTerm scopeQueryTBU = scopeQuery;
						LanguageQueryTerm langQueryTBU = langQuery;
						Set<String> jsonKeysTBU = jsonKeys;
						DataSetIdTerm dataSetIdTermTBU = dataSetIdTerm;
						String joinTBU = join;
						int joinLevelTBU = joinLevel;
						PickTerm pickTermTBU = pickTerm;
						OmitTerm omitTermTBU = omitTerm;

						if (entityMap.isRegEmptyOrNoRegEntryAndNoLinkedQuery()) {
							return handleEntityMap(t.getItem2(), t.getItem1(), tenant, idsAndTypeQueryAndIdPatternTBU,
									attrsQuery, qQuery, geoQuery, scopeQuery, langQuery, limit, offSet, count,
									dataSetIdTerm, join, joinLevel, context, jsonKeys, headersFromReq, pickTerm,
									omitTerm, viaHeaders);
						} else if (entityMap.isNoRootLevelRegEntryAndLinkedQuery()) {
							if (attrsQuery == null && pickTerm == null && qQuery == null && omitTerm == null
									&& dataSetIdTerm == null) {
								Query storedQuery = entityMap.getQuery();
								attrsQueryTBU = storedQuery.getAttrsQueryTerm();
								qQueryTBU = storedQuery.getqQueryTerm();
								dataSetIdTermTBU = storedQuery.getDataSetIdTerm();
								pickTermTBU = storedQuery.getPickTerm();
								omitTermTBU = storedQuery.getOmitTerm();
								jsonKeysTBU = storedQuery.getJsonKeys();
							}
						} else {
							if (idsAndTypeQueryAndIdPattern == null && attrsQuery == null && qQuery == null
									&& csf == null && geoQuery == null && scopeQuery == null && langQuery == null
									&& jsonKeys == null && dataSetIdTerm == null && join == null && joinLevel <= 0
									&& pickTerm == null && omitTerm == null) {
								Query storedQuery = entityMap.getQuery();
								idsAndTypeQueryAndIdPatternTBU = storedQuery.getIdsAndTypeAndIdPattern();
								attrsQueryTBU = storedQuery.getAttrsQueryTerm();
								qQueryTBU = storedQuery.getqQueryTerm();
								geoQueryTBU = storedQuery.getGeoQueryTerm();
								scopeQueryTBU = storedQuery.getScopeQueryTerm();
								langQueryTBU = storedQuery.getLanguageQueryTerm();
								jsonKeysTBU = storedQuery.getJsonKeys();
								dataSetIdTermTBU = storedQuery.getDataSetIdTerm();
								joinTBU = storedQuery.getJoin();
								joinLevelTBU = storedQuery.getJoinLevel();
								pickTermTBU = storedQuery.getPickTerm();
								omitTermTBU = storedQuery.getOmitTerm();
							}
						}
						return handleEntityMap(t.getItem2(), t.getItem1(), tenant, idsAndTypeQueryAndIdPatternTBU,
								attrsQueryTBU, qQueryTBU, geoQueryTBU, scopeQueryTBU, langQueryTBU, limit, offSet,
								count, dataSetIdTermTBU, joinTBU, joinLevelTBU, context, jsonKeysTBU, headersFromReq,
								pickTermTBU, omitTermTBU, viaHeaders);
					});
		}
	}

	private Uni<Tuple2<EntityCache, EntityMap>> getEntityMapAndEntitiesAndUpdateExpires(String tenant,
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPattern, int limit, int offset,
			String qToken, String checkSum) {
		return queryDAO.queryForEntityMapAndEntities(tenant, qToken, idsAndTypeQueryAndIdPattern, limit, offset,
				checkSum);

	}

	private Uni<QueryResult> handleEntityMap(EntityMap entityMap, EntityCache entityCache, String tenant,
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPattern, AttrsQueryTerm attrsQuery,
			QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, int limit,
			int offSet, boolean count, DataSetIdTerm dataSetIdTerm, String join, int joinLevel, Context context,
			Set<String> jsonKeys, io.vertx.core.MultiMap headersFromReq, PickTerm pickTerm, OmitTerm omitTerm,
			ViaHeaders viaHeaders) {
		QueryResult result = new QueryResult();

		List<Map<String, Object>> resultData = Lists.newArrayList();
		result.setData(resultData);
		result.setCount((long) entityMap.size());
		result.setqToken(entityMap.getId());
		result.setLimit(limit);
		result.setOffset(offSet);
		result.setLanguageQueryTerm(langQuery);
		long leftAfter = entityMap.size() - (offSet + limit);
		if (leftAfter < 0) {
			leftAfter = 0;
		}
		result.setResultsLeftAfter(leftAfter);
		result.setResultsLeftBefore((long) offSet);
		Stream<Entry<String, Set<String>>> subMap = entityMap.getEntityId2CSourceIds().entrySet().stream().skip(offSet)
				.limit(limit);

		// no registry entries just push out the result
		boolean isFlatJoin = NGSIConstants.FLAT.equals(join);
		result.setIsFlatJoin(isFlatJoin);
		if (entityMap.isRegEmptyOrNoRegEntryAndNoLinkedQuery()) {

			subMap.forEach(id2Hosts -> {
				//String id = id2Hosts.getKey();
				if (isFlatJoin) {
					resultData.add(entityCache.remove(id2Hosts.getKey()).getItem1());
				} else {
					resultData.add(entityCache.get(id2Hosts.getKey()).getItem1());
				}
			});

			if (join != null && joinLevel > 0) {
				if (NGSIConstants.FLAT.equals(join)) {
					Map<String, Map<String, Object>> flatEntities = new HashMap<>(entityCache.size());
					for (Entry<String, Tuple2<Map<String, Object>, Set<String>>> entityEntry : entityCache.entrySet()) {
						Map<String, Object> entity = entityEntry.getValue().getItem1();
						flatEntities.put((String) entity.get(NGSIConstants.JSON_LD_ID), entity);
						result.setFlatJoin(flatEntities);
					}
				} else if (NGSIConstants.INLINE.equals(join)) {
					for (Map<String, Object> entity : resultData) {
						inlineEntity(entity, entityCache, 1, joinLevel, false);
					}
				}
				if ((pickTerm != null && pickTerm.isHasAnyLinked())
						|| (omitTerm != null && omitTerm.isHasAnyLinked())) {
					EntityTools.evaluateFilterQueries(result, null, null, null, null, pickTerm, omitTerm, null,
							entityCache, null, true);
				} else {
					if (result.getFlatJoin() != null) {
						result.getData().addAll(result.getFlatJoin().values());
					}
				}
			}

			return Uni.createFrom().item(result);
		} else if (entityMap.isNoRootLevelRegEntryAndLinkedQuery()) {

			subMap.forEach(id2Hosts -> {
				resultData.add(entityCache.get(id2Hosts.getKey()).getItem1());
			});
			if (qQuery != null && qQuery.hasLinkedQ()) {
				return retrieveJoins(tenant, resultData, entityCache, context, qQuery, 0, -1, qQuery.getMaxJoinLevel(),
						viaHeaders, entityMap.isDistEntities()).onItem().transformToUni(updatedEntityCache -> {
							Map<String, Map<String, Object>> deleted = EntityTools.evaluateFilterQueries(result, qQuery,
									null, null, attrsQuery, pickTerm, omitTerm, dataSetIdTerm, updatedEntityCache,
									jsonKeys, false);
							if (deleted.isEmpty()) {
								if (entityMap.isChanged()) {
									return queryDAO.storeEntityMap(tenant, entityMap.getId(), entityMap).onItem()
											.transformToUni(v -> {
												return doJoinIfNeeded(tenant, result, updatedEntityCache, context, join,
														joinLevel, viaHeaders, pickTerm, omitTerm,
														entityMap.isDistEntities());
											});
								}
								return doJoinIfNeeded(tenant, result, updatedEntityCache, context, join, joinLevel,
										viaHeaders, pickTerm, omitTerm, entityMap.isDistEntities());
							} else {
								return updateEntityMapAndRepull(deleted, entityMap, updatedEntityCache, tenant,
										idsAndTypeQueryAndIdPattern, attrsQuery, qQuery, geoQuery, scopeQuery,
										langQuery, limit, offSet, count, dataSetIdTerm, join, joinLevel, context,
										jsonKeys, headersFromReq, pickTerm, omitTerm, viaHeaders);
							}
						});
			} else {
				if (attrsQuery != null) {
					attrsQuery.calculateQuery(resultData);
				}
				return doJoinIfNeeded(tenant, result, entityCache, context, join, joinLevel, viaHeaders, pickTerm,
						omitTerm, entityMap.isDistEntities());
			}
		} else {
			return fillCacheFromEntityMap(entityMap, entityCache, context, headersFromReq, false, tenant, offSet, limit)
					.onItem().transformToUni(updatedEntityCache -> {
						subMap.forEach(id2Hosts -> {
							Tuple2<Map<String, Object>, Set<String>> tpl = updatedEntityCache.get(id2Hosts.getKey());
							if (tpl != null) {
								Map<String, Object> itm1 = tpl.getItem1();
								if (itm1 != null) {
									resultData.add(itm1);
								}
							}

						});
						if ((join == null || joinLevel < 0) && (qQuery == null || !qQuery.hasLinkedQ())) {
							if (!entityMap.isDistEntities()) {
								return Uni.createFrom().item(result);
							} else {
								Map<String, Map<String, Object>> deleted = EntityTools.evaluateFilterQueries(result,
										qQuery, scopeQuery, geoQuery, attrsQuery, pickTerm, omitTerm, dataSetIdTerm,
										updatedEntityCache, jsonKeys, false);
								if (deleted.isEmpty()) {
									if (entityMap.isChanged()) {
										return queryDAO.storeEntityMap(tenant, entityMap.getId(), entityMap).onItem()
												.transformToUni(v -> {
													return doJoinIfNeeded(tenant, result, updatedEntityCache, context,
															join, joinLevel, viaHeaders, pickTerm, omitTerm,
															entityMap.isDistEntities());
												});
									}
									return doJoinIfNeeded(tenant, result, updatedEntityCache, context, join, joinLevel,
											viaHeaders, pickTerm, omitTerm, entityMap.isDistEntities());
								} else {
									return updateEntityMapAndRepull(deleted, entityMap, updatedEntityCache, tenant,
											idsAndTypeQueryAndIdPattern, attrsQuery, qQuery, geoQuery, scopeQuery,
											langQuery, limit, offSet, count, dataSetIdTerm, join, joinLevel, context,
											jsonKeys, headersFromReq, pickTerm, omitTerm, viaHeaders);
								}
							}

						} else {
							int qMaxJoinLevel;
							if (qQuery != null && qQuery.hasLinkedQ()) {
								qMaxJoinLevel = qQuery.getMaxJoinLevel();
							} else {
								qMaxJoinLevel = -1;
							}
							if (qMaxJoinLevel < joinLevel) {
								qMaxJoinLevel = joinLevel;
							}
							return retrieveJoins(tenant, resultData, entityCache, context, qQuery, 0, joinLevel,
									qMaxJoinLevel, viaHeaders, false).onItem().transformToUni(updatedEntityCache2 -> {
										if (!entityMap.isDistEntities()) {
											Map<String, Map<String, Object>> deleted = EntityTools
													.evaluateFilterQueries(result, qQuery, null, null, attrsQuery,
															pickTerm, omitTerm, null, entityCache, jsonKeys, false);
											if (deleted.isEmpty()) {
												if (entityMap.isChanged()) {
													return queryDAO.storeEntityMap(tenant, entityMap.getId(), entityMap)
															.onItem().transformToUni(v -> {
																return doJoinIfNeeded(tenant, result,
																		updatedEntityCache, context, join, joinLevel,
																		viaHeaders, pickTerm, omitTerm,
																		entityMap.isDistEntities());
															});
												}
												return doJoinIfNeeded(tenant, result, updatedEntityCache2, context,
														join, joinLevel, viaHeaders, pickTerm, omitTerm,
														entityMap.isDistEntities());
											} else {
												return updateEntityMapAndRepull(deleted, entityMap, updatedEntityCache2,
														tenant, idsAndTypeQueryAndIdPattern, attrsQuery, qQuery,
														geoQuery, scopeQuery, langQuery, limit, offSet, count,
														dataSetIdTerm, join, joinLevel, context, jsonKeys,
														headersFromReq, pickTerm, omitTerm, viaHeaders);
											}
										} else {
											Map<String, Map<String, Object>> deleted = EntityTools
													.evaluateFilterQueries(result, qQuery, scopeQuery, geoQuery,
															attrsQuery, pickTerm, omitTerm, dataSetIdTerm, entityCache,
															jsonKeys, false);
											if (deleted.isEmpty()) {
												if (entityMap.isChanged()) {
													return queryDAO.storeEntityMap(tenant, entityMap.getId(), entityMap)
															.onItem().transformToUni(v -> {
																return doJoinIfNeeded(tenant, result,
																		updatedEntityCache, context, join, joinLevel,
																		viaHeaders, pickTerm, omitTerm,
																		entityMap.isDistEntities());
															});
												}
												return doJoinIfNeeded(tenant, result, updatedEntityCache, context, join,
														joinLevel, viaHeaders, pickTerm, omitTerm,
														entityMap.isDistEntities());
											} else {
												return updateEntityMapAndRepull(deleted, entityMap, updatedEntityCache,
														tenant, idsAndTypeQueryAndIdPattern, attrsQuery, qQuery,
														geoQuery, scopeQuery, langQuery, limit, offSet, count,
														dataSetIdTerm, join, joinLevel, context, jsonKeys,
														headersFromReq, pickTerm, omitTerm, viaHeaders);
											}
										}
									});

						}

					});

		}

	}

	private Uni<EntityCache> fillCacheFromEntityMap(EntityMap entityMap, EntityCache entityCache, Context context,
			io.vertx.core.MultiMap headersFromReq, boolean callDB, String tenant, int offset, int limit) {
		Map<QueryRemoteHost, Set<String>> remoteHost2Ids = Maps.newHashMap();
		Set<String> idsForDBCall = Sets.newHashSet();
		Stream<Entry<String, Set<String>>> subMap = entityMap.getEntityId2CSourceIds().entrySet().stream().skip(offset)
				.limit(limit);
		subMap.forEach(id2Host -> {

			List<QueryRemoteHost> hosts = Lists.newArrayList();
			String entityId = id2Host.getKey();
			id2Host.getValue().forEach(cId -> {
				if (!NGSIConstants.JSON_LD_NONE.equals(cId)) {
					hosts.add(entityMap.getRemoteHost(cId));
				}
			});
			for (QueryRemoteHost host : hosts) {

				if (callDB) {
					Tuple2<Map<String, Object>, Set<String>> cacheEntry = entityCache.get(entityId);
					if (cacheEntry == null || !cacheEntry.getItem2().contains(NGSIConstants.JSON_LD_NONE)) {
						idsForDBCall.add(entityId);
					}
				}

				Set<String> ids = remoteHost2Ids.get(host);
				if (ids == null) {
					ids = Sets.newHashSet();
					remoteHost2Ids.put(host, ids);
				}
				ids.add(entityId);
			}
		});
		List<Uni<Tuple2<List<Map<String, Object>>, QueryRemoteHost>>> unis = Lists.newArrayList();
		for (Entry<QueryRemoteHost, Set<String>> entry : remoteHost2Ids.entrySet()) {
			QueryRemoteHost host = entry.getKey();
			Map<String, String> queryParams = host.getQueryParam();
			queryParams.put(NGSIConstants.ID, StringUtils.join(entry.getValue(), ','));

			unis.add(EntityTools.getRemoteEntities(host, webClient, timeout, ldService).onItem()
					.transform(entities -> Tuple2.of(entities, host)));
		}
		if (!idsForDBCall.isEmpty()) {
			unis.add(0, queryDAO.queryForEntities(tenant, idsForDBCall));
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().item(entityCache);
		}
		return Uni.combine().all().unis(unis).with(l -> {
			mergeMultipleQueryResults(l, entityCache, null);
			return entityCache;
		});
	}

	private Collection<Map<String, Object>> mergeMultipleQueryResults(List<?> l, EntityCache entityCache,
			EntityMap entityMap) {
		Map<String, Set<String>> entityId2Types = Maps.newHashMap();
		Map<String, Set<String>> entityId2Scopes = Maps.newHashMap();
		Map<String, Long> entityId2YoungestModified = Maps.newHashMap();
		Map<String, Long> entityId2OldestCreatedAt = Maps.newHashMap();
		Map<String, Map<String, Integer>> entityId2AttrDatasetId2CurrentRegMode = Maps.newHashMap();
		Map<String, Map<String, Map<String, Map<String, Object>>>> entityId2AttrName2DatasetId2AttrValue = Maps
				.newHashMap();
		List<String> onlyAddedToCache = Lists.newArrayList();
		String id;
		Map<String, Map<String, Object>> id2Entity = Maps.newHashMap();
		for (Object o : l) {
			Tuple2<List<Map<String, Object>>, QueryRemoteHost> t = (Tuple2<List<Map<String, Object>>, QueryRemoteHost>) o;
			List<Map<String, Object>> entities = t.getItem1();
			QueryRemoteHost remoteHost = t.getItem2();
			for (Map<String, Object> entity : entities) {
				id = (String) entity.get(NGSIConstants.JSON_LD_ID);
				Tuple2<Map<String, Object>, Set<String>> potentialLocalEntity = entityCache.get(id);
				if (entityMap != null) {
					entityMap.addEntry(id, "generated-" + remoteHost.cSourceId(), remoteHost);
				}
				if (potentialLocalEntity == null || potentialLocalEntity.getItem1() == null) {
					entityCache.putEntity(id, entity, t.getItem2().cSourceId());
					id2Entity.put(id, entity);
					onlyAddedToCache.add(id);
				} else {
					if (!entityId2Types.containsKey(id)) {
						mergeEntity(id, potentialLocalEntity.getItem1(), entityId2AttrName2DatasetId2AttrValue,
								entityId2Types, entityId2Scopes, entityId2YoungestModified, entityId2OldestCreatedAt,
								entityId2AttrDatasetId2CurrentRegMode);
					}
					mergeEntity(id, entity, entityId2AttrName2DatasetId2AttrValue, entityId2Types, entityId2Scopes,
							entityId2YoungestModified, entityId2OldestCreatedAt, entityId2AttrDatasetId2CurrentRegMode);
				}

			}
		}

		for (Entry<String, Map<String, Map<String, Map<String, Object>>>> entry : entityId2AttrName2DatasetId2AttrValue
				.entrySet()) {
			String entityId = entry.getKey();
			Set<String> types = entityId2Types.get(entityId);
			Map<String, Map<String, Map<String, Object>>> attribMap = entry.getValue();
			Map<String, Object> entity = new HashMap<>(attribMap.size() + 5);
			entity.put(NGSIConstants.JSON_LD_ID, entityId);
			entity.put(NGSIConstants.JSON_LD_TYPE, Lists.newArrayList(types));
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
			entity.remove(AppConstants.REG_MODE_KEY);
			entityCache.putEntity(entityId, entity, "dummy");
			id2Entity.put(entityId, entity);
		}
		for (String idEntry : onlyAddedToCache) {
			id2Entity.get(idEntry).remove(AppConstants.REG_MODE_KEY);
		}
		return id2Entity.values();

	}

	private Uni<QueryResult> doJoinIfNeeded(String tenant, QueryResult result, EntityCache entityCache, Context context,
			String join, int joinLevel, ViaHeaders viaHeaders, PickTerm pick, OmitTerm omit, boolean splitEntities) {
		if (join != null && joinLevel > 0) {
			List<Map<String, Object>> resultData = result.getData();
			return retrieveJoins(tenant, resultData, entityCache, context, null, 0, joinLevel, joinLevel, viaHeaders,
					splitEntities).onItem().transformToUni(updatedCache2 -> {
						boolean doFlatJoin = NGSIConstants.FLAT.equals(join);
						if (doFlatJoin) {
							Map<String, Map<String, Object>> toAdd = Maps.newHashMap();
							for (Map<String, Object> entity : resultData) {
								flatAddEntity(entity, entityCache, 0, joinLevel, toAdd, false);
							}
							result.setFlatJoin(toAdd);
						} else if (NGSIConstants.INLINE.equals(join)) {
							for (Map<String, Object> entity : resultData) {
								inlineEntity(entity, entityCache, 0, joinLevel, false);
							}
						}
						if ((pick != null && pick.isHasAnyLinked()) || (omit != null && omit.isHasAnyLinked())) {
							EntityTools.evaluateFilterQueries(result, null, null, null, null, pick, omit, null,
									entityCache, null, true);
						} else if (doFlatJoin) {
							result.getData().addAll(result.getFlatJoin().values());
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

	private void flatAddAttrib(Object attribObj, EntityCache entityCache, int currentJoinLevel, int joinLevel,
			Map<String, Map<String, Object>> toAdd, boolean localOnly) {
		if (attribObj instanceof Map<?, ?> attribMap) {
			Object typeObj = attribMap.get(NGSIConstants.JSON_LD_TYPE);
			if (typeObj != null && typeObj instanceof List<?> typeList) {
				if (typeList.contains(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
					List<Map<String, String>> hasObject = (List<Map<String, String>>) attribMap
							.get(NGSIConstants.NGSI_LD_HAS_OBJECT);

					if (localOnly) {
						Map<String, Tuple2<Map<String, Object>, Set<String>>> ids2EntityAndHost = entityCache
								.getAllIds2EntityAndHosts();
						for (Map<String, String> idEntry : hasObject) {
							String entityId = idEntry.get(NGSIConstants.JSON_LD_ID);
							Tuple2<Map<String, Object>, Set<String>> entityAndHosts = ids2EntityAndHost.get(entityId);
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
					} else if (attribMap.containsKey(NGSIConstants.NGSI_LD_OBJECT_TYPE)) {
						List<Object> linkedTypes = (List<Object>) attribMap.get(NGSIConstants.NGSI_LD_OBJECT_TYPE);
						Set<String> tmpTypeSet = new HashSet<>(linkedTypes.size());
						for (Object linkedTypeObj : linkedTypes) {
							if (linkedTypeObj instanceof Map<?, ?> linkedTypeMap
									&& linkedTypeMap.containsKey(NGSIConstants.JSON_LD_ID)) {
								tmpTypeSet.add((String) linkedTypeMap.get(NGSIConstants.JSON_LD_ID));
							}
						}
						for (Map<String, String> idEntry : hasObject) {
							String entityId = idEntry.get(NGSIConstants.JSON_LD_ID);
							Tuple2<Map<String, Object>, Set<String>> entityAndHosts = entityCache.get(entityId);
							if (entityAndHosts != null) {
								Map<String, Object> ogEntity = entityAndHosts.getItem1();
								if (ogEntity != null && ((List<String>) ogEntity.get(NGSIConstants.JSON_LD_TYPE))
										.stream().anyMatch(tmpTypeSet::contains)) {
									toAdd.put((String) ogEntity.get(NGSIConstants.JSON_LD_ID), ogEntity);
									if (currentJoinLevel + 1 <= joinLevel) {
										flatAddEntity(ogEntity, entityCache, currentJoinLevel + 1, joinLevel, toAdd,
												localOnly);
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

						for (Map<String, List<Map<String, List<Map<String, String>>>>> atListEntry : hasObjectList) {
							List<Map<String, List<Map<String, String>>>> atList = atListEntry
									.get(NGSIConstants.JSON_LD_LIST);
							for (Map<String, List<Map<String, String>>> hasObjectEntry : atList) {
								List<Map<String, String>> objectList = hasObjectEntry
										.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
								for (Map<String, String> objectEntry : objectList) {
									String entityId = objectEntry.get(NGSIConstants.JSON_LD_ID);
									Tuple2<Map<String, Object>, Set<String>> entityAndHosts = entityCache.get(entityId);
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
					} else if (attribMap.containsKey(NGSIConstants.NGSI_LD_OBJECT_TYPE)) {
						List<Object> linkedTypes = (List<Object>) attribMap.get(NGSIConstants.NGSI_LD_OBJECT_TYPE);
						Set<String> tmpTypeSet = new HashSet<>(linkedTypes.size());
						for (Object linkedTypeObj : linkedTypes) {
							if (linkedTypeObj instanceof Map<?, ?> linkedTypeMap
									&& linkedTypeMap.containsKey(NGSIConstants.JSON_LD_ID)) {
								tmpTypeSet.add((String) linkedTypeMap.get(NGSIConstants.JSON_LD_ID));
							}
						}
						for (Map<String, List<Map<String, List<Map<String, String>>>>> atListEntry : hasObjectList) {
							List<Map<String, List<Map<String, String>>>> atList = atListEntry
									.get(NGSIConstants.JSON_LD_LIST);
							for (Map<String, List<Map<String, String>>> hasObjectEntry : atList) {
								List<Map<String, String>> objectList = hasObjectEntry
										.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
								for (Map<String, String> objectEntry : objectList) {
									String entityId = objectEntry.get(NGSIConstants.JSON_LD_ID);
									Tuple2<Map<String, Object>, Set<String>> entityAndHosts = entityCache.get(entityId);
									if (entityAndHosts != null) {
										Map<String, Object> ogEntity = entityAndHosts.getItem1();
										if (ogEntity != null
												&& ((List<String>) ogEntity.get(NGSIConstants.JSON_LD_TYPE)).stream()
														.anyMatch(tmpTypeSet::contains)) {
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
					}
				}
			}
		}

	}

	private Uni<QueryResult> updateEntityMapAndRepull(Map<String, Map<String, Object>> deleted, EntityMap entityMap,
			EntityCache entityCache, String tenant,
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPattern, AttrsQueryTerm attrsQuery,
			QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, int limit,
			int offSet, boolean count, DataSetIdTerm dataSetIdTerm, String join, int joinLevel, Context context,
			Set<String> jsonKeys, io.vertx.core.MultiMap headersFromReq, PickTerm pickTerm, OmitTerm omitTerm,
			ViaHeaders viaHeaders) {
		if (entityMap.removeEntries(deleted.keySet())) {
			QueryResult result = new QueryResult();
			List<Map<String, Object>> resultData = Lists.newArrayList();
			result.setData(resultData);
			result.setCount((long) entityMap.size());
			result.setqToken(entityMap.getId());
			result.setLimit(limit);
			result.setOffset(offSet);

			long leftAfter = entityMap.size() - (offSet + limit);
			if (leftAfter < 0) {
				leftAfter = 0;
			}
			result.setResultsLeftAfter(leftAfter);
			result.setResultsLeftBefore((long) offSet);
			Stream<Entry<String, Set<String>>> subMap = entityMap.getEntityId2CSourceIds().entrySet().stream()
					.skip(offSet).limit(limit);
			subMap.forEach(id2Hosts -> {
				resultData.add(entityCache.getAllIds2EntityAndHosts().get(id2Hosts.getKey()).getItem1());
			});

			return doJoinIfNeeded(tenant, result, entityCache, context, join, joinLevel, viaHeaders, pickTerm, omitTerm,
					entityMap.isDistEntities());

		}

		entityMap.setChanged(true);

		return fillCacheFromEntityMap(entityMap, entityCache, context, headersFromReq, true, tenant, offSet,
				limit + (deleted.size() * 3)).onItem().transformToUni(updatedCache -> {
					return handleEntityMap(entityMap, entityCache, tenant, idsAndTypeQueryAndIdPattern, attrsQuery,
							qQuery, geoQuery, scopeQuery, langQuery, limit, offSet, count, dataSetIdTerm, join,
							joinLevel, context, jsonKeys, headersFromReq, pickTerm, omitTerm, viaHeaders);
				});

	}

	private Uni<EntityCache> retrieveJoins(String tenant, Collection<Map<String, Object>> currentLevel,
			EntityCache entityCache, Context context, QQueryTerm qQuery, int currentJoinLevel, int joinLevel,
			int maxJoinLevel, ViaHeaders viaHeaders, boolean splitEntities) {
		Map<Set<String>, Set<String>> types2EntityIds = null;
		if (currentJoinLevel < joinLevel) {
			types2EntityIds = getAllTypesAndIds(currentLevel);
		} else if (qQuery != null) {
			Map<String, Set<String>> attribAndTypes = qQuery.getJoinLevel2AttribAndTypes().get(currentJoinLevel);
			if (attribAndTypes == null) {
				return Uni.createFrom().item(entityCache);
			}
			types2EntityIds = getAllTypesAndIds(currentLevel, attribAndTypes);
		}
		if (types2EntityIds == null || types2EntityIds.isEmpty()) {
			return Uni.createFrom().item(entityCache);
		}
		return getEntitiesFromUncalledHosts(tenant, types2EntityIds, entityCache, context, qQuery, viaHeaders, context,
				splitEntities).onItem().transformToUni(t -> {
					int nextLevel = currentJoinLevel + 1;
					if (nextLevel < maxJoinLevel && !t.getItem2().isEmpty()) {
						return retrieveJoins(tenant, t.getItem2(), entityCache, context, qQuery, nextLevel, joinLevel,
								maxJoinLevel, viaHeaders, splitEntities);
					}
					return Uni.createFrom().item(t.getItem1());
				});

	}

	private Map<Set<String>, Set<String>> getAllTypesAndIds(Collection<Map<String, Object>> currentLevel,
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

	private Map<Set<String>, Set<String>> getAllTypesAndIds(Collection<Map<String, Object>> currentLevel) {
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
				if (attrValueObj instanceof Map<?, ?> map && map.containsKey(NGSIConstants.JSON_LD_TYPE)
						&& map.get(NGSIConstants.JSON_LD_TYPE) instanceof List<?>) {
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
			if (attribObj instanceof List<?> list) {
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
		if (attribObj instanceof @SuppressWarnings("rawtypes") Map attribMap) {
			Object typeObj = attribMap.get(NGSIConstants.JSON_LD_TYPE);
			if (typeObj != null && typeObj instanceof List<?> typeList) {
				if (typeList.contains(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
					List<Map<String, String>> hasObject = (List<Map<String, String>>) attribMap
							.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
					List<Map<String, Object>> entities = new ArrayList<>(hasObject.size());

					if (localOnly) {

						for (Map<String, String> idEntry : hasObject) {
							String entityId = idEntry.get(NGSIConstants.JSON_LD_ID);
							Tuple2<Map<String, Object>, Set<String>> entityAndHosts = entityCache.get(entityId);
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
					} else if (attribMap.containsKey(NGSIConstants.NGSI_LD_OBJECT_TYPE)) {
						List<Object> linkedTypes = (List<Object>) attribMap.get(NGSIConstants.NGSI_LD_OBJECT_TYPE);
						Set<String> tmpTypeSet = new HashSet<>(linkedTypes.size());
						for (Object linkedTypeObj : linkedTypes) {
							if (linkedTypeObj instanceof Map<?, ?> linkedTypeMap
									&& linkedTypeMap.containsKey(NGSIConstants.JSON_LD_ID)) {
								tmpTypeSet.add((String) linkedTypeMap.get(NGSIConstants.JSON_LD_ID));
							}
						}
						for (Map<String, String> idEntry : hasObject) {
							String entityId = idEntry.get(NGSIConstants.JSON_LD_ID);
							Tuple2<Map<String, Object>, Set<String>> entityAndHosts = entityCache.get(entityId);
							if (entityAndHosts != null) {
								Map<String, Object> ogEntity = entityAndHosts.getItem1();
								if (ogEntity != null && ((List<String>) ogEntity.get(NGSIConstants.JSON_LD_TYPE))
										.stream().anyMatch(tmpTypeSet::contains)) {
									Map<String, Object> entity = MicroServiceUtils.deepCopyMap(ogEntity);
									entities.add(entity);
									if (currentJoinLevel + 1 <= joinLevel) {
										inlineEntity(entity, entityCache, currentJoinLevel + 1, joinLevel, localOnly);
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
						for (Map<String, List<Map<String, List<Map<String, String>>>>> atListEntry : hasObjectList) {
							List<Map<String, List<Map<String, String>>>> atList = atListEntry
									.get(NGSIConstants.JSON_LD_LIST);
							for (Map<String, List<Map<String, String>>> hasObjectEntry : atList) {
								List<Map<String, String>> objectList = hasObjectEntry
										.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
								for (Map<String, String> objectEntry : objectList) {
									String entityId = objectEntry.get(NGSIConstants.JSON_LD_ID);
									Tuple2<Map<String, Object>, Set<String>> entityAndHosts = entityCache.get(entityId);
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
					} else if (attribMap.containsKey(NGSIConstants.NGSI_LD_OBJECT_TYPE)) {
						List<Object> linkedTypes = (List<Object>) attribMap.get(NGSIConstants.NGSI_LD_OBJECT_TYPE);
						Set<String> tmpTypeSet = new HashSet<>(linkedTypes.size());
						for (Object linkedTypeObj : linkedTypes) {
							if (linkedTypeObj instanceof Map<?, ?> linkedTypeMap
									&& linkedTypeMap.containsKey(NGSIConstants.JSON_LD_ID)) {
								tmpTypeSet.add((String) linkedTypeMap.get(NGSIConstants.JSON_LD_ID));
							}
						}
						for (Map<String, List<Map<String, List<Map<String, String>>>>> atListEntry : hasObjectList) {
							List<Map<String, List<Map<String, String>>>> atList = atListEntry
									.get(NGSIConstants.JSON_LD_LIST);
							for (Map<String, List<Map<String, String>>> hasObjectEntry : atList) {
								List<Map<String, String>> objectList = hasObjectEntry
										.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
								for (Map<String, String> objectEntry : objectList) {
									String entityId = objectEntry.get(NGSIConstants.JSON_LD_ID);
									Tuple2<Map<String, Object>, Set<String>> entityAndHosts = entityCache.get(entityId);
									if (entityAndHosts != null) {
										Map<String, Object> ogEntity = entityAndHosts.getItem1();
										if (ogEntity != null
												&& ((List<String>) ogEntity.get(NGSIConstants.JSON_LD_TYPE)).stream()
														.anyMatch(tmpTypeSet::contains)) {
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
					if (!entities.isEmpty()) {
						attribMap.put(NGSIConstants.NGSI_LD_ENTITY_LIST,
								List.of(Map.of(NGSIConstants.JSON_LD_LIST, entities)));
					}

				}
			}
		}
	}

	public Uni<Map<String, Object>> getEntityMap(String tenant, String qToken) {
		// todo transform to output data model
		return queryDAO.getEntityMap(tenant, qToken);
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
		if (entity.containsKey(AppConstants.REG_MODE_KEY)) {
			regMode = (Integer) entity.remove(AppConstants.REG_MODE_KEY);
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

	
	public Uni<List<Map<String, Object>>> getTypesWithDetail(String tenant, boolean localOnly,
			io.vertx.core.MultiMap headersFromReq) {
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
								NGSIConstants.NGSI_LD_TYPES_ENDPOINT + "?details=true", headersFromReq);
						return Uni.combine().all().unis(unis).with(list -> {
							for (Object entry : list) {
								if (((List<?>) entry).isEmpty()) {
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

	public Uni<Map<String, Object>> getTypes(String tenant, boolean localOnly, io.vertx.core.MultiMap headersFromReq) {
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
						List<Uni<List<Object>>> unis = getRemoteCalls(rows, NGSIConstants.NGSI_LD_TYPES_ENDPOINT,
								headersFromReq);
						return Uni.combine().all().unis(unis).with(list -> {
							for (Object entry : list) {
								if (!((List<?>) entry).isEmpty()) {
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

	public Uni<Map<String, Object>> getType(String tenant, String type, boolean localOnly,
			io.vertx.core.MultiMap headersFromReq) {
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
								NGSIConstants.NGSI_LD_TYPES_ENDPOINT + "/" + type, headersFromReq);
						return Uni.combine().all().unis(remoteResults).with(list -> {
							long count = 0;
							for (Object obj : list) {
								if (!((List<?>) obj).isEmpty()) {
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

	public Uni<List<Map<String, Object>>> getAttribsWithDetails(String tenant, boolean localOnly,
			io.vertx.core.MultiMap headersFromReq) {
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
								NGSIConstants.NGSI_LD_ATTRIBUTES_ENDPOINT + "?details=true", headersFromReq);
						return Uni.combine().all().unis(remoteResults).with(list -> {
							for (Object obj : list) {
								if (((List<?>) obj).isEmpty()) {
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

	public Uni<Map<String, Object>> getAttribs(String tenant, boolean localOnly,
			io.vertx.core.MultiMap headersFromReq) {
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
								NGSIConstants.NGSI_LD_ATTRIBUTES_ENDPOINT, headersFromReq);
						return Uni.combine().all().unis(remoteResults).with(list -> {
							for (Object obj : list) {
								if (((List<?>) obj).isEmpty()) {
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

	public Uni<Map<String, Object>> getAttrib(String tenant, String attrib, boolean localOnly,
			io.vertx.core.MultiMap headersFromReq) {
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
								NGSIConstants.NGSI_LD_ATTRIBUTES_ENDPOINT + "/" + attrib, headersFromReq);
						return Uni.combine().all().unis(remoteResults).with(list -> {
							long count = 0;
							for (Object obj : list) {
								if (((List<?>) obj).isEmpty()) {
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

	private List<Uni<List<Object>>> getRemoteCalls(RowSet<Row> rows, String endpoint,
			io.vertx.core.MultiMap headersFromReq) {
		List<Uni<List<Object>>> unis = Lists.newArrayList();
		rows.forEach(row -> {
			// C.endpoint C.tenant_id, c.headers, c.reg_mode
			String host = row.getString(0);
			MultiMap remoteHeaders = MultiMap
					.newInstance(HttpUtils.getHeadersForRemoteCall(row.getJsonArray(2), row.getString(1)));
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHeaders, headersFromReq);

			unis.add(webClient.getAbs(host + endpoint).putHeaders(toFrwd).send().onFailure().recoverWithNull().onItem()
					.transformToUni(response -> {
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

	public Uni<Void> handleRegistryChange(CSourceBaseRequest req) {
		return RegistrationEntry.fromRegPayload(req.getPayload(), ldService).onItem().transformToUni(regs -> {
			List<RegistrationEntry> newRegs = Lists.newArrayList();
			tenant2CId2RegEntries.remove(req.getTenant(), req.getId());
			if (req.getRequestType() != AppConstants.DELETE_REQUEST) {
				for (RegistrationEntry regEntry : regs) {
					if (regEntry.retrieveEntity() || regEntry.queryEntity() || regEntry.queryBatch()) {
						newRegs.add(regEntry);
					}
				}
				tenant2CId2RegEntries.put(req.getTenant(), req.getId(), newRegs);
			}
			return Uni.createFrom().voidItem();
		});
	}

	public Uni<Tuple2<EntityCache, EntityMap>> getAndStoreEntityMap(String tenant, String qToken,
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPattern, AttrsQueryTerm attrsQuery,
			GeoQueryTerm geoQuery, QQueryTerm qQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, int limit,
			int offset, Context context, io.vertx.core.MultiMap headersFromReq, boolean doNotCompact,
			DataSetIdTerm dataSetIdTerm, String join, int joinLevel, boolean splitEntities, PickTerm pickTerm,
			OmitTerm omitTerm, String queryCechksum, ViaHeaders viaHeaders, String typePattern, boolean localOnly) {

		if (tenant2CId2RegEntries.isEmpty()) {
			return queryDAO.createEntityMapAndFillEntityCache(tenant, idsAndTypeQueryAndIdPattern, attrsQuery, qQuery,
					geoQuery, scopeQuery, context, limit, offset, dataSetIdTerm, join, joinLevel, qToken, pickTerm,
					omitTerm, queryCechksum, splitEntities, true, false, typePattern, localOnly);
		} else {
			EntityCache fullEntityCache = new EntityCache();
			Collection<QueryRemoteHost> remoteHost2Query = EntityTools.getRemoteQueries(tenant,
					idsAndTypeQueryAndIdPattern, attrsQuery, qQuery, geoQuery, scopeQuery, langQuery,
					tenant2CId2RegEntries, context, fullEntityCache, splitEntities, viaHeaders);
			if (remoteHost2Query.isEmpty()) {
				if ((join == null || joinLevel <= 0) && (qQuery == null || !qQuery.hasLinkedQ())) {
					return queryDAO.createEntityMapAndFillEntityCache(tenant, idsAndTypeQueryAndIdPattern, attrsQuery,
							qQuery, geoQuery, scopeQuery, context, limit, offset, dataSetIdTerm, join, joinLevel,
							qToken, pickTerm, omitTerm, queryCechksum, splitEntities, true, false, typePattern, localOnly);
				} else {
					return queryDAO.createEntityMapAndFillEntityCache(tenant, idsAndTypeQueryAndIdPattern, attrsQuery,
							qQuery, geoQuery, scopeQuery, context, limit, offset, dataSetIdTerm, join, joinLevel,
							qToken, pickTerm, omitTerm, queryCechksum, splitEntities, false, true, typePattern, localOnly);
				}
			} else {
				Uni<Tuple2<EntityCache, EntityMap>> localEntityCacheAndEntityMap = queryDAO
						.createEntityMapAndFillEntityCache(tenant, idsAndTypeQueryAndIdPattern, attrsQuery, qQuery,
								geoQuery, scopeQuery, context, limit, offset, dataSetIdTerm, join, joinLevel, qToken,
								pickTerm, omitTerm, queryCechksum, splitEntities, false, false, typePattern, localOnly);
				List<Uni<Tuple2<List<Map<String, Object>>, QueryRemoteHost>>> unisForEntityRetrieval = Lists
						.newArrayList();
				List<Uni<Tuple2<Map<String, Object>, QueryRemoteHost>>> unisForEntityMapRetrieval = Lists
						.newArrayList();

				for (QueryRemoteHost remoteHost : remoteHost2Query) {
					if (remoteHost.canDoEntityMap()) {
						List<Tuple3<String, String, String>> idsAndTypesAndIdPattern = remoteHost
								.getIdsAndTypesAndIdPattern();
						if (idsAndTypesAndIdPattern == null) {
							Tuple3<String, String, String> tmpTpl = Tuple3.of(null, null, null);
							idsAndTypesAndIdPattern = Lists.newArrayList();
							idsAndTypesAndIdPattern.add(tmpTpl);
						}
						for (Tuple3<String, String, String> tpl : idsAndTypesAndIdPattern) {
							String id = tpl.getItem1();
							String type = tpl.getItem2();
							String idPattern = tpl.getItem3();
							HttpRequest<Buffer> req = webClient
									.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITY_MAP_ENDPOINT).timeout(timeout);
							if (id != null) {
								req = req.setQueryParam(NGSIConstants.ID, id);
							}
							if (type != null) {
								req = req.setQueryParam(NGSIConstants.TYPE, type);
							}
							if (idPattern != null) {
								req = req.setQueryParam(NGSIConstants.QUERY_PARAMETER_IDPATTERN, idPattern);
							}
							for (Entry<String, String> param : remoteHost.getQueryParam().entrySet()) {
								req = req.setQueryParam(param.getKey(), (String) param.getValue());
							}
							req = req.putHeader(HttpHeaders.VIA, remoteHost.getViaHeaders().getViaHeaders());
							// todo check how to solve this proper once and for all
							Context contextTBU = remoteHost.context();
							List<String> ogAtContext = contextTBU.getOriginalAtContext();
							if (ogAtContext != null && !ogAtContext.isEmpty()) {
								req = req.putHeader(HttpHeaders.LINK, "<" + ogAtContext.get(0)
										+ ">; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
							}
							req = req.putHeader(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSON);
							unisForEntityMapRetrieval.add(req.putHeaders(remoteHost.headers()).timeout(timeout).send()
									.onItem().transform(response -> {
										Map<String, Object> result;
										if (response != null && response.statusCode() == 200) {
											result = response.bodyAsJsonObject().getMap();

										} else if (response != null && response.statusCode() == 404) {
											result = null;
										} else {
											result = Maps.newHashMap();
										}
										logger.debug("from remote host: " + remoteHost.host()
												+ NGSIConstants.NGSI_LD_ENTITY_MAP_ENDPOINT
												+ remoteHost.getQueryParam());
										return Tuple2.of(result, remoteHost);
									}).onFailure().recoverWithItem(e -> {
										logger.warn(
												"Failed to query entity list from remote host" + remoteHost.toString());
										return Tuple2.of(null, remoteHost);
									}));
						}
					} else {
						unisForEntityRetrieval
								.add(EntityTools.getRemoteEntities(remoteHost, webClient, timeout, ldService).onItem()
										.transform(entities -> Tuple2.of(entities, remoteHost)));
					}
				}
				Uni<List<?>> combinedMaps;
				Uni<List<?>> combinedRemoteEntities;
				if (!unisForEntityMapRetrieval.isEmpty()) {
					combinedMaps = Uni.combine().all().unis(unisForEntityMapRetrieval).with(l -> l);
				} else {
					combinedMaps = Uni.createFrom().nullItem();
				}

				if (!unisForEntityRetrieval.isEmpty()) {
					combinedRemoteEntities = Uni.combine().all().unis(unisForEntityRetrieval).with(l -> l);
				} else {
					combinedRemoteEntities = Uni.createFrom().nullItem();
				}
				return Uni.combine().all().unis(localEntityCacheAndEntityMap, combinedMaps, combinedRemoteEntities)
						.asTuple().onItem().transformToUni(tpl -> {
							Tuple2<EntityCache, EntityMap> localTpl = tpl.getItem1();
							List<?> maps = tpl.getItem2();
							List<?> remoteEntities = tpl.getItem3();
							EntityMap entityMap = localTpl.getItem2();
							EntityCache entityCache = localTpl.getItem1();
							List<Uni<Tuple2<List<Map<String, Object>>, QueryRemoteHost>>> recoverEntityMapFail = Lists
									.newArrayList();
							if (maps != null) {

								for (Object obj : maps) {
									Tuple2<Map<String, Object>, QueryRemoteHost> mapT = (Tuple2<Map<String, Object>, QueryRemoteHost>) obj;
									QueryRemoteHost rHost = mapT.getItem2();
									Map<String, Object> rMap = mapT.getItem1();
									if (rMap == null) {
										recoverEntityMapFail
												.add(EntityTools.getRemoteEntities(rHost, webClient, timeout, ldService)
														.onItem().transform(entities -> Tuple2.of(entities, rHost)));
										continue;
									}
									String cSourceId = rHost.cSourceId();
									entityMap.addLinkedMap(cSourceId, (String) rMap.get(NGSIConstants.ID));
									Map<String, List<String>> entityMapEntry = (Map<String, List<String>>) rMap
											.get(NGSIConstants.ENTITY_MAP_COMPACTED_ENTRY);
									if (entityMapEntry != null) {
										for (Entry<String, List<String>> mapEntry : entityMapEntry.entrySet()) {
											entityMap.addEntry(mapEntry.getKey(), cSourceId, rHost);
										}
									}
								}
							}
							if (!recoverEntityMapFail.isEmpty()) {
								return Uni.combine().all().unis(recoverEntityMapFail).with(l -> {
									@SuppressWarnings("rawtypes")
									List tmpList = l;
									if (remoteEntities != null) {
										tmpList.addAll(remoteEntities);
									}
									return tmpList;
								}).onItem().transformToUni(l1 -> {
									mergeMultipleQueryResults(l1, entityCache, entityMap);
									return queryDAO.storeEntityMap(tenant, qToken, entityMap).onItem()
											.transform(v -> localTpl);
								});
							}
							if (remoteEntities != null) {
								mergeMultipleQueryResults(remoteEntities, entityCache, entityMap);
							}
							return queryDAO.storeEntityMap(tenant, qToken, entityMap).onItem().transform(v -> localTpl);
						});
			}

		}

	}

	@Scheduled(every = "${scorpio.entitymap.cleanup.schedule}", delayed = "${scorpio.startupdelay}")
	public Uni<Void> scheduleEntityMapCleanUp() {
		return queryDAO.runEntityMapCleanup(entityMapTTL);
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

	public Uni<Tuple2<EntityCache, Collection<Map<String, Object>>>> getEntitiesFromUncalledHosts(String tenant,
			Map<Set<String>, Set<String>> types2EntityIds, EntityCache fullEntityCache, Context linkHeaders,
			QQueryTerm linkedQ, ViaHeaders viaHeaders, Context context, boolean splitEntities) {
		TypeQueryTerm typeQueryTerm = new TypeQueryTerm(linkHeaders);
		TypeQueryTerm currentTypeQuery = typeQueryTerm;
		Map<Set<String>, Set<String>> types2EntityIdsForDB = Maps.newHashMap();
		// List<Uni<>>
		List<Uni<Tuple2<List<Map<String, Object>>, QueryRemoteHost>>> unis = Lists.newArrayList();
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
				Map<String, Tuple2<Map<String, Object>, Set<String>>> cacheIds = fullEntityCache
						.getAllIds2EntityAndHosts();
				// todo
//						fullEntityCache
//						.getByType(type);
				if (cacheIds == null) {
					cacheIds = new HashMap<>(0);
				}
				for (String id : entityIds) {
					if (cacheIds.containsKey(id)) {
						Tuple2<Map<String, Object>, Set<String>> entityAndHosts = cacheIds.get(id);
						Set<String> hostName = entityAndHosts.getItem2();
						if (hostName == null) {
							idsForDB.add(id);
						} else {
							if (!hostName.contains(NGSIConstants.JSON_LD_NONE)) {
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
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypes = new ArrayList<>(1);
			idsAndTypes.add(Tuple3.of(entityIds, typeQueryTerm, null));
			Collection<QueryRemoteHost> remoteQueries = EntityTools.getRemoteQueries(idsAndTypes, null, linkedQ, null,
					null, null, tenant2CId2RegEntries.row(tenant).values(), linkHeaders, fullEntityCache, viaHeaders,
					splitEntities);
			for (QueryRemoteHost remoteQuery : remoteQueries) {

				unis.add(EntityTools.getRemoteEntities(remoteQuery, webClient, timeout, ldService).onItem()
						.transform(l -> Tuple3.of(l, remoteQuery)));
			}

		}
		if (!types2EntityIdsForDB.isEmpty()) {
			unis.add(0, queryDAO.getEntities(tenant, types2EntityIdsForDB, linkedQ).onItem()
					.transform(entities -> Tuple2.of(entities, AppConstants.DB_REMOTE_HOST)));
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().item(Tuple2.of(fullEntityCache, Lists.newArrayList()));
		}
		return Uni.combine().all().unis(unis).with(l -> {
			Collection<Map<String, Object>> result = mergeMultipleQueryResults(l, fullEntityCache, null);
			return Tuple2.of(fullEntityCache, result);
		});
	}

	public Uni<Void> deleteEntityMap(String tenant, String entityMapId) {

		return queryDAO.deleteEntityMap(tenant, entityMapId);
	}

	public Uni<Void> updateEntityMap(String tenant, String entityMapId, String expiresAt) {
		return queryDAO.updateEntityMap(tenant, entityMapId, expiresAt);
	}
}
