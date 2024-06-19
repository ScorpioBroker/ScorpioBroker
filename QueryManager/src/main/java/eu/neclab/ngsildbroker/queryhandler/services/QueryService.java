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
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
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
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
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
import static eu.neclab.ngsildbroker.commons.tools.HttpUtils.parseLinkHeaderNoUni;

@ApplicationScoped
public class QueryService {

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

	public Uni<QueryResult> query(String tenant, String qToken, boolean tokenProvided, String[] id,
			TypeQueryTerm typeQuery, String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf,
			GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, int limit, int offSet,
			boolean count, boolean localOnly, Context context, io.vertx.core.MultiMap headersFromReq,
			boolean doNotCompact,Set<String> jsonKeys, DataSetIdTerm dataSetIdTerm) {
		if (localOnly) {
			return localQueryLevel1(tenant, id, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery,
					langQuery, limit, offSet, count,dataSetIdTerm);
		}
		if (!tokenProvided) {
			return getAndStoreEntityIdList(tenant, id, idPattern, qToken, typeQuery, attrsQuery, geoQuery, qQuery,
					scopeQuery, langQuery, limit, offSet, context, headersFromReq, doNotCompact,dataSetIdTerm).onItem()
					.transformToUni(t -> {
						Map<String, Map<String, Object>> localResults = t.getItem1();
						EntityMap entityMap = t.getItem2();
						List<EntityMapEntry> resultEntityMap = entityMap.getSubMap(offSet, limit + offSet);
						Long resultCount = (long) entityMap.size();
						return handleEntityMap(resultCount, resultEntityMap, attrsQuery, localResults, count, limit,
								offSet, qToken, headersFromReq,qQuery,jsonKeys);
					});
		} else {
			return queryDAO.getEntityMap(tenant, qToken, limit, offSet, count).onItem().transformToUni(t -> {
				Long resultCount = t.getItem1();
				EntityMap entityMap = t.getItem2();
				Map<String, Map<String, Object>> localResults = t.getItem3();
				return handleEntityMap(resultCount, entityMap.getEntityList(), attrsQuery, localResults, count, limit,
						offSet, qToken, headersFromReq,qQuery,jsonKeys);

			});
		}
	}

	private Uni<QueryResult> handleEntityMap(Long resultCount, List<EntityMapEntry> resultEntityMap,
			AttrsQueryTerm attrsQuery, Map<String, Map<String, Object>> localResults, boolean count, int limit,
			int offSet, String qToken, io.vertx.core.MultiMap headersFromReq, QQueryTerm queryTerm,Set<String> jsonKeys) {
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
				MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
				String idList = String.join(",", entry.getValue());
				logger.debug("calling: " + remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "?id=" + idList
						+ "&options=sysAttrs&limit=1000");

				unis.add(webClient
						.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "?id=" + idList
								+ "&options=sysAttrs&limit=1000")
						.putHeaders(toFrwd).putHeader("Link", linkHead).send().onItem()
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
										contextLinks,headersFromReq);
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
				if(entityId2OldestCreatedAt.get(entityId)!=null) {
					entity.put(NGSIConstants.NGSI_LD_CREATED_AT,
							List.of(Map.of(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME,
									NGSIConstants.JSON_LD_VALUE,
									SerializationTools.toDateTimeString(entityId2OldestCreatedAt.get(entityId)))));
				}
				if(entityId2YoungestModified.get(entityId)!=null) {
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
						if (queryTerm.calculate(EntityTools.getBaseProperties(entity)) || queryTerm.calculate(entity,jsonKeys)) {
							resultData.add(entity);
						}
					} catch (Exception e) {
						// Handling any exceptions occurred during calculation
						if (queryTerm.calculate(entity,jsonKeys)) {
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
															String idsString, List<Object> contextLinks, io.vertx.core.MultiMap headersFromReq) {
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
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
			backUpUnis.add(webClient
					.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "?id=" + toUseIds
							+ "&options=sysAttrs&limit=1000")
					.putHeaders(toFrwd).putHeader("Link", linkHead).send().onItem()
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
						.putHeaders(toFrwd).putHeader("Link", linkHead).send().onItem()
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

	private List<QueryRemoteHost> getRemoteQueries(String tenant, String[] id, TypeQueryTerm typeQuery,
			String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, Context context) {

		Iterator<List<RegistrationEntry>> it = tenant2CId2RegEntries.row(tenant).values().iterator();
		// ids, types, attrs, geo, scope
		Map<QueryRemoteHost, QueryInfos> remoteHost2QueryInfo = Maps.newHashMap();
		while (it.hasNext()) {
			Iterator<RegistrationEntry> tenantRegs = it.next().iterator();
			while (tenantRegs.hasNext()) {

				RegistrationEntry regEntry = tenantRegs.next();
				if (regEntry.expiresAt() > 0 && regEntry.expiresAt() <= System.currentTimeMillis()) {
					it.remove();
					continue;
				}
				if (!regEntry.queryBatch() && !regEntry.queryEntity()) {
					continue;
				}

				if (regEntry.matches(id, idPattern, typeQuery, attrsQuery, qQuery, geoQuery, scopeQuery) == null) {
					continue;
				}

				RemoteHost regHost = regEntry.host();
				QueryRemoteHost hostToQuery = QueryRemoteHost.fromRemoteHost(regHost, null, regEntry.canDoIdQuery(),
						regEntry.canDoZip(), null);
				QueryInfos queryInfos = remoteHost2QueryInfo.get(hostToQuery);
				if (queryInfos == null) {
					queryInfos = new QueryInfos();
					remoteHost2QueryInfo.put(hostToQuery, queryInfos);
				}

				if (!queryInfos.isFullIdFound()) {
					if (regEntry.eId() != null) {
						queryInfos.getIds().add(regEntry.eId());
					} else {
						if (id != null) {
							queryInfos.setIds(Sets.newHashSet(id));
							queryInfos.setFullIdFound(true);
						} else if (idPattern != null) {
							queryInfos.setIdPattern(idPattern);
						}
					}
				}
				if (!queryInfos.isFullTypesFound()) {
					if (regEntry.type() != null) {
						queryInfos.getTypes().add(regEntry.type());
					} else {
						if (typeQuery != null) {
							queryInfos.setTypes(typeQuery.getAllTypes());
							queryInfos.setFullTypesFound(true);
						}
					}
				}
				if (!queryInfos.isFullAttrsFound()) {
					if (regEntry.eProp() != null) {
						queryInfos.getAttrs().add(regEntry.eProp());
					} else if (regEntry.eRel() != null) {
						queryInfos.getAttrs().add(regEntry.eRel());
					} else {
						queryInfos.setFullAttrsFound(true);
						if (attrsQuery != null && attrsQuery.getAttrs() != null && !attrsQuery.getAttrs().isEmpty()) {
							queryInfos.setAttrs(attrsQuery.getAttrs());
						}
					}
				}
			}

		}
		List<QueryRemoteHost> result = new ArrayList<>(remoteHost2QueryInfo.size());
		for (Entry<QueryRemoteHost, QueryInfos> entry : remoteHost2QueryInfo.entrySet()) {
			QueryRemoteHost tmpHost = entry.getKey();
			String queryString = entry.getValue().toQueryString(context, typeQuery, geoQuery, langQuery, false);
			result.add(new QueryRemoteHost(tmpHost.host(), tmpHost.tenant(), tmpHost.headers(), tmpHost.cSourceId(),
					tmpHost.canDoSingleOp(), tmpHost.canDoBatchOp(), tmpHost.regMode(), queryString,
					tmpHost.canDoIdQuery(), tmpHost.canDoZip(), tmpHost.remoteToken()));
		}
		return result;
	}

	private Uni<QueryResult> localQueryLevel1(String tenant, String[] id, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery,
			LanguageQueryTerm langQuery, int limit, int offSet, boolean count, DataSetIdTerm dataSetIdTerm) {
		return queryDAO.queryLocalOnly(tenant, id, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery,
				langQuery, limit, offSet, count, dataSetIdTerm).onItem().transform(rows -> {
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

	public Uni<List<Map<String, Object>>> getTypesWithDetail(String tenant, boolean localOnly,io.vertx.core.MultiMap headersFromReq) {
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
								NGSIConstants.NGSI_LD_TYPES_ENDPOINT + "?details=true",headersFromReq);
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

	public Uni<Map<String, Object>> getTypes(String tenant, boolean localOnly,io.vertx.core.MultiMap headersFromReq) {
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
						List<Uni<List<Object>>> unis = getRemoteCalls(rows, NGSIConstants.NGSI_LD_TYPES_ENDPOINT,headersFromReq);
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

	public Uni<Map<String, Object>> getType(String tenant, String type, boolean localOnly,io.vertx.core.MultiMap headersFromReq) {
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
								NGSIConstants.NGSI_LD_TYPES_ENDPOINT + "/" + type,headersFromReq);
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

	public Uni<List<Map<String, Object>>> getAttribsWithDetails(String tenant, boolean localOnly,io.vertx.core.MultiMap headersFromReq) {
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
								NGSIConstants.NGSI_LD_ATTRIBUTES_ENDPOINT + "?details=true",headersFromReq);
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

	public Uni<Map<String, Object>> getAttribs(String tenant, boolean localOnly,io.vertx.core.MultiMap headersFromReq) {
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
								NGSIConstants.NGSI_LD_ATTRIBUTES_ENDPOINT,headersFromReq);
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

	public Uni<Map<String, Object>> getAttrib(String tenant, String attrib, boolean localOnly,io.vertx.core.MultiMap headersFromReq) {
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
								NGSIConstants.NGSI_LD_ATTRIBUTES_ENDPOINT + "/" + attrib,headersFromReq);
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

	private List<Uni<List<Object>>> getRemoteCalls(RowSet<Row> rows, String endpoint,io.vertx.core.MultiMap headersFromReq) {
		List<Uni<List<Object>>> unis = Lists.newArrayList();
		rows.forEach(row -> {
			// C.endpoint C.tenant_id, c.headers, c.reg_mode
			String host = row.getString(0);
			MultiMap remoteHeaders = MultiMap
					.newInstance(HttpUtils.getHeadersForRemoteCall(row.getJsonArray(2), row.getString(1)));
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHeaders,headersFromReq);

			unis.add(webClient.getAbs(host + endpoint).putHeaders(toFrwd).send().onFailure().recoverWithNull()
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
												   int joinLevel, io.vertx.core.MultiMap headersFromReq) {
		AttrsQueryTerm attrsQuery;
		try {
			attrsQuery = QueryParser.parseAttrs(attrs, context);
		} catch (ResponseException e) {
			throw new RuntimeException(e);
		}
		Map<String, Object> attrsMap = QueryParser.parseInput(attrs);
		Uni<Map<String, Object>> entity;
		if (idsOnly) {
			entity = idsOnly(getEntityFlat(context,tenant,entityId,attrsMap,lang,localOnly,containedBy,joinLevel,null,headersFromReq));
		} else if (join == null) {
			entity = retrieveEntity(context,tenant,entityId,attrsQuery,lang,localOnly,headersFromReq);
		} else if (join.equals(NGSIConstants.FLAT)) {
			entity = getEntityFlat(context,tenant,entityId,attrsMap,lang,localOnly,containedBy,joinLevel,null,headersFromReq);
		} else if (join.equals(NGSIConstants.INLINE)) {
			entity = getEntityInline(context,tenant,entityId,attrsMap,lang,localOnly,containedBy,joinLevel,headersFromReq);
		} else {
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData));
		}
        return entity;
    }
	public Uni<Map<String, Object>> retrieveEntity(Context context, String tenant, String entityId, AttrsQueryTerm attrsQuery,
			LanguageQueryTerm lang, boolean localOnly, io.vertx.core.MultiMap headersFromReq) {
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
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);

			unis.add(webClient
					.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId + queryString)
					.putHeaders(toFrwd).send().onItem().transformToUni(response -> {
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

	public Uni<List<String>> queryForEntityIds(String tenant, String[] ids, TypeQueryTerm typeQueryTerm,
			String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQueryTerm, GeoQueryTerm geoQueryTerm,
			ScopeQueryTerm scopeQueryTerm, LanguageQueryTerm queryTerm, Context context,
			io.vertx.core.MultiMap headersFromReq, boolean idsOnly, String join, String containedBy, int joinLevel,
			boolean doNotCompact,DataSetIdTerm dataSetIdTerm) {
		return getAndStoreEntityIdList(tenant, ids, idPattern, idPattern, typeQueryTerm, attrsQuery, geoQueryTerm,
				qQueryTerm, scopeQueryTerm, queryTerm, 0, 0, context, headersFromReq, doNotCompact,dataSetIdTerm).onItem()
				.transform(t -> {
					// List<Map<String, Object>> localResults = t.getItem1();

					List<String> result = Lists.newArrayList();
					EntityMap entityMap = t.getItem2();
					for (EntityMapEntry entry : entityMap.getEntityList()) {
						result.add(entry.getEntityId());
					}
					return result;

				});
	}

	private Uni<Tuple2<Map<String, Map<String, Object>>, EntityMap>> getAndStoreEntityIdList(String tenant, String[] id,
			String idPattern, String qToken, TypeQueryTerm typeQuery, AttrsQueryTerm attrsQuery, GeoQueryTerm geoQuery,
			QQueryTerm qQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, int limit, int offset,
			Context context, io.vertx.core.MultiMap headersFromReq, boolean doNotCompact, DataSetIdTerm dataSetIdTerm) {
		Uni<Tuple2<Map<String, Map<String, Object>>, List<String>>> localIds = queryDAO.queryForEntityIds(tenant, id,
				typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, context, limit, offset, dataSetIdTerm);
		List<QueryRemoteHost> remoteHost2Query = getRemoteQueries(tenant, id, typeQuery, idPattern, attrsQuery, qQuery,
				geoQuery, scopeQuery, langQuery, context);
		Uni<Map<QueryRemoteHost, List<String>>> remoteIds;
		if (remoteHost2Query.isEmpty()) {
			remoteIds = Uni.createFrom().item(Maps.newHashMap());
		} else {
			List<Uni<Tuple2<QueryRemoteHost, List<String>>>> unis = Lists.newArrayList();
			for (QueryRemoteHost remoteHost : remoteHost2Query) {
				MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
				String linkHead;
				if (!remoteHost.headers().contains("Link") && headersFromReq.contains("Link")) {
					linkHead = headersFromReq.get("Link");
				} else if (remoteHost.headers().contains("Link")) {
					linkHead = remoteHost.headers().get("Link");
				} else {
					linkHead = "";
				}
				if (remoteHost.canDoIdQuery()) {
					String entityMapString;
					if (remoteHost.canDoZip()) {
						entityMapString = "&entityMap=true";
						// TODO add headers to support compress
					} else {
						entityMapString = "&entityMap=true";
					}
					unis.add(webClient
							.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT
									+ remoteHost.queryString() + entityMapString)
							.putHeaders(toFrwd).putHeader("Link", linkHead).timeout(timeout).send()
							.onItem().transformToUni(response -> {
								List<String> result;
								if (response != null && response.statusCode() == 200) {
									result = response.bodyAsJsonArray().getList();
								} else {
									logger.warn("Failed to query entity list from remote host" + remoteHost.toString());
									logger.warn("Attempting split");
									// request to long split it up
									if (response != null && response.statusCode() == 414) {
										int baseLength = (remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT
												+ entityMapString).length();
										return handle414IdQuery(baseLength, remoteHost, linkHead, true,headersFromReq);
									}
									if (response != null) {
										logger.warn("status code: " + response.statusCode());
									}
									result = Lists.newArrayList();
								}
								logger.debug("retrieved entity list: " + result.toString());
								logger.debug("from remote host: " + remoteHost.host()
										+ NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + remoteHost.queryString()
										+ entityMapString);
								return Uni.createFrom().item(Tuple2.of(remoteHost, result));
							}).onFailure().recoverWithItem(e -> {
								logger.warn("Failed to query entity list from remote host" + remoteHost.toString());
								return null;
							}));
				} else {
					unis.add(webClient
							.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT
									+ remoteHost.queryString() + "&limit=1000")
							.putHeaders(toFrwd).putHeader("Link", linkHead).timeout(timeout).send()
							.onItem().transformToUni(response -> {
								List<String> result = Lists.newArrayList();
								if (response != null && response.statusCode() == 200) {
									List tmpList = response.bodyAsJsonArray().getList();
									for (Object obj : tmpList) {
										result.add((String) ((Map<String, Object>) obj).get(NGSIConstants.ID));
									}
									logger.debug("retrieved entity list: " + result.toString());
									logger.debug("from remote host: " + remoteHost.host()
											+ NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + remoteHost.queryString()
											+ "&limit=1000");
									return Uni.createFrom().item(Tuple2.of(remoteHost, result));
								} else {
									logger.warn("Failed to query entity list from remote host" + remoteHost.toString());
									logger.warn("Attempting split");
									// request to long split it up
									if (response != null && response.statusCode() == 414) {
										int baseLength = (remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT
												+ "&limit=1000").length();
										return handle414IdQuery(baseLength, remoteHost, linkHead, false,headersFromReq);
									}
									if (response != null) {
										logger.warn("status code: " + response.statusCode());
									}
									return Uni.createFrom().item(Tuple2.of(remoteHost, result));
								}

							}).onFailure().recoverWithItem(e -> {
								logger.warn("Failed to query remote host" + remoteHost.toString());
								return null;
							}));
				}
			}
			remoteIds = Uni.combine().all().unis(unis).combinedWith(list -> {
				Map<QueryRemoteHost, List<String>> result = Maps.newHashMap();
				for (Object obj : list) {
					if (obj != null) {
						Tuple2<QueryRemoteHost, List<String>> tuple = (Tuple2<QueryRemoteHost, List<String>>) obj;
						result.put(tuple.getItem1(), tuple.getItem2());
					}
				}
				return result;
			});

		}

		return Uni.combine().all().unis(localIds, remoteIds).asTuple().onItem().transform(t -> {
			EntityMap result = new EntityMap();
			Tuple2<Map<String, Map<String, Object>>, List<String>> local = t.getItem1();
			Map<QueryRemoteHost, List<String>> remote = t.getItem2();
			for (String entry : local.getItem2()) {
				result.getEntry(entry).getRemoteHosts()
						.add(new QueryRemoteHost(null, null, null, null, false, false, -1, null, false, false, null));
			}
			for (Entry<QueryRemoteHost, List<String>> entry : remote.entrySet()) {
				for (String entityId : entry.getValue()) {
					result.getEntry(entityId).getRemoteHosts().add(entry.getKey());
				}
			}
			return Tuple2.of(result, local.getItem1());
		}).onItem().transform(tuple -> {
			EntityMap entityMap = tuple.getItem1();
			Map<String, Map<String, Object>> localResults = tuple.getItem2();
			if (!doNotCompact) {
				queryDAO.storeEntityMap(tenant, qToken, entityMap).subscribe().with(t -> {
					logger.debug("Stored entity map " + qToken);
				});
			}
			// vertx.executeBlockingAndForget(queryDAO.storeEntityMap(tenant, qToken,
			// entityMap));
			// return queryDAO.storeEntityMap(tenant, qToken,
			// entityMap).onItem().transform(v -> {
			return Tuple2.of(localResults, entityMap);
			// });
		});

	}

	private Uni<Tuple2<QueryRemoteHost, List<String>>> handle414IdQuery(int baseLength, QueryRemoteHost remoteHost,
																		String linkHead, boolean entityMap, io.vertx.core.MultiMap headersFromReq) {
		int start = remoteHost.queryString().indexOf("id=");
		MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
		if (start == -1) {
			logger.warn("failed to split up query");
			return Uni.createFrom().item(Tuple2.of(remoteHost, Lists.newArrayList()));
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
			return Uni.createFrom().item(Tuple2.of(remoteHost, Lists.newArrayList()));
		}
		int index;
		List<Uni<Tuple2<QueryRemoteHost, List<String>>>> backUpUnis = Lists.newArrayList();
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
					.putHeaders(toFrwd).putHeader("Link", linkHead).timeout(timeout).send().onItem()
					.transformToUni(backupResponse -> {
						List<String> backupResult = Lists.newArrayList();
						if (backupResponse != null && backupResponse.statusCode() == 200) {
							List tmpList = backupResponse.bodyAsJsonArray().getList();
							for (Object obj : tmpList) {
								backupResult.add((String) ((Map<String, Object>) obj).get(NGSIConstants.ID));
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
						.putHeaders(toFrwd).putHeader("Link", linkHead).timeout(timeout).send().onItem()
						.transformToUni(backupResponse -> {
							List<String> backupResult = Lists.newArrayList();
							if (backupResponse != null && backupResponse.statusCode() == 200) {
								List tmpList = backupResponse.bodyAsJsonArray().getList();
								for (Object obj : tmpList) {
									backupResult.add((String) ((Map<String, Object>) obj).get(NGSIConstants.ID));
								}
								logger.debug("retrieved entity list: " + backupResult.toString());
							}
							return Uni.createFrom().item(Tuple2.of(remoteHost, backupResult));
						}).onFailure().recoverWithItem(e1 -> null));
				idsString = "";
			}
		}
		return Uni.combine().all().unis(backUpUnis).combinedWith(l1 -> {
			List<String> allIds = Lists.newArrayList();
			QueryRemoteHost qrh = null;
			for (Object obj1 : l1) {
				Tuple2<QueryRemoteHost, List<String>> t1 = (Tuple2<QueryRemoteHost, List<String>>) obj1;
				qrh = t1.getItem1();
				allIds.addAll(t1.getItem2());
			}
			return Tuple2.of(qrh, allIds);
		});

	}

	@Scheduled(every = "${scorpio.entitymap.cleanup.schedule}", delayed = "${scorpio.startupdelay}")
	public Uni<Void> scheduleEntityMapCleanUp() {
		return queryDAO.runEntityMapCleanup(entityMapTTL);
	}

	public Uni<Map<String, Object>> getEntityFlat(Context context, String tenant, String entityId, Map<String, Object> attrsMap,
												  LanguageQueryTerm lang, boolean localOnly, String containedBy,
												  int joinLevel, List<Map<String, Object>> relResult,io.vertx.core.MultiMap headersFromReq) {
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
		return retrieveEntity(context, tenant, entityId, attrsQuery, lang, localOnly,headersFromReq)
				.onItem().transformToUni(ent -> {
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
					if(((Map<String, Object>)attrib.get(0)).containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST)){
						for(Map<String,Object> idsMap: ((Map<String, List<Map<String, List<Map<String,Object>>>>>)attrib.get(0)).get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST).get(0).get(JsonLdConsts.LIST)){
							unisOfMaps.add(getEntityFlat(context,tenant, (String) idsMap.get(JsonLdConsts.VALUE),finalAttrs,lang,localOnly,
									containedBy + ent.get(JsonLdConsts.ID),joinLvl.get(),finalRelResult,headersFromReq));
						}
					}
					else {
						for (Map<String, Object> idMap : ((Map<String, List<Map<String, Object>>>) attrib.get(0))
								.get(NGSIConstants.NGSI_LD_HAS_OBJECT)) {
							unisOfMaps.add(getEntityFlat(context, tenant, (String) idMap.get(JsonLdConsts.ID), finalAttrs, lang, localOnly,
									containedBy + ent.get(JsonLdConsts.ID), joinLvl.get(), finalRelResult,headersFromReq));
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

public Uni<Map<String, Object>> getEntityInline(Context context, String tenant, String entityId, Map<String, Object> attrsMap,
												LanguageQueryTerm lang, boolean localOnly, String containedBy,
												int joinLevel, io.vertx.core.MultiMap headersFromReq) {
		AttrsQueryTerm attrsQuery = null;
		if (attrsMap != null && !attrsMap.isEmpty()) {
			try {
				attrsQuery = QueryParser.parseAttrs(String.join(",", attrsMap.keySet()), context);
			} catch (ResponseException e) {
				throw new RuntimeException(e);
			}
		}
		AtomicInteger joinLvl = new AtomicInteger(joinLevel);
		return retrieveEntity(context,tenant,entityId,attrsQuery,lang,localOnly,headersFromReq).onItem().transformToUni(ent -> {
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
                    if(((Map<String, Object>)attrib.get(0)).containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST)){
                        for(Map<String,Object> idsMap: ((Map<String, List<Map<String, List<Map<String,Object>>>>>)attrib.get(0)).get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST).get(0).get(JsonLdConsts.LIST)){
							unisOfMaps.add(getEntityInline(context, tenant, (String) idsMap.get(JsonLdConsts.VALUE), finalAttrsMap, lang, localOnly,
									containedBy + ent.get(JsonLdConsts.ID), joinLvl.get(),headersFromReq).onItem()
									.transform(map -> {
										flatResult.add(map);
										((List<Map<String, Object>>) ent.get(key)).get(0).put("entity", flatResult);
										return ent;
									}));
						}

					}
					else {
                        for(Map<String,Object> idMap:((Map<String, List<Map<String, Object>>>) attrib.get(0))
								.get(NGSIConstants.NGSI_LD_HAS_OBJECT)){
							unisOfMaps.add(getEntityInline(context, tenant, (String) idMap.get(JsonLdConsts.ID), finalAttrsMap, lang, localOnly,
									containedBy + ent.get(JsonLdConsts.ID), joinLvl.get(),headersFromReq).onItem()
									.transform(map -> {
										flatResult.add(map);
										((List<Map<String, Object>>) ent.get(key)).get(0).put("entity", flatResult);
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

}
