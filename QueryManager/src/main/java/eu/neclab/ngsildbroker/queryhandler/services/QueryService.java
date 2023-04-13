package eu.neclab.ngsildbroker.queryhandler.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
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
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.SqlConnection;

@ApplicationScoped
public class QueryService {

	private static Logger logger = LoggerFactory.getLogger(QueryService.class);

	@Inject
	QueryDAO queryDAO;

	@Inject
	Vertx vertx;

	private WebClient webClient;

	protected JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
	private Random random = new Random();
	private Table<String, String, RegistrationEntry> tenant2CId2RegEntries = HashBasedTable.create();

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
			boolean count, boolean localOnly, Context context) {
		if (localOnly) {
			return localQueryLevel1(tenant, id, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery,
					langQuery, limit, offSet, count);
		}
		if (!tokenProvided) {
			return getAndStoreEntityIdList(tenant, id, idPattern, qToken, typeQuery, attrsQuery, geoQuery, qQuery,
					scopeQuery, context).onItem().transformToUni(t -> {
						SqlConnection conn = t.getItem1();
						List<Tuple2<String, List<QueryRemoteHost>>> entityMap = t.getItem2();

						List<Tuple2<String, List<QueryRemoteHost>>> resultEntityMap = entityMap.subList(limit,
								limit + offSet);
						Long resultCount = (long) entityMap.size();
						return handleEntityMap(resultCount, resultEntityMap, attrsQuery, conn, count, limit, offSet);
					});
		} else {
			return queryDAO.getEntityMap(tenant, qToken, limit, offSet, count).onItem().transformToUni(t -> {
				SqlConnection conn = t.getItem1();
				Long resultCount = t.getItem2();
				List<Tuple2<String, List<QueryRemoteHost>>> entityMap = t.getItem3();
				return handleEntityMap(resultCount, entityMap, attrsQuery, conn, count, limit, offSet);

			});
		}
	}

	private Set<QueryRemoteHost> getRemoteQueries(String tenant, String[] id, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery,
			Context context) {
		// TODO Auto-generated method stub
		return null;
	}

	private Uni<QueryResult> handleEntityMap(Long resultCount,
			List<Tuple2<String, List<QueryRemoteHost>>> resultEntityMap, AttrsQueryTerm attrsQuery, SqlConnection conn,
			boolean count, int limit, int offSet) {
		Map<QueryRemoteHost, List<String>> remoteHost2EntityIds = Maps.newHashMap();
		// has to be linked. We want to keep order here
		Map<String, Map<String, Object>> resultEntityId2Entity = Maps.newLinkedHashMap();
		for (Tuple2<String, List<QueryRemoteHost>> entry : resultEntityMap) {
			List<QueryRemoteHost> remoteHosts = entry.getItem2();
			for (QueryRemoteHost remoteHost : remoteHosts) {
				List<String> tmp = remoteHost2EntityIds.get(remoteHost);
				if (tmp == null) {
					tmp = Lists.newArrayList();
					remoteHost2EntityIds.put(remoteHost, tmp);
				}
				tmp.add(entry.getItem1());
			}
			resultEntityId2Entity.put(entry.getItem1(), new HashMap<>(0));
		}
		List<Uni<Map<String, Map<String, Object>>>> unis = Lists.newArrayList();
		for (Entry<QueryRemoteHost, List<String>> entry : remoteHost2EntityIds.entrySet()) {
			QueryRemoteHost remoteHost = entry.getKey();
			if (remoteHost.isLocal()) {
				unis.add(queryDAO.getEntities(conn, entry.getValue(), attrsQuery));
			} else {

				unis.add(webClient
						.get(remoteHost.host() + "/" + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT
								+ remoteHost.queryString())
						.putHeaders(remoteHost.headers()).send().onItem().transform(response -> {
							Map<String, Map<String, Object>> result = Maps.newHashMap();
							if (response != null && response.statusCode() == 200) {
								List<Object> expanded;
								try {
									expanded = JsonLdProcessor.expand(response.bodyAsJsonArray().getList());
									expanded.forEach(obj -> {
										Map<String, Object> entity = (Map<String, Object>) obj;
										result.put((String) entity.get(NGSIConstants.JSON_LD_ID), entity);
									});
								} catch (JsonLdError | ResponseException e) {
									e.printStackTrace();
								}

							}
							return result;
						}));
			}
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			QueryResult result = new QueryResult();
			for (Object obj : list) {
				Map<String, Map<String, Object>> entityId2Entity = (Map<String, Map<String, Object>>) obj;
				for (Entry<String, Map<String, Object>> entry : entityId2Entity.entrySet()) {
					mergeEntity(resultEntityId2Entity, entry.getKey(), entry.getValue());
				}
			}
			List<Map<String, Object>> resultData = Lists.newArrayList(resultEntityId2Entity.values());
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

			return result;
		});

	}

	private void mergeEntity(Map<String, Map<String, Object>> resultEntityId2Entity, String key,
			Map<String, Object> value) {
		// TODO Auto-generated method stub

	}

	private Set<QueryRemoteHost> getRemoteQueriesForIds(String tenant, String[] id, TypeQueryTerm typeQuery,
			String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, Context context) {
		// TODO Auto-generated method stub
		return null;
	}

	private List<Uni<QueryResult>> getRemoteQueries(String tenant, String[] id, TypeQueryTerm typeQuery,
			String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, int limit, int offSet, boolean count) {
		List<RemoteHost> result = Lists.newArrayList();
		Iterator<RegistrationEntry> it = tenant2CId2RegEntries.row(tenant).values().iterator();
		// ids, types, attrs, geo, scope
		Map<RemoteHost, QueryInfos> remoteHost2QueryInfo = Maps.newHashMap();
		while (it.hasNext()) {
			RegistrationEntry regEntry = it.next();
			if (regEntry.expiresAt() > System.currentTimeMillis()) {
				it.remove();
				continue;
			}
			if (!regEntry.queryBatch() || !regEntry.queryEntity()) {
				continue;
			}

			if (!regEntry.matches(id, idPattern, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery)) {
				continue;
			}
			RemoteHost regHost = regEntry.host();
			RemoteHost hostToQuery = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
					regHost.cSourceId(), regEntry.queryEntity(), regEntry.queryBatch(), regEntry.regMode());
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
					queryInfos.setAttrs(attrsQuery.getAttrs());
				}
			}

		}

		return null;
	}

	private void mergeQueryResults(QueryResult queryResult, QueryResult toMerge) {

	}

	private Uni<QueryResult> localQueryLevel1(String tenant, String[] id, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery,
			LanguageQueryTerm langQuery, int limit, int offSet, boolean count) {
		return queryDAO.queryLocalOnly(tenant, id, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery,
				langQuery, limit, offSet, count).onItem().transform(rows -> {
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

			unis.add(webClient.get(host + endpoint).putHeaders(remoteHeaders).send().onFailure().recoverWithNull()
					.onItem().transformToUni(response -> {
						String responseTypes;
						if (response == null || response.statusCode() != 200) {
							return Uni.createFrom().item(Lists.newArrayList());
						} else {
							responseTypes = response.bodyAsString();
							try {
								return Uni.createFrom()
										.item(JsonLdProcessor.expand(HttpUtils.getContextFromHeader(remoteHeaders),
												JsonUtils.fromString(responseTypes), opts, -1, false));
							} catch (Exception e) {
								logger.debug("failed to handle response from host " + host + " : " + responseTypes, e);
								return Uni.createFrom().item(Lists.newArrayList());
							}
						}

					}).onFailure().recoverWithItem(e -> Lists.newArrayList()));
		});
		return unis;
	}

	public Uni<Map<String, Object>> retrieveEntity(Context context, String tenant, String entityId,
			AttrsQueryTerm attrsQuery, LanguageQueryTerm lang, boolean localOnly) {
		Uni<Map<String, Object>> getEntity = queryDAO.getEntity(entityId, tenant, attrsQuery);
		Uni<Map<String, Object>> getRemoteEntities;
		if (localOnly) {
			getRemoteEntities = Uni.createFrom().item(new HashMap<String, Object>(0));
		} else {
			getRemoteEntities = queryDAO.getRemoteSourcesForEntity(entityId, attrsQuery, tenant).onItem()
					.transformToUni(rows -> {
						List<Uni<Map<String, Object>>> tmp = Lists.newArrayList();
						// C.endpoint C.tenant_id, c.headers, c.reg_mode
						rows.forEach(row -> {

							StringBuilder url = new StringBuilder(
									row.getString(0) + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId);
							url.append("?");
							String[] callAttrs = row.getArrayOfStrings(4);
							// TODO remove the unneeded checks ... don't know how the db [null] will be
							// return
							if (callAttrs != null && callAttrs.length > 0 && callAttrs[0] != null) {
								url.append("attrs=");
								for (String callAttr : callAttrs) {
									url.append(context.compactIri(callAttr));
									url.append(',');
								}
								;
								url.setLength(url.length() - 1);
								url.append('&');
							} else {
								if (attrsQuery != null && !attrsQuery.getCompactedAttrs().isEmpty()) {
									url.append("attrs=" + String.join(",", attrsQuery.getCompactedAttrs()) + "&");
								}
							}
							if (lang != null) {
								url.append("lang=" + lang + "&");
							}
							url.append("options=sysAttrs");
							MultiMap remoteHeaders = MultiMap.newInstance(
									HttpUtils.getHeadersForRemoteCall(row.getJsonArray(2), row.getString(1)));
							tmp.add(webClient.get(url.toString()).putHeaders(remoteHeaders).send().onFailure()
									.recoverWithNull().onItem().transform(response -> {
										Map<String, Object> responseEntity;
										if (response == null || response.statusCode() != 200) {
											responseEntity = null;
										} else {
											responseEntity = response.bodyAsJsonObject().getMap();
											try {
												responseEntity = (Map<String, Object>) JsonLdProcessor
														.expand(HttpUtils.getContextFromHeader(remoteHeaders),
																responseEntity, opts, -1, false)
														.get(0);
											} catch (JsonLdError e) {
												// TODO Auto-generated catch block
												e.printStackTrace();
											} catch (ResponseException e) {
												// TODO Auto-generated catch block
												e.printStackTrace();
											}

											responseEntity.put(EntityTools.REG_MODE_KEY, row.getInteger(3));

										}
										return responseEntity;
									}));
						});
						if (tmp.isEmpty()) {
							return Uni.createFrom().item(Maps.newHashMap());
						}
						return Uni.combine().all().unis(tmp).combinedWith(list -> {
							Map<String, Object> result = Maps.newHashMap();
							for (Object entry : list) {
								if (entry == null) {
									continue;
								}
								Map<String, Object> entityMap = (Map<String, Object>) entry;
								int regMode = (int) entityMap.remove(EntityTools.REG_MODE_KEY);
								for (Entry<String, Object> attrib : entityMap.entrySet()) {
									String key = attrib.getKey();
									if (EntityTools.DO_NOT_MERGE_KEYS.contains(key)) {
										if (!result.containsKey(key)) {
											result.put(key, attrib.getValue());
										} else {
											if (key.equals(NGSIConstants.JSON_LD_TYPE)) {
												List<String> newType = (List<String>) attrib.getValue();
												List<String> currentType = (List<String>) result.get(key);
												if (!newType.equals(currentType)) {
													Set<String> tmpSet = Sets.newHashSet();
													tmpSet.addAll(newType);
													tmpSet.addAll(currentType);
													result.put(key, Lists.newArrayList(tmpSet));
												}
											}
										}
										continue;
									}
									Object currentValue = result.get(key);
									List<Map<String, Object>> newValue = (List<Map<String, Object>>) attrib.getValue();
									EntityTools.addRegModeToValue(newValue, regMode);
									if (currentValue == null) {
										result.put(key, newValue);
									} else {
										EntityTools.mergeValues((List<Map<String, Object>>) currentValue, newValue);
									}

								}

							}
							return result;
						}).onFailure().recoverWithItem(new HashMap<String, Object>());
					});
		}
		return Uni.combine().all().unis(getEntity, getRemoteEntities).asTuple().onItem().transformToUni(t -> {
			Map<String, Object> localEntity = t.getItem1();
			Map<String, Object> remoteEntity = t.getItem2();
			// if (attrs != null && !attrs.isEmpty()) {
			// EntityTools.removeAttrs(localEntity, attrs);
			// }
			if (localEntity.isEmpty() && remoteEntity.isEmpty()) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, entityId + " was not found"));
			}

			if (remoteEntity.isEmpty()) {
				return Uni.createFrom().item(localEntity);
			}
			if (localEntity.isEmpty()) {
				EntityTools.removeRegKey(remoteEntity);
				return Uni.createFrom().item(remoteEntity);
			}
			for (Entry<String, Object> attrib : remoteEntity.entrySet()) {
				String key = attrib.getKey();
				if (EntityTools.DO_NOT_MERGE_KEYS.contains(key)) {
					if (key.equals(NGSIConstants.JSON_LD_TYPE)) {
						List<String> newType = (List<String>) attrib.getValue();
						List<String> currentType = (List<String>) localEntity.get(key);
						if (!newType.equals(currentType)) {
							Set<String> tmpSet = Sets.newHashSet();
							tmpSet.addAll(newType);
							tmpSet.addAll(currentType);
							localEntity.put(key, Lists.newArrayList(tmpSet));
						}
					}
					continue;
				}
				Object currentValue = localEntity.get(key);
				List<Map<String, Object>> newValue = (List<Map<String, Object>>) attrib.getValue();
				if (currentValue == null) {
					localEntity.put(key, newValue);
				} else {
					EntityTools.mergeValues((List<Map<String, Object>>) currentValue, newValue);
				}

			}
			EntityTools.removeRegKey(localEntity);
			return Uni.createFrom().item(localEntity);

		});

	}

	public Uni<Void> handleRegistryChange(BaseRequest req) {
		tenant2CId2RegEntries.remove(req.getTenant(), req.getId());
		if (req.getRequestType() != AppConstants.DELETE_REQUEST) {
			for (RegistrationEntry regEntry : RegistrationEntry.fromRegPayload(req.getPayload())) {
				if (regEntry.retrieveEntity() || regEntry.queryEntity() || regEntry.queryBatch()) {
					tenant2CId2RegEntries.put(req.getTenant(), req.getId(), regEntry);
				}
			}
		}
		return Uni.createFrom().voidItem();
	}

	public Uni<List<String>> queryForEntityIds(String tenant, String[] ids, TypeQueryTerm typeQueryTerm,
			String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQueryTerm, GeoQueryTerm geoQueryTerm,
			ScopeQueryTerm scopeQueryTerm, Context context) {
		return getAndStoreEntityIdList(tenant, ids, idPattern, idPattern, typeQueryTerm, attrsQuery, geoQueryTerm,
				qQueryTerm, scopeQueryTerm, context).onItem().transformToUni(t -> {
					SqlConnection conn = t.getItem1();
					return conn.close().onItem().transform(v -> {
						List<String> result = Lists.newArrayList();
						List<Tuple2<String, List<QueryRemoteHost>>> entityMap = t.getItem2();
						for (Tuple2<String, List<QueryRemoteHost>> entry : entityMap) {
							result.add(entry.getItem1());
						}
						return result;
					});
				});
	}

	private Uni<Tuple2<SqlConnection, List<Tuple2<String, List<QueryRemoteHost>>>>> getAndStoreEntityIdList(
			String tenant, String[] id, String idPattern, String qToken, TypeQueryTerm typeQuery,
			AttrsQueryTerm attrsQuery, GeoQueryTerm geoQuery, QQueryTerm qQuery, ScopeQueryTerm scopeQuery,
			Context context) {
		Uni<List<String>> localIds = queryDAO.queryForEntityIds(tenant, id, typeQuery, idPattern, attrsQuery, qQuery,
				geoQuery, scopeQuery, context);
		Set<QueryRemoteHost> remoteHost2Query = getRemoteQueries(tenant, id, typeQuery, idPattern, attrsQuery, qQuery,
				geoQuery, scopeQuery, context);
		Uni<Map<QueryRemoteHost, List<String>>> remoteIds;
		if (remoteHost2Query.isEmpty()) {
			remoteIds = Uni.createFrom().item(Maps.newHashMap());
		} else {
			List<Uni<Tuple2<QueryRemoteHost, List<String>>>> unis = Lists.newArrayList();
			for (QueryRemoteHost remoteHost : remoteHost2Query) {
				unis.add(webClient
						.get(remoteHost.host() + "/" + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT
								+ remoteHost.queryString() + "&entityMap=true&zipEntityMap=true")
						.send().onItem().transform(response -> {
							List<String> result;
							if (response != null && response.statusCode() == 200) {
								result = response.bodyAsJsonArray().getList();
							} else {
								result = Lists.newArrayList();
							}
							return Tuple2.of(remoteHost, result);
						}));
			}
			remoteIds = Uni.combine().all().unis(unis).combinedWith(list -> {
				Map<QueryRemoteHost, List<String>> result = Maps.newHashMap();
				for (Object obj : list) {
					Tuple2<QueryRemoteHost, List<String>> tuple = (Tuple2<QueryRemoteHost, List<String>>) obj;
					result.put(tuple.getItem1(), tuple.getItem2());
				}
				return result;
			});

		}

		return Uni.combine().all().unis(localIds, remoteIds).asTuple().onItem().transform(t -> {
			List<Tuple2<String, List<QueryRemoteHost>>> result = Lists.newArrayList();

			return result;
		}).onItem().transformToUni(entityMap -> {
			if (entityMap.isEmpty()) {

				return Uni.createFrom().nullItem();
			}
			return queryDAO.storeEntityMap(tenant, qToken, entityMap).onItem().transform(conn -> {
				return Tuple2.of(conn, entityMap);
			});
		});

	}
}