package eu.neclab.ngsildbroker.queryhandler.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
import javax.inject.Singleton;

import org.locationtech.spatial4j.shape.Shape;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryInfos;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
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
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple5;
import io.smallrye.mutiny.tuples.Tuple6;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;

@ApplicationScoped
public class QueryService {

	private static List<String> ENTITY_TYPE_LIST_TYPE = Lists.newArrayList();
	{
		ENTITY_TYPE_LIST_TYPE.add(NGSIConstants.NGSI_LD_ENTITY_LIST);
	}
	private static List<String> ENTITY_TYPE_TYPE = Lists.newArrayList();
	{
		ENTITY_TYPE_TYPE.add(NGSIConstants.NGSI_LD_ENTITY_TYPE);
	}

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

	public Uni<QueryResult> query(String tenant, Set<String> id, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, int limit, int offSet, boolean count,
			boolean localOnly, Context context) {
		if (localOnly) {
			return localQueryLevel1(tenant, id, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery,
					langQuery, limit, offSet, count);
		} else {
			Uni<QueryResult> local = localQueryLevel1(tenant, id, typeQuery, idPattern, attrsQuery, qQuery, geoQuery,
					scopeQuery, langQuery, limit, offSet, count);
			List<Uni<QueryResult>> remoteQueries = getRemoteQueries(tenant, id, typeQuery, idPattern, attrsQuery,
					qQuery, geoQuery, scopeQuery, langQuery, limit, offSet, count);
			if (remoteQueries == null || remoteQueries.isEmpty()) {
				return local;
			} else {
				remoteQueries.add(local);
				return Uni.combine().all().unis(remoteQueries).combinedWith(list -> {
					QueryResult queryResult = new QueryResult();
					for (Object obj : list) {
						QueryResult tmp = (QueryResult) obj;
						mergeQueryResults(queryResult, tmp);
					}
					return queryResult;
				});
			}
		}
	}

	private List<Uni<QueryResult>> getRemoteQueries(String tenant, Set<String> id, TypeQueryTerm typeQuery,
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
						queryInfos.setIds(id);
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

	private Uni<QueryResult> localQueryLevel1(String tenant, Set<String> id, TypeQueryTerm typeQuery, String idPattern,
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
		Uni<Map<String, Set<String>>> queryRemoteTypes;

		if (localOnly) {
			Map<String, Object> tmp = Maps.newHashMap();
			tmp.put(NGSIConstants.NGSI_LD_TYPE_LIST, new ArrayList<Map<String, Object>>());
			queryRemoteTypes = Uni.createFrom().item(new HashMap<String, Set<String>>());
		} else {
			queryRemoteTypes = queryDAO.getRemoteSourcesForTypesWithDetails(tenant).onItem().transformToUni(rows -> {
				List<Uni<Map<String, Object>>> unis = Lists.newArrayList();
				rows.forEach(row -> {
					// C.endpoint C.tenant_id, c.headers, c.reg_mode
					MultiMap remoteHeaders = MultiMap
							.newInstance(HttpUtils.getHeadersForRemoteCall(row.getJsonArray(2), row.getString(1)));
					unis.add(webClient.get(row.getString(0) + NGSIConstants.NGSI_LD_TYPES_ENDPOINT)
							.putHeaders(remoteHeaders).send().onFailure().recoverWithNull().onItem()
							.transform(response -> {
								Map<String, Object> responseTypes;
								if (response == null || response.statusCode() != 200) {
									responseTypes = null;
								} else {
									responseTypes = response.bodyAsJsonObject().getMap();
									try {
										responseTypes = (Map<String, Object>) JsonLdProcessor
												.expand(HttpUtils.getContextFromHeader(remoteHeaders), responseTypes,
														opts, -1, false)
												.get(0);
									} catch (JsonLdError e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (ResponseException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
								return responseTypes;
							}));
				});
				return Uni.combine().all().unis(unis).combinedWith(list -> {
					Map<String, Set<String>> currentType2Attrib = Maps.newHashMap();
					for (Object entry : list) {
						if (entry == null) {
							continue;
						}
						List<Map<String, Object>> typeList = (List<Map<String, Object>>) entry;

						mergeTypeListWithDetails(typeList, currentType2Attrib);

					}
					return currentType2Attrib;
				});
			});
		}
		return Uni.combine().all().unis(queryRemoteTypes, queryDAO.getTypesWithDetails(tenant)).asTuple().onItem()
				.transform(t -> {
					List<Map<String, Object>> result = Lists.newArrayList();
					Map<String, Set<String>> currentType2Attr = t.getItem1();
					t.getItem2().forEach(row -> {
						// T.e_type, A.attr
						String type = row.getString(0);
						String attr = row.getString(1);
						Set<String> currentAttribs;
						if (currentType2Attr.containsKey(type)) {
							currentAttribs = currentType2Attr.get(type);
						} else {
							currentAttribs = Sets.newHashSet();
							currentType2Attr.put(type, currentAttribs);
						}
						currentAttribs.add(attr);
					});
					currentType2Attr.entrySet().forEach(entry -> {
						Map<String, Object> resultEntry = Maps.newHashMap();
						String type = entry.getKey();
						resultEntry.put(NGSIConstants.JSON_LD_ID, type);
						List<Map<String, String>> typeName = Lists.newArrayList();
						Map<String, String> typeEntry = Maps.newHashMap();
						typeEntry.put(NGSIConstants.JSON_LD_ID, type);
						resultEntry.put(NGSIConstants.NGSI_LD_TYPE_NAME, typeName);
						resultEntry.put(NGSIConstants.JSON_LD_TYPE, ENTITY_TYPE_TYPE);
						List<Map<String, String>> attribList = Lists.newArrayList();
						for (String attrib : entry.getValue()) {
							Map<String, String> attribValue = Maps.newHashMap();
							attribValue.put(NGSIConstants.JSON_LD_ID, attrib);
							attribList.add(attribValue);
						}
						resultEntry.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAMES, attribList);
						result.add(resultEntry);
					});
					return result;
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
		Uni<Set<String>> queryRemoteTypes;

		if (localOnly) {
			queryRemoteTypes = Uni.createFrom().item(new HashSet<String>());
		} else {
			queryRemoteTypes = queryDAO.getRemoteSourcesForTypes(tenant).onItem().transformToUni(rows -> {
				List<Uni<Map<String, Object>>> unis = Lists.newArrayList();
				if (rows.size() == 0) {
					return Uni.createFrom().item(new HashSet<>());
				}
				rows.forEach(row -> {
					// C.endpoint C.tenant_id, c.headers, c.reg_mode
					MultiMap remoteHeaders = MultiMap
							.newInstance(HttpUtils.getHeadersForRemoteCall(row.getJsonArray(2), row.getString(1)));
					unis.add(webClient.get(row.getString(0) + NGSIConstants.NGSI_LD_TYPES_ENDPOINT)
							.putHeaders(remoteHeaders).send().onFailure().recoverWithNull().onItem()
							.transform(response -> {
								Map<String, Object> responseTypes;
								if (response == null || response.statusCode() != 200) {
									responseTypes = null;
								} else {
									responseTypes = response.bodyAsJsonObject().getMap();
									try {
										responseTypes = (Map<String, Object>) JsonLdProcessor
												.expand(HttpUtils.getContextFromHeader(remoteHeaders), responseTypes,
														opts, -1, false)
												.get(0);
									} catch (JsonLdError e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (ResponseException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
								return responseTypes;
							}));
				});
				return Uni.combine().all().unis(unis).combinedWith(list -> {
					Set<String> currentTypes = Sets.newHashSet();
					for (Object entry : list) {
						if (entry == null) {
							continue;
						}
						Map<String, Object> typeMap = (Map<String, Object>) entry;
						mergeTypeList(typeMap.get(NGSIConstants.NGSI_LD_TYPE_LIST), currentTypes);
					}
					return currentTypes;
				});
			});
		}
		return Uni.combine().all().unis(queryRemoteTypes, queryDAO.getTypes(tenant)).asTuple().onItem().transform(t -> {
			Map<String, Object> result = Maps.newHashMap();
			Set<String> currentTypes = t.getItem1();
			List<Map<String, String>> types = Lists.newArrayList();
			result.put(NGSIConstants.NGSI_LD_TYPE_LIST, types);
			t.getItem2().forEach(row -> {
				currentTypes.add(row.getString(0));
			});
			currentTypes.forEach(type -> {
				Map<String, String> tmp = Maps.newHashMap();
				tmp.put(NGSIConstants.JSON_LD_ID, type);
				types.add(tmp);
			});
			result.put(NGSIConstants.JSON_LD_ID, "urn:ngsi-ld:EntityTypeList:" + random.nextInt());
			result.put(NGSIConstants.JSON_LD_TYPE, ENTITY_TYPE_LIST_TYPE);
			return result;
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
		return null;
	}

	public Uni<List<Map<String, Object>>> getAttribs(String tenant, boolean details, boolean localOnly) {
		Uni<List<Map<String, Object>>> local;
		if (details) {
			local = queryDAO.getAttributeList(tenant);
		} else {
			local = queryDAO.getAttributesDetail(tenant);
		}
		return local;
	}

	public Uni<Map<String, Object>> getAttrib(String tenant, String attrib, boolean localOnly) {
		Uni<Map<String, Object>> local = queryDAO.getAttributeDetail(tenant, attrib);
		return local;
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
}