package eu.neclab.ngsildbroker.queryhandler.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;

@Singleton
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

	@PostConstruct
	void setup() {
		webClient = WebClient.create(vertx);
	}

	public Uni<QueryResult> query(String tenant, Set<String> id, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, String lang, int limit, int offSet, boolean count, boolean localOnly,
			Context context) {
		Uni<QueryResult> localQuery = queryDAO.queryLocalOnly(tenant, id, typeQuery, idPattern, attrsQuery, qQuery,
				geoQuery, scopeQuery, limit, offSet, count).onItem().transform(rows -> {
					QueryResult result = new QueryResult();
					if (limit == 0 && count) {
						result.setCount(rows.iterator().next().getLong(0));
					} else {
						RowIterator<Row> it = rows.iterator();
						Row next = null;

						List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>(rows.size());
						while (it.hasNext()) {
							next = it.next();
							resultData.add(next.getJsonObject(1).getMap());
						}
						Long resultCount = next.getLong(0);
						result.setCount(resultCount);
						long leftAfter = resultCount - (offSet + limit);
						if (leftAfter < 0) {
							leftAfter = 0;
						}
						long leftBefore = offSet;
						result.setResultsLeftAfter(leftAfter);
						result.setResultsLeftBefore(leftBefore);
						result.setLimit(limit);
						result.setOffset(offSet);
					}

					return result;
				});
		if (localOnly) {
			Uni<List<Map<String, Object>>> queryRemoteEntities = Uni.createFrom()
					.item(new ArrayList<Map<String, Object>>(0));
		} else {
			Uni<List<Map<String, Object>>> queryRemoteEntities = queryDAO.getRemoteSourcesForQuery(tenant, id,
					typeQuery, idPattern, attrsQuery, qQuery, csf, geoQuery, scopeQuery).onItem()
					.transformToUni(rows -> {
						// 0 C.endpoint 1C.tenant_id, 2c.headers, 3c.reg_mode,4 c.queryEntity,
						// 5c.queryBatch,
						// 6entityType, 7entityId, 8attrs, 9geoq, 10scopeq

						rows.forEach(row -> {
							String[] entityTypes = row.getArrayOfStrings(6);
							String[] entityIds = row.getArrayOfStrings(7);
							String[] attrs = row.getArrayOfStrings(7);
							StringBuilder url = new StringBuilder();
							TypeQueryTerm callTypeQuery;
							if (entityTypes != null && entityTypes.length > 0 && entityTypes[0] != null) {
								if (typeQuery != null) {
									callTypeQuery = typeQuery.getDuplicateAndRemoveNotKnownTypes(entityTypes);
								} else {
									callTypeQuery = new TypeQueryTerm(context);
									TypeQueryTerm currentCallTypeQuery = callTypeQuery;
									for (String entityType : entityTypes) {
										currentCallTypeQuery.setType(entityType);
										currentCallTypeQuery.setNextAnd(false);
										currentCallTypeQuery.setNext(new TypeQueryTerm(context));
										currentCallTypeQuery = currentCallTypeQuery.getNext();
									}
									currentCallTypeQuery.getPrev().setNext(null);
								}
							} else {
								callTypeQuery = typeQuery;
							}
							if (callTypeQuery != null) {
								url.append("type=");
								url.append(callTypeQuery.getTypeQuery());
								url.append('&');
							}
							if (entityIds != null && entityIds.length > 0 && entityIds[0] != null) {
								url.append("id=");
								for (String entityId : entityIds) {
									url.append(context.compactIri(entityId));
									url.append(',');
								}
								url.setLength(url.length() - 1);
								url.append('&');

							} else {
								if (id != null) {
									url.append("id=");
									for (String entityId : id) {
										url.append(context.compactIri(entityId));
										url.append(',');
									}
									url.setLength(url.length() - 1);
									url.append('&');
								} else if (idPattern != null) {
									url.append("idPattern=");
									url.append(idPattern);
									url.append('&');
								}
							}

							if (attrs != null && attrs.length > 0 && attrs[0] != null) {
								url.append("attrs=");
								for (String attr : attrs) {
									url.append(context.compactIri(attr));
									url.append(',');
								}
								url.setLength(url.length() - 1);
								url.append('&');
							} else {
								if (attrsQuery != null) {
									url.append("attrs=");
									for (String attr : attrsQuery.getAttrs()) {
										url.append(context.compactIri(attr));
										url.append(',');
									}
									url.setLength(url.length() - 1);
									url.append('&');
								}
							}

						});
						return null;
					});
		}
		return null;
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
		return Uni.combine().all().unis(queryRemoteTypes, queryDAO.getTypes(tenant)).asTuple().onItem().transform(t -> {
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

	public Uni<Map<String, Object>> getAttribs(String tenant, boolean localOnly) {
		return null;
	}

	public Uni<Map<String, Object>> getAttrib(String tenant, String attrib, boolean localOnly) {
		return null;
	}

	public Uni<Map<String, Object>> retrieveEntity(Context context, String tenant, String entityId,
			AttrsQueryTerm attrsQuery, String lang, boolean localOnly) {
		Uni<Map<String, Object>> getEntity = queryDAO.getEntity(entityId, tenant);
		Uni<Map<String, Object>> getRemoteEntities;
		if (localOnly) {
			getRemoteEntities = Uni.createFrom().item(new HashMap<String, Object>(0));
		} else {
			getRemoteEntities = queryDAO.getRemoteSourcesForEntity(entityId, attrsQuery.getAttrs(), tenant).onItem()
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
//			if (attrs != null && !attrs.isEmpty()) {
//				EntityTools.removeAttrs(localEntity, attrs);
//			}
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

	public Uni<List<QueryResult>> postQuery(String tenant, List<Map<String, Object>> entities, String lang, int limit,
			int offSet, boolean count, boolean localOnly, Context context) {
		List<Uni<QueryResult>> listResults = new ArrayList<>();
		for (Map<String, Object> entity : entities) {
			Set<String> ids = new HashSet<>();
			if (entity.get("id") instanceof List<?> idList) {
				ids.addAll((List<String>) idList);
			} else
				ids.add((String) entity.get("id"));
			TypeQueryTerm typeQueryTerm = new TypeQueryTerm(context);
			typeQueryTerm.setType((String) entity.get("type"));
			AttrsQueryTerm attrsQueryTerm = new AttrsQueryTerm(context);
			attrsQueryTerm.addAttr((String) entity.get("attrs"));
			QQueryTerm qQueryTerm = new QQueryTerm(context);
			CSFQueryTerm csfQueryTerm = new CSFQueryTerm(context);
			GeoQueryTerm geoQueryTerm = new GeoQueryTerm(context);
			geoQueryTerm.setGeometry((String) entity.get("Geometry"));
			ScopeQueryTerm scopeQueryTerm = new ScopeQueryTerm();
			scopeQueryTerm.setScopeLevels(((String) entity.get("scopeQ")).split(","));

			listResults.add(query(tenant, ids, typeQueryTerm, (String) entity.get("idPattern"), attrsQueryTerm,
					qQueryTerm, csfQueryTerm, geoQueryTerm, scopeQueryTerm, lang, limit, offSet, count, localOnly,
					context));

		}
		return Uni.join().all(listResults).andCollectFailures();
	}

}