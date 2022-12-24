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
import com.google.common.collect.ArrayListMultimap;
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
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;

@Singleton
public class QueryService {

	private static String REG_MODE_KEY = "!@#$%";
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

	protected WebClient webClient;

	protected JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
	private Random random = new Random();

	private static final Set<String> DO_NOT_MERGE_KEYS = Sets.newHashSet(NGSIConstants.JSON_LD_ID,
			NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_CREATED_AT, NGSIConstants.NGSI_LD_OBSERVED_AT,
			NGSIConstants.NGSI_LD_MODIFIED_AT);

	@PostConstruct
	void setup() {
		webClient = WebClient.create(vertx);
	}

	public Uni<QueryResult> query(ArrayListMultimap<String, String> headers, Set<String> id, TypeQueryTerm typeQuery,
			String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, String lang, int limit, int offSet, boolean count, boolean localOnly,
			Context context) {
		if (localOnly) {
			return queryDAO.query(HttpUtils.getTenantFromHeaders(headers), id, typeQuery,
				idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, limit, offSet, count).onItem().transform(rows -> {
					QueryResult result = new QueryResult();
					if(limit == 0 && count) {
						result.setCount(rows.iterator().next().getLong(0));
					}else {
						RowIterator<Row> it = rows.iterator();
						Row next = null;
						
						List<Map<String, Object>> resultData = new ArrayList<Map<String,Object>>(rows.size());
						while(it.hasNext()) {
							next = it.next();
							resultData.add(next.getJsonObject(1).getMap());
						}
						Long resultCount = next.getLong(0);
						result.setCount(resultCount);
						long leftAfter = resultCount - (offSet+limit);
						long leftBefore = offSet;
						result.setResultsLeftAfter(leftAfter);
						result.setResultsLeftBefore(leftBefore);
						result.setLimit(limit);
						result.setOffset(offSet);
					}
					
					return result;
				});
		}else {
			
		}
		Uni<List<Map<String, Object>>> queryRemoteEntities;
		if (localOnly) {
			queryRemoteEntities = Uni.createFrom().item(new ArrayList<Map<String, Object>>(0));
		} else {
			queryRemoteEntities = queryDAO.getRemoteSourcesForQuery(HttpUtils.getTenantFromHeaders(headers), id,
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

	public Uni<List<Map<String, Object>>> getTypesWithDetail(ArrayListMultimap<String, String> headers,
			boolean localOnly) {
		Uni<Map<String, Set<String>>> queryRemoteTypes;
		String tenantId = HttpUtils.getTenantFromHeaders(headers);
		if (localOnly) {
			Map<String, Object> tmp = Maps.newHashMap();
			tmp.put(NGSIConstants.NGSI_LD_TYPE_LIST, new ArrayList<Map<String, Object>>());
			queryRemoteTypes = Uni.createFrom().item(new HashMap<String, Set<String>>());
		} else {
			queryRemoteTypes = queryDAO.getRemoteSourcesForTypesWithDetails(tenantId).onItem().transformToUni(rows -> {
				List<Uni<Map<String, Object>>> unis = Lists.newArrayList();
				rows.forEach(row -> {
					// C.endpoint C.tenant_id, c.headers, c.reg_mode
					MultiMap remoteHeaders = HttpUtils.getHeaders(row.getJsonArray(2), headers, row.getString(1));
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
												.expand(getContextFromHeader(remoteHeaders), responseTypes, opts, -1,
														false)
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
		return Uni.combine().all().unis(queryRemoteTypes, queryDAO.getTypes(tenantId)).asTuple().onItem()
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

	public Uni<Map<String, Object>> getTypes(ArrayListMultimap<String, String> headers, boolean localOnly) {
		Uni<Set<String>> queryRemoteTypes;
		String tenantId = HttpUtils.getTenantFromHeaders(headers);
		if (localOnly) {
			queryRemoteTypes = Uni.createFrom().item(new HashSet<String>());
		} else {
			queryRemoteTypes = queryDAO.getRemoteSourcesForTypes(tenantId).onItem().transformToUni(rows -> {
				List<Uni<Map<String, Object>>> unis = Lists.newArrayList();
				rows.forEach(row -> {
					// C.endpoint C.tenant_id, c.headers, c.reg_mode
					MultiMap remoteHeaders = HttpUtils.getHeaders(row.getJsonArray(2), headers, row.getString(1));
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
												.expand(getContextFromHeader(remoteHeaders), responseTypes, opts, -1,
														false)
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
		return Uni.combine().all().unis(queryRemoteTypes, queryDAO.getTypes(tenantId)).asTuple().onItem()
				.transform(t -> {
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

	public Uni<Map<String, Object>> getType(ArrayListMultimap<String, String> headers, String type, boolean localOnly) {
		return null;
	}

	public Uni<Map<String, Object>> getAttribs(ArrayListMultimap<String, String> headers, boolean localOnly) {
		return null;
	}

	public Uni<Map<String, Object>> getAttrib(ArrayListMultimap<String, String> headers, String attrib,
			boolean localOnly) {
		return null;
	}

	public Uni<Map<String, Object>> retrieveEntity(Context context, ArrayListMultimap<String, String> headers,
			String entityId, Set<String> attrs, Set<String> expandedAttrs, String geometryProperty, String lang,
			boolean localOnly) {
		Uni<Map<String, Object>> getEntity = queryDAO.getEntity(entityId, HttpUtils.getTenantFromHeaders(headers));
		Uni<Map<String, Object>> getRemoteEntities;
		if (localOnly) {
			getRemoteEntities = Uni.createFrom().item(new HashMap<String, Object>(0));
		} else {
			getRemoteEntities = queryDAO
					.getRemoteSourcesForEntity(entityId, expandedAttrs, HttpUtils.getTenantFromHeaders(headers))
					.onItem().transformToUni(rows -> {
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
								if (attrs != null && !attrs.isEmpty()) {
									url.append("attrs=" + String.join(",", attrs) + "&");
								}
							}
							if (lang != null) {
								url.append("lang=" + lang + "&");
							}
							url.append("options=sysAttrs");
							MultiMap remoteHeaders = HttpUtils.getHeaders(row.getJsonArray(2), headers,
									row.getString(1));
							tmp.add(webClient.get(url.toString()).putHeaders(remoteHeaders).send().onFailure()
									.recoverWithNull().onItem().transform(response -> {
										Map<String, Object> responseEntity;
										if (response == null || response.statusCode() != 200) {
											responseEntity = null;
										} else {
											responseEntity = response.bodyAsJsonObject().getMap();
											try {
												responseEntity = (Map<String, Object>) JsonLdProcessor
														.expand(getContextFromHeader(remoteHeaders), responseEntity,
																opts, -1, false)
														.get(0);
											} catch (JsonLdError e) {
												// TODO Auto-generated catch block
												e.printStackTrace();
											} catch (ResponseException e) {
												// TODO Auto-generated catch block
												e.printStackTrace();
											}
											responseEntity.put(REG_MODE_KEY, row.getInteger(3));

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
								int regMode = (int) entityMap.remove(REG_MODE_KEY);
								for (Entry<String, Object> attrib : entityMap.entrySet()) {
									String key = attrib.getKey();
									if (DO_NOT_MERGE_KEYS.contains(key)) {
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
									addRegModeToValue(newValue, regMode);
									if (currentValue == null) {
										result.put(key, newValue);
									} else {
										mergeValues((List<Map<String, Object>>) currentValue, newValue);
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
			if (attrs != null && !attrs.isEmpty()) {
				removeAttrs(localEntity, attrs);
			}
			if (localEntity.isEmpty() && remoteEntity.isEmpty()) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, entityId + " was not found"));
			}

			if (remoteEntity.isEmpty()) {
				return Uni.createFrom().item(localEntity);
			}
			if (localEntity.isEmpty()) {
				removeRegKey(remoteEntity);
				return Uni.createFrom().item(remoteEntity);
			}
			for (Entry<String, Object> attrib : remoteEntity.entrySet()) {
				String key = attrib.getKey();
				if (DO_NOT_MERGE_KEYS.contains(key)) {
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
					mergeValues((List<Map<String, Object>>) currentValue, newValue);
				}

			}
			removeRegKey(localEntity);
			return Uni.createFrom().item(localEntity);

		});

	}

	private void removeAttrs(Map<String, Object> localEntity, Set<String> attrs) {
		Set<String> entityAttrs = localEntity.keySet();
		boolean attrsFound = false;
		for (String attr : attrs) {
			if (entityAttrs.contains(attr)) {
				attrsFound = true;
				break;
			}
		}
		entityAttrs.removeAll(attrs);
		entityAttrs.removeAll(DO_NOT_MERGE_KEYS);
		if (entityAttrs.isEmpty() && !attrsFound) {
			localEntity.clear();
		} else {
			for (String attr : entityAttrs) {
				localEntity.remove(attr);
			}
		}

	}

	private void removeRegKey(Map<String, Object> remoteEntity) {
		for (Entry<String, Object> attrib : remoteEntity.entrySet()) {
			String key = attrib.getKey();
			if (DO_NOT_MERGE_KEYS.contains(key)) {
				continue;
			}
			List<Map<String, Object>> list = (List<Map<String, Object>>) attrib.getValue();
			for (Map<String, Object> entry : list) {
				entry.remove(REG_MODE_KEY);
			}
		}

	}

	private void mergeValues(List<Map<String, Object>> currentValue, List<Map<String, Object>> newValue) {
		long newObservedAt = -1, newModifiedAt = -1, newCreatedAt = -1, currentObservedAt = -1, currentModifiedAt = -1,
				currentCreatedAt = -1;
		int currentRegMode = -1;
		String newDatasetId;
		int removeIndex = -1;
		int regMode = -1;
		boolean found = false;
		for (Map<String, Object> entry : newValue) {
			if (entry.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
				newDatasetId = ((List<Map<String, String>>) entry.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0)
						.get(NGSIConstants.JSON_LD_ID);
			} else {
				newDatasetId = null;
			}
			newObservedAt = -1;
			newModifiedAt = -1;
			newCreatedAt = -1;
			try {
				newCreatedAt = SerializationTools.date2Long((String) entry.get(NGSIConstants.NGSI_LD_CREATED_AT));
				newModifiedAt = SerializationTools.date2Long((String) entry.get(NGSIConstants.NGSI_LD_MODIFIED_AT));
				if (entry.containsKey(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
					newObservedAt = SerializationTools.date2Long((String) entry.get(NGSIConstants.NGSI_LD_OBSERVED_AT));
				}
			} catch (Exception e) {
				// do nothing intentionally
			}
			regMode = -1;
			if (entry.containsKey(REG_MODE_KEY)) {
				regMode = (int) entry.get(REG_MODE_KEY);
			}
			removeIndex = -1;
			found = false;
			for (int i = 0; i < currentValue.size(); i++) {
				Map<String, Object> currentEntry = currentValue.get(i);
				currentRegMode = -1;
				if (currentEntry.containsKey(REG_MODE_KEY)) {
					currentRegMode = (int) currentEntry.get(REG_MODE_KEY);
				}
				String currentDatasetId;
				if (currentEntry.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
					currentDatasetId = ((List<Map<String, String>>) currentEntry.get(NGSIConstants.NGSI_LD_DATA_SET_ID))
							.get(0).get(NGSIConstants.JSON_LD_ID);
				} else {
					currentDatasetId = null;
				}
				if ((currentDatasetId == null && newDatasetId == null) || (currentDatasetId != null
						&& newDatasetId != null && currentDatasetId.equals(newDatasetId))) {
					// 0 auxilliary
					// 1 inclusive
					// 2 proxy
					// 3 exclusive
					found = true;
					if (currentRegMode == 3 || regMode == 0) {
						break;
					}
					if (regMode == 3 || currentRegMode == 0) {
						removeIndex = i;
						break;
					}
					currentObservedAt = -1;
					currentModifiedAt = -1;
					currentCreatedAt = -1;
					try {
						currentCreatedAt = SerializationTools
								.date2Long((String) currentEntry.get(NGSIConstants.NGSI_LD_CREATED_AT));
						currentModifiedAt = SerializationTools
								.date2Long((String) currentEntry.get(NGSIConstants.NGSI_LD_MODIFIED_AT));
						if (currentEntry.containsKey(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
							currentObservedAt = SerializationTools
									.date2Long((String) currentEntry.get(NGSIConstants.NGSI_LD_OBSERVED_AT));
						}
					} catch (Exception e) {
						// do nothing intentionally
					}
					// if observedAt is set it will take preference over modifiedAt
					if (currentObservedAt != -1 || newObservedAt != -1) {
						if (currentObservedAt >= newObservedAt) {
							break;
						} else {
							removeIndex = i;
							break;
						}
					}
					if (currentModifiedAt >= newModifiedAt) {
						break;
					} else {
						removeIndex = i;
						break;
					}

				}
			}
			if (found) {
				if (removeIndex != -1) {
					currentValue.remove(removeIndex);
					currentValue.add(entry);
				}
			} else {
				currentValue.add(entry);
			}
		}
	}

	private void addRegModeToValue(List<Map<String, Object>> newValue, int regMode) {
		for (Map<String, Object> entry : newValue) {
			entry.put(REG_MODE_KEY, regMode);
		}

	}

	private List<Object> getContextFromHeader(MultiMap remoteHeaders) {
		String tmp = remoteHeaders.get("Link").split(";")[0];
		if (tmp.charAt(0) == '<') {
			tmp = tmp.substring(1, tmp.length() - 1);
		}
		return Lists.newArrayList(tmp);
	}
}