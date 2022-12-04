package eu.neclab.ngsildbroker.queryhandler.services;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.querybase.BaseQueryService;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.queryhandler.repository.CSourceDAO;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.core.MultiMap;

@Singleton
public class QueryService {

	private static String REG_MODE_KEY = "!@#$%";
	@Inject
	QueryDAO queryDAO;

	@Inject
	Vertx vertx;

	protected WebClient webClient;

	protected JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	private static final Set<String> DO_NOT_MERGE_KEYS = Sets.newHashSet(NGSIConstants.JSON_LD_ID,
			NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_CREATED_AT, NGSIConstants.NGSI_LD_OBSERVED_AT,
			NGSIConstants.NGSI_LD_MODIFIED_AT);

	@PostConstruct
	void setup() {
		webClient = WebClient.create(vertx);
	}

	public Uni<Map<String, Object>> retrieveEntity(ArrayListMultimap<String, String> headers, String entityId,
			Set<String> attrs, Set<String> expandedAttrs, String geometryProperty, String lang) {

		Uni<Map<String, Object>> getEntity = queryDAO.getEntity(entityId, HttpUtils.getTenantFromHeaders(headers));
		Uni<Map<String, Object>> getRemoteEntities = queryDAO
				.getRemoteSourcesForEntity(entityId, expandedAttrs, HttpUtils.getTenantFromHeaders(headers)).onItem()
				.transformToUni(rows -> {
					List<Uni<Map<String, Object>>> tmp = Lists.newArrayList();
					// C.endpoint C.tenant_id, c.headers, c.reg_mode
					rows.forEach(row -> {
						StringBuilder url = new StringBuilder(
								row.getString(0) + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId);
						url.append("?");

						if (attrs != null && !attrs.isEmpty()) {
							url.append("attrs=" + String.join(",", attrs) + "&");
						}
						if (geometryProperty != null) {
							url.append("geometryProperty=" + geometryProperty + "&");
						}
						if (lang != null) {
							url.append("lang=" + lang + "&");
						}
						url.append("options=sysAttrs");
						MultiMap remoteHeaders = HttpUtils.getHeaders(row.getJsonArray(2), headers, row.getString(1));
						tmp.add(webClient.get(url.toString()).putHeaders(remoteHeaders).send().onFailure()
								.recoverWithNull().onItem().transform(response -> {
									Map<String, Object> responseEntity;
									if (response == null || response.statusCode() != 200) {
										responseEntity = null;
									} else {
										responseEntity = response.bodyAsJsonObject().getMap();
										try {
											responseEntity = (Map<String, Object>) JsonLdProcessor
													.expand(getContextFromHeader(remoteHeaders), responseEntity, opts,
															-1, false)
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
		return Uni.combine().all().unis(getEntity, getRemoteEntities).asTuple().onItem().transformToUni(t -> {
			Map<String, Object> localEntity = t.getItem1();
			Map<String, Object> remoteEntity = t.getItem2();
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