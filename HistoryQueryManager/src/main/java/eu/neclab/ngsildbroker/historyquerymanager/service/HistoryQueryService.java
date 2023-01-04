package eu.neclab.ngsildbroker.historyquerymanager.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AggrTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TemporalQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historyquerymanager.repository.HistoryDAO;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;

@Singleton
public class HistoryQueryService {

	private final static Logger logger = LoggerFactory.getLogger(HistoryQueryService.class);

	@Inject
	Vertx vertx;

	private WebClient webClient;

	@Inject
	HistoryDAO historyDAO;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	@PostConstruct
	void setup() {
		webClient = WebClient.create(vertx);
	}

	public Uni<QueryResult> query(String tenant, Set<String> entityIds, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, TemporalQueryTerm tempQuery, AggrTerm aggrQuery, String lang, int lastN,
			int limit, int offSet, boolean count, boolean localOnly, Context context) {
		return null;
	}

	/**
	 * 
	 * @param tenant
	 * @param entityId
	 * @param attrsQuery
	 * @param aggrQuery
	 * @param tempQuery
	 * @param lang
	 * @param lastN
	 * @param localOnly
	 * @param context
	 * @return Single entity merged with potential
	 */
	public Uni<Map<String, Object>> retrieveEntity(String tenant, String entityId, AttrsQueryTerm attrsQuery,
			AggrTerm aggrQuery, TemporalQueryTerm tempQuery, String lang, int lastN, boolean localOnly,
			Context context) {
		Uni<Map<String, Object>> getRemoteEntities;
		Uni<Map<String, Object>> getEntity = historyDAO
				.retrieveEntity(tenant, entityId, attrsQuery, aggrQuery, tempQuery, lang, lastN).onItem()
				.transform(resultTable -> {
					if (resultTable.size() == 0) {
						return new HashMap<>(0);
					}
					return resultTable.iterator().next().getJsonObject(0).getMap();
				});
		if (localOnly) {
			getRemoteEntities = Uni.createFrom().item(new HashMap<String, Object>(0));
		} else {
			getRemoteEntities = historyDAO.getRemoteSourcesForEntity(tenant, entityId, attrsQuery.getAttrs()).onItem()
					.transformToUni(rows -> {

						List<Uni<Map<String, Object>>> tmp = Lists.newArrayList();
						// C.endpoint C.tenant_id, c.headers, c.reg_mode
						rows.forEach(row -> {

							StringBuilder url = new StringBuilder(
									row.getString(0) + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/" + entityId);
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
														.expand(HttpUtils.getContextFromHeader(remoteHeaders), responseEntity,
																HttpUtils.opts, -1, false)
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
//				if (attrs != null && !attrs.isEmpty()) {
//					EntityTools.removeAttrs(localEntity, attrs);
//				}
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
