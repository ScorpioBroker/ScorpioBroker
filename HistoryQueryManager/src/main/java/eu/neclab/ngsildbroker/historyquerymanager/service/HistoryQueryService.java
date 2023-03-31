package eu.neclab.ngsildbroker.historyquerymanager.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import javax.annotation.PostConstruct;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AggrTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TemporalQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historyquerymanager.repository.HistoryDAO;
import io.quarkus.runtime.StartupEvent;
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

	private Table<String, String, RegistrationEntry> tenant2CId2RegEntries = HashBasedTable.create();

	@PostConstruct
	void init() {
		webClient = WebClient.create(vertx);
		historyDAO.getAllRegistries().onItem().transform(t -> {
			tenant2CId2RegEntries = t;
			return null;
		}).await().indefinitely();
	}

	// This is needed so that @postconstruct runs on the startup thread and not on a
	// worker thread later on
	void startup(@Observes StartupEvent event) {
	}

	public Uni<QueryResult> query(String tenant, String[] entityIds, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, TemporalQueryTerm tempQuery, AggrTerm aggrQuery, LanguageQueryTerm langQuery,
			Integer lastN, Integer limit, Integer offSet, Boolean count, Boolean localOnly, Context context) {
		return historyDAO.query(tenant, entityIds, typeQuery, idPattern, attrsQuery, qQuery,
				tempQuery, aggrQuery, geoQuery, scopeQuery, lastN, limit, offSet, count);
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

		Uni<Map<String, Object>> local = historyDAO.retrieveEntity(tenant, entityId, attrsQuery, aggrQuery, tempQuery,
				lang, lastN);
		if (localOnly) {
			return local;
		} else {
			Map<RemoteHost, Set<String>> remoteHosts = getRemoteHostsForRetrieve(tenant, entityId, attrsQuery);
			if (remoteHosts.isEmpty()) {
				return local;
			}
			List<Uni<Map<String, Object>>> remoteCalls = new ArrayList<>(remoteHosts.size());
			for (Entry<RemoteHost, Set<String>> entry : remoteHosts.entrySet()) {
				RemoteHost remoteHost = entry.getKey();
				String url = remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/" + entityId;
				Set<String> attrs = entry.getValue();
				if (attrs != null) {
					url += "?" + NGSIConstants.QUERY_PARAMETER_ATTRS + "=";
					for (String attr : attrs) {
						url += context.compactIri(attr) + ",";
					}
					url = url.substring(0, url.length() - 1);
				}

				remoteCalls
						.add(webClient.get(url).putHeaders(remoteHost.headers()).send().onItem().transform(response -> {
							Map<String, Object> responseEntity;
							if (response == null || response.statusCode() != 200) {
								responseEntity = null;
							} else {
								responseEntity = response.bodyAsJsonObject().getMap();
								try {
									responseEntity = (Map<String, Object>) JsonLdProcessor
											.expand(HttpUtils.getContextFromHeader(remoteHost.headers()),
													responseEntity, HttpUtils.opts, -1, false)
											.get(0);
								} catch (JsonLdError e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (ResponseException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}

								responseEntity.put(EntityTools.REG_MODE_KEY, remoteHost.regMode());

							}
							return responseEntity;
						}));
			}
			Uni<Map<String, Object>> remote = Uni.combine().all().unis(remoteCalls).combinedWith(list -> {
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

			return Uni.combine().all().unis(local, remote).asTuple().onItem().transformToUni(t -> {
				Map<String, Object> localEntity = t.getItem1();
				Map<String, Object> remoteEntity = t.getItem2();
				if (localEntity.isEmpty() && remoteEntity.isEmpty()) {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.NotFound, entityId + " was not found"));
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

	private Map<RemoteHost, Set<String>> getRemoteHostsForRetrieve(String tenant, String entityId,
			AttrsQueryTerm attrsQuery) {
		Map<RemoteHost, Set<String>> result = Maps.newHashMap();

		for (RegistrationEntry regEntry : tenant2CId2RegEntries.row(tenant).values()) {
			if (!regEntry.retrieveTemporal()) {
				continue;
			}
			if ((regEntry.eId() == null && regEntry.eIdp() == null)
					|| (regEntry.eId() != null && regEntry.eId().equals(entityId))
					|| (regEntry.eIdp() != null && entityId.matches(regEntry.eIdp()))) {
				RemoteHost remoteHost = new RemoteHost(regEntry.host().host(), regEntry.host().tenant(),
						regEntry.host().headers(), regEntry.host().cSourceId(), true, false, regEntry.regMode());

				Set<String> attribs;
				if (result.containsKey(remoteHost)) {
					attribs = result.get(remoteHost);
				} else {
					attribs = Sets.newHashSet();
					result.put(remoteHost, attribs);
				}
				if (regEntry.eProp() != null) {
					attribs.add(regEntry.eProp());
				}
				if (regEntry.eRel() != null) {
					attribs.add(regEntry.eRel());
				}
			}
		}
		if (attrsQuery != null) {
			for (Entry<RemoteHost, Set<String>> entry : result.entrySet()) {
				Set<String> regAttrs = entry.getValue();
				if (regAttrs.isEmpty()) {
					regAttrs.addAll(attrsQuery.getAttrs());
				} else {
					regAttrs.retainAll(attrsQuery.getAttrs());
				}
			}
		}
		return result;
	}

}
