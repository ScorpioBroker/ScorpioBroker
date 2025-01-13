package eu.neclab.ngsildbroker.historyquerymanager.service;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.github.jsonldjava.core.JsonLdConsts;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.mutiny.core.MultiMap;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
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
import eu.neclab.ngsildbroker.commons.interfaces.CSourceHandler;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.historyquerymanager.repository.HistoryDAO;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.pgclient.PgException;

import static eu.neclab.ngsildbroker.commons.tools.HttpUtils.parseLinkHeaderNoUni;

@Singleton
@SuppressWarnings("unchecked")
public class HistoryQueryService implements CSourceHandler {

	private final static Logger logger = LoggerFactory.getLogger(HistoryQueryService.class);

	@Inject
	JsonLDService ldService;

	@Inject
	Vertx vertx;

	private WebClient webClient;

	@Inject
	HistoryDAO historyDAO;
	
	@Inject
	MicroServiceUtils microServiceUtils;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	private Table<String, String, List<RegistrationEntry>> tenant2CId2RegEntries = HashBasedTable.create();

	@PostConstruct
	void init() {
		webClient = WebClient.create(vertx);
		historyDAO.getAllRegistries().onItem().transform(t -> {
			tenant2CId2RegEntries = t;
			return null;
		}).await().indefinitely();
		this.microServiceUtils.registerCSourceReceiver(this);
	}

	// This is needed so that @postconstruct runs on the startup thread and not on a
	// worker thread later on
	void startup(@Observes StartupEvent event) {
	}

	public Uni<QueryResult> query(String tenant, String[] entityIds, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, TemporalQueryTerm tempQuery, AggrTerm aggrQuery, LanguageQueryTerm langQuery,
			Integer lastN, Integer limit, Integer offSet, Boolean count, Boolean localOnly, Context context,
			HttpServerRequest request) {

		Uni<QueryResult> local = historyDAO.query(tenant, entityIds, typeQuery, idPattern, attrsQuery, qQuery,
				tempQuery, aggrQuery, geoQuery, scopeQuery, lastN, limit, offSet, count).onFailure()
				.recoverWithUni(e -> {
					if (e instanceof PgException) {
						PgException pge = (PgException) e;
						logger.debug("At position " + pge.getPosition());
						logger.debug("failed to query", pge);
						if (pge.getSqlState().equals(AppConstants.SQL_INVALID_OPERATOR)) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.InvalidRequest,
									"Invalid operator in q query or aggr query"));
						}
					}
					return Uni.createFrom().failure(e);
				});
		if (localOnly) {
			return local;
		}
		Map<RemoteHost, String> remoteHosts = getRemoteHostsForQuery(tenant, request.query(), context);
		if (remoteHosts.isEmpty()) {
			return local;
		}
		List<Uni<QueryResult>> remoteCalls = new ArrayList<>(remoteHosts.size());
		for (Entry<RemoteHost, String> entry : remoteHosts.entrySet()) {
			RemoteHost remoteHost = entry.getKey();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), request.headers());

			String url = remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "?" + entry.getValue();
			String linkHead;
			List<Object> contextLinks;
			if (!remoteHost.headers().contains(NGSIConstants.LINK_HEADER)) {
				linkHead = request.headers().get(NGSIConstants.LINK_HEADER);
				contextLinks = parseLinkHeaderNoUni(request.headers().getAll(NGSIConstants.LINK_HEADER),
						NGSIConstants.HEADER_REL_LDCONTEXT);
			} else {
				linkHead = remoteHost.headers().get(NGSIConstants.LINK_HEADER);
				contextLinks = parseLinkHeaderNoUni(remoteHost.headers().getAll(NGSIConstants.LINK_HEADER),
						NGSIConstants.HEADER_REL_LDCONTEXT);
			}
			remoteCalls.add(webClient.getAbs(url).putHeaders(toFrwd).putHeader(NGSIConstants.LINK_HEADER, linkHead)
					.send().onItem().transformToUni(response -> {

						if (response == null || response.statusCode() != 200) {
							return Uni.createFrom().nullItem();
						} else {
							List<Object> responseEntity = response.bodyAsJsonArray().getList();
							return ldService.expand(contextLinks, responseEntity, HttpUtils.opts, -1, false).onItem()
									.transform(expanded -> {
										QueryResult result = new QueryResult();
										List<Map<String, Object>> resultList = new ArrayList<>(expanded.size());
										for (Object entry2 : expanded) {
											Map<String, Object> tmp = (Map<String, Object>) entry2;
//											tmp.put(EntityTools.REG_MODE_KEY, remoteHost.regMode());
											resultList.add(tmp);
										}
										result.setData(resultList);
										if (count) {
											result.setCount(Long
													.parseLong(response.getHeader(NGSIConstants.COUNT_HEADER_RESULT)));
										}
										return result;
									});
						}

					}));
		}
		remoteCalls.add(0, local);
		return Uni.combine().all().unis(remoteCalls).with(list -> {
			QueryResult result = new QueryResult();
			Map<String, Map<String, Object>> entityId2Entity = Maps.newHashMap();
			long rCount = 0;
			for (Object entry : list) {
				QueryResult qResult = (QueryResult) entry;
				mergeInResult(entityId2Entity, qResult.getData());
				if (count) {
					rCount += qResult.getCount();
				}
			}
			result.setData(Lists.newArrayList(entityId2Entity.values()));
			if (count) {
				result.setCount(rCount);
			} else {
				result.setCount(-1l);
			}
			return result;
		});

	}

	private void mergeInResult(Map<String, Map<String, Object>> entityId2Entity, List<Map<String, Object>> data) {
		for (Map<String, Object> item : data) {
			entityId2Entity.put((String) item.getOrDefault(NGSIConstants.ID, item.get(JsonLdConsts.ID)), item);
		}
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
			AggrTerm aggrQuery, TemporalQueryTerm tempQuery, String lang, int lastN, boolean localOnly, Context context,
			io.vertx.core.MultiMap headersFromReq) {

		Uni<Map<String, Object>> local = historyDAO.retrieveEntity(tenant, entityId, attrsQuery, aggrQuery, tempQuery,
				lang, lastN);
		if (localOnly) {
			return local.onItem().transformToUni(localItem -> {
				if (localItem.isEmpty()) {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.NotFound, entityId + " was not found"));
				}
				return Uni.createFrom().item(localItem);
			});

		} else {
			Map<RemoteHost, Set<String>> remoteHosts = getRemoteHostsForRetrieve(tenant, entityId, attrsQuery);
			if (remoteHosts.isEmpty()) {
				return local.onItem().transformToUni(item -> {
					if (item.isEmpty()) {
						return Uni.createFrom()
								.failure(new ResponseException(ErrorType.NotFound, entityId + " was not found"));
					}
					return Uni.createFrom().item(item);
				});
			}
			List<Uni<Map<String, Object>>> remoteCalls = new ArrayList<>(remoteHosts.size());
			for (Entry<RemoteHost, Set<String>> entry : remoteHosts.entrySet()) {
				RemoteHost remoteHost = entry.getKey();
				MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);

				String url = remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/" + entityId;
				Set<String> attrs = entry.getValue();
				if (attrs != null) {
					url += "?" + NGSIConstants.QUERY_PARAMETER_ATTRS + "=";
					for (String attr : attrs) {
						url += context.compactIri(attr) + ",";
					}
					url = url.substring(0, url.length() - 1);
				}

				remoteCalls.add(webClient.getAbs(url).putHeaders(toFrwd).send().onItem().transformToUni(response -> {
					if (response == null || response.statusCode() != 200) {
						return Uni.createFrom().nullItem();
					} else {
						Map<String, Object> responseEntity = response.bodyAsJsonObject().getMap();
						return ldService.expand(HttpUtils.getContextFromHeader(remoteHost.headers()), responseEntity,
								HttpUtils.opts, -1, false).onItem().transform(expanded -> {
									Map<String, Object> result = (Map<String, Object>) expanded.get(0);
									result.put(AppConstants.REG_MODE_KEY, remoteHost.regMode());
									return result;
								});
					}
				}));
			}
			Uni<Map<String, Object>> remote = Uni.combine().all().unis(remoteCalls).with(list -> {
				Map<String, Object> result = Maps.newHashMap();
				for (Object entry : list) {
					if (entry == null) {
						continue;
					}
					Map<String, Object> entityMap = (Map<String, Object>) entry;
					int regMode = (int) entityMap.remove(AppConstants.REG_MODE_KEY);
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

		for (List<RegistrationEntry> regEntries : tenant2CId2RegEntries.row(tenant).values()) {
			for (RegistrationEntry regEntry : regEntries) {
				if (!regEntry.retrieveTemporal()) {
					continue;
				}
				if ((regEntry.eId() == null && regEntry.eIdp() == null)
						|| (regEntry.eId() != null && regEntry.eId().equals(entityId))
						|| (regEntry.eIdp() != null && entityId.matches(regEntry.eIdp()))) {
					RemoteHost remoteHost = new RemoteHost(regEntry.host().host(), regEntry.host().tenant(),
							regEntry.host().headers(), regEntry.host().cSourceId(), true, false, regEntry.regMode(),
							false, regEntry.queryEntityMap());

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
		}
		return result;
	}

	private Map<RemoteHost, String> getRemoteHostsForQuery(String tenant, String query, Context context) {
		Map<RemoteHost, String> result = new HashMap<>();
		Map<String, String> queryMap = queryStrToMap(query);
		Set<String> filteredIds = new HashSet<>();
		Set<String> entityIds = new HashSet<>(Arrays.asList(queryMap.getOrDefault("id", "").split(",")));
		Set<String> filteredTypes = new HashSet<>();
		Set<String> types = new HashSet<>(Arrays.asList(queryMap.getOrDefault("type", "").split(",")));
		for (List<RegistrationEntry> regEntries : tenant2CId2RegEntries.row(tenant).values()) {
			for (RegistrationEntry regEntry : regEntries) {
				if (!regEntry.retrieveTemporal()) {
					continue;
				}

				RemoteHost remoteHost = new RemoteHost(regEntry.host().host(), regEntry.host().tenant(),
						regEntry.host().headers(), regEntry.host().cSourceId(), true, false, regEntry.regMode(), false,
						regEntry.queryEntityMap());

				if (regEntry.eId() != null || regEntry.eIdp() != null) {
					for (String id : entityIds) {
						if ((regEntry.eId() != null && regEntry.eId().equals(id))
								|| (regEntry.eIdp() != null && id.matches(regEntry.eIdp()))) {
							filteredIds.add(id);
						}
					}
				} else {
					filteredIds = entityIds;
				}

				if (regEntry.type() != null) {
					for (String type : types) {
						if (regEntry.type().equals(context.expandIri(type, false, true, null, null))) {
							filteredTypes.add(type);
						}
					}
				} else {
					filteredTypes = types;
				}

				queryMap.put("id", String.join(",", filteredIds));
				queryMap.put("type", String.join(",", filteredTypes));

				if (regEntry.eProp() != null || regEntry.eRel() != null) {
					Set<String> uniqueAttrs = new HashSet<>(
							Arrays.asList(queryMap.getOrDefault("attrs", "").split(",")));
					uniqueAttrs.add(regEntry.eProp());
					uniqueAttrs.add(regEntry.eRel());
					uniqueAttrs.remove("");
					uniqueAttrs.remove(null);
					queryMap.put("attrs", String.join(",", uniqueAttrs));
				}
				result.put(remoteHost, queryMapToStr(queryMap));
			}
		}
		return result;
	}

	private static Map<String, String> queryStrToMap(String queryString) {
		Map<String, String> paramMap = new HashMap<>();

		String[] paramPairs = queryString.split("&");
		for (String paramPair : paramPairs) {
			String[] keyValue = paramPair.split("=");
			if (keyValue.length == 2) {
				String key = keyValue[0];
				String value = keyValue[1];
				paramMap.put(key, value);
			}
		}

		return paramMap;
	}

	public static String queryMapToStr(Map<String, String> paramMap) {
		StringBuilder queryStringBuilder = new StringBuilder();
		List<String> keyList = paramMap.keySet().stream().toList();
		for (String key : keyList) {
			if (paramMap.get(key).isEmpty()) {
				paramMap.remove(key);
			}
		}

		for (Map.Entry<String, String> entry : paramMap.entrySet()) {
			if (!queryStringBuilder.isEmpty()) {
				queryStringBuilder.append("&");
			}
			String key = URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8);
			String value = URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8);
			queryStringBuilder.append(key).append("=").append(value);
		}
		return queryStringBuilder.toString();
	}

	public Uni<Void> handleRegistryChange(CSourceBaseRequest req) {
		return RegistrationEntry.fromRegPayload(req.getPayload(), ldService).onItem().transformToUni(regs -> {
			tenant2CId2RegEntries.remove(req.getTenant(), req.getId());
			if (req.getRequestType() != AppConstants.DELETE_REQUEST) {
				List<RegistrationEntry> newRegs = Lists.newArrayList();
				for (RegistrationEntry regEntry : regs) {
					if (regEntry.retrieveTemporal() || regEntry.queryTemporal()) {
						newRegs.add(regEntry);
					}
				}
				tenant2CId2RegEntries.put(req.getTenant(), req.getId(), newRegs);
			}
			return Uni.createFrom().voidItem();
		});
	}

}
