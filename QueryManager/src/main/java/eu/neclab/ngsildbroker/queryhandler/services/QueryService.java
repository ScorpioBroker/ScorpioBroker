package eu.neclab.ngsildbroker.queryhandler.services;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.querybase.BaseQueryService;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
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
		List<Uni<Map<String, Object>>> unis = Lists.newArrayList();
		unis.add(queryDAO.getEntity(entityId, HttpUtils.getTenantFromHeaders(headers)));
		unis.add(queryDAO.getRemoteSourcesForEntity(entityId, expandedAttrs, HttpUtils.getTenantFromHeaders(headers))
				.onItem().transformToUni(rows -> {
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
						tmp.add(webClient.get(url.toString())
								.putHeaders(remoteHeaders).send()
								.onFailure().recoverWithNull().onItem().transform(response -> {
									Map<String, Object> responseEntity;
									if (response == null || response.statusCode() != 200) {
										responseEntity = null;
									} else {
										responseEntity = response.bodyAsJsonObject().getMap();
										try {
											responseEntity = (Map<String, Object>) JsonLdProcessor.expand(getContextFromHeader(remoteHeaders), responseEntity, opts, -1, false).get(0);
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
					return Uni.combine().all().unis(unis).combinedWith(list -> {
						Map<String, Object> result = Maps.newHashMap();
						for(Object entry: list) {
							if(entry == null) {
								continue;
							}
							Map<String, Object> entityMap = (Map<String, Object>) entry;
							int regMode = (int) entityMap.remove(REG_MODE_KEY);
							for(Entry<String, Object> attrib: entityMap.entrySet()) {
								String key = attrib.getKey();
								if(DO_NOT_MERGE_KEYS.contains(key)) {
									continue;
								}
								Object currentValue = result.get(key);
								Object newValue = attrib.getValue();
								if(currentValue == null) {
									addRegModeToValue(newValue);
									result.put(key, newValue);
								}else {
									
								}
								
							}
							
						}
						return result;
					});
				}));

		return null;
	}

	private void addRegModeToValue(Object newValue) {
		// TODO Auto-generated method stub
		
	}

	private List<Object> getContextFromHeader(MultiMap remoteHeaders) {
		String tmp = remoteHeaders.get("Link").split(";")[0];
		if (tmp.charAt(0) == '<') {
			tmp = tmp.substring(1, tmp.length() - 1);
		}
		return Lists.newArrayList(tmp);
	}
}