package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.Map;

import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.core.json.JsonArray;
import io.vertx.mutiny.core.MultiMap;

public class CreateResult implements CRUDBaseResult {

	String entityId;
	private String endpoint;
	private MultiMap headers;
	private Map<String, Object> json = Maps.newHashMap();

	public CreateResult(String entityId) {
		this(entityId, null, null);
	}

	public CreateResult(String entityId, String endpoint, MultiMap headers) {
		this.entityId = entityId;
		this.endpoint = endpoint;
		this.headers = headers;
		json.put(NGSIConstants.QUERY_PARAMETER_ID, entityId);
		if (endpoint != null) {
			json.put(NGSIConstants.ERROR_DETAIL_ENDPOINT, endpoint);
		}

	}

	public String getEntityId() {
		return entityId;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public MultiMap getHeaders() {
		return headers;
	}

	@Override
	public Object getJson() {
		return json;
	}

}
