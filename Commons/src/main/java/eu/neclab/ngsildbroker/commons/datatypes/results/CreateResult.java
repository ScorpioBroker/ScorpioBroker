package eu.neclab.ngsildbroker.commons.datatypes.results;

import io.vertx.core.json.JsonArray;
import io.vertx.mutiny.core.MultiMap;

public class CreateResult {

	String entityId;
	private String endpoint;
	private MultiMap headers;

	public CreateResult(String entityId) {
		this.entityId = entityId;
	}

	public CreateResult(String entityId, String endpoint, MultiMap headers) {
		this(entityId);
		this.endpoint = endpoint;
		this.headers = headers;

	}

	public String getEntityId() {
		return entityId;
	}

	public void setEntityId(String entityId) {
		this.entityId = entityId;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public MultiMap getHeaders() {
		return headers;
	}

	public void setHeaders(MultiMap headers) {
		this.headers = headers;
	}

}
