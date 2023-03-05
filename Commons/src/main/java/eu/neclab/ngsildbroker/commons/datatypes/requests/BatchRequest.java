package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.List;
import java.util.Map;

import com.github.jsonldjava.core.Context;

import io.smallrye.mutiny.tuples.Tuple2;

public class BatchRequest {

	private String tenant;
	private List<Map<String, Object>> requestPayload;
	private List<Context> contexts;

	public BatchRequest(String tenant, List<Map<String, Object>> requestPayload, List<Context> contexts) {
		this.tenant = tenant;
		this.requestPayload = requestPayload;
		this.contexts = contexts;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public List<Map<String, Object>> getRequestPayload() {
		return requestPayload;
	}

	public List<Context> getContexts() {
		return contexts;
	}

}
