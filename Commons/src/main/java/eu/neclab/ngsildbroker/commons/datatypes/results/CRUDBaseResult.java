package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.Map;

import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.mutiny.core.MultiMap;

public abstract class CRUDBaseResult {
	private String endpoint;
	private MultiMap headers;
	private String cSourceId;
	protected Map<String, Object> json = Maps.newHashMap();

	public CRUDBaseResult(String endpoint, MultiMap headers, String cSourceId) {
		super();
		this.endpoint = endpoint;
		this.headers = headers;
		this.cSourceId = cSourceId;
		if (endpoint != null) {
			json.put(NGSIConstants.ERROR_DETAIL_ENDPOINT, endpoint);
		}
		if (cSourceId != null) {
			json.put(NGSIConstants.ERROR_DETAIL_CSOURCE_ID, cSourceId);
		}

	}

	public String getEndpoint() {
		return endpoint;
	}

	public MultiMap getHeaders() {
		return headers;
	}

	public String getcSourceId() {
		return cSourceId;
	}

	public abstract Object getJson();
}
