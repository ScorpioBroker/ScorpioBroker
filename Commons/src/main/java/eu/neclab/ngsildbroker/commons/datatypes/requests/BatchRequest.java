package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.smallrye.mutiny.tuples.Tuple2;

public class BatchRequest {

	private String tenant;
	private List<Map<String, Object>> requestPayload;
	private List<Context> contexts;
	private List<String> entityIds;

	public BatchRequest(String tenant, List<Map<String, Object>> requestPayload, List<Context> contexts,
			int requestType) {
		this.tenant = tenant;
		this.requestPayload = requestPayload;
		this.contexts = contexts;
	}

	public List<String> getEntityIds() {
		return entityIds;
	}

	public void setEntityIds(List<String> entityIds) {
		this.entityIds = entityIds;
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

	public void removeFromPayloadAndContext(String entityId) {

		ListIterator<Map<String, Object>> it = requestPayload.listIterator();
		ListIterator<Context> it2 = contexts.listIterator();
		while (it.hasPrevious()) {
			Map<String, Object> tmp = it.previous();
			Context tmp2 = it2.previous();
			if (tmp.get(NGSIConstants.JSON_LD_ID).equals(entityId)) {
				it.remove();
				it2.remove();
			}
		}

	}

}
