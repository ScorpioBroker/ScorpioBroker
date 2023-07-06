package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import com.github.jsonldjava.core.Context;

import com.github.jsonldjava.core.JsonLdConsts;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class BatchRequest extends BaseRequest {

	private String tenant;
	private List<Map<String, Object>> requestPayload;
	private List<Context> contexts;
	private List<String> entityIds;
	private int requestType;
	private long sendTimestamp = System.currentTimeMillis();

	public BatchRequest(String tenant, List<Map<String, Object>> requestPayload, List<Context> contexts,
			int requestType) {
		this.tenant = tenant;
		this.requestPayload = requestPayload;
		this.contexts = contexts;
		this.requestType = requestType;
		if (requestPayload != null) {
			List<String> ids = new ArrayList<>();
			for (Map<String, Object> entity : requestPayload) {
				ids.add((String) entity.get(NGSIConstants.JSON_LD_ID));
			}
			setEntityIds(ids);
		}
	}

	public int getRequestType() {
		return requestType;
	}

	public List<String> getEntityIds() {
		return entityIds;
	}
	public String getId(){return String.join(",",entityIds);}

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

	public Map<String,Object> getPayload(){
		if(requestPayload == null || requestPayload.isEmpty()){
			return null;
		}
		Map<String,Object> payload = new HashMap<>();
		payload.put(JsonLdConsts.GRAPH,requestPayload);
		return payload;
	}
	public List<Context> getContexts() {
		return contexts;
	}

	public void removeFromPayloadAndContext(String entityId) {

		ListIterator<Map<String, Object>> it = requestPayload.listIterator();
		ListIterator<Context> it2 = contexts.listIterator();
		ListIterator<String> it3 = entityIds.listIterator();
		while (it.hasNext()) {
			Map<String, Object> tmp = it.next();
			Context tmp2 = it2.next();
			String tmp3 = it3.next();
			if (tmp.get(NGSIConstants.JSON_LD_ID).equals(entityId)) {
				it.remove();
				it2.remove();
				it3.remove();
				break;
			}
		}

	}

	public long getSendTimestamp() {
		return sendTimestamp;
	}

}
