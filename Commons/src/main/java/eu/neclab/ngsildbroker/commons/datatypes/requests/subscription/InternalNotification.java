package eu.neclab.ngsildbroker.commons.datatypes.requests.subscription;

import java.io.Serializable;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

public class InternalNotification implements Serializable{

	

	/**
	 * 
	 */
	private static final long serialVersionUID = -6832696880800268499L;
	private Map<String, Object> payload;
	private String id;
	private String tenant;

	public InternalNotification() {
		// for serialzation
	}

	public InternalNotification(String tenant, String subscriptionId, Map<String, Object> notification) {
		this.tenant = tenant;
		this.id = subscriptionId;
		this.payload = notification;
	}

	public Map<String, Object> getPayload() {
		return payload;
	}

	public void setPayload(Map<String, Object> payload) {
		this.payload = payload;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	@Override
	public String toString() {
		return "InternalNotification [payload=" + payload + ", id=" + id + ", tenant=" + tenant + "]";
	}
	
	

}
