package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.Serializable;
import java.util.Map;

public class CSourceBaseRequest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5061211752229129883L;
	String tenant;
	String id;
	Map<String, Object> payload;
	int requestType;
	long sendTimeStamp;

	public CSourceBaseRequest() {
		// TODO Auto-generated constructor stub
	}

	public CSourceBaseRequest(String tenant, String id, Map<String, Object> payload, int requestType) {
		this.tenant = tenant;
		this.id = id;
		this.requestType = requestType;
		this.payload = payload;
		this.sendTimeStamp = System.currentTimeMillis();
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Map<String, Object> getPayload() {
		return payload;
	}

	public void setPayload(Map<String, Object> payload) {
		this.payload = payload;
	}

	public int getRequestType() {
		return requestType;
	}

	public void setRequestType(int requestType) {
		this.requestType = requestType;
	}

	public long getSendTimeStamp() {
		return sendTimeStamp;
	}

	public void setSendTimeStamp(long sendTimeStamp) {
		this.sendTimeStamp = sendTimeStamp;
	}

}
