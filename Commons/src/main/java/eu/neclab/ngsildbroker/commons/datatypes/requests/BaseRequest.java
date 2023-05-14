package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

import java.util.HashMap;
import java.util.Map;

public class BaseRequest {

	private String tenant;
	protected Map<String, Object> payload = new HashMap<>();
	private int requestType;
	private long sendTimestamp = System.currentTimeMillis();
	private BatchInfo batchInfo;
	private String id;

	public BaseRequest() {

	}

	protected BaseRequest(String tenant, String id, Map<String, Object> requestPayload, BatchInfo batchInfo,
			int requestType) {
		super();
		this.tenant = tenant;
		this.requestType = requestType;
		this.batchInfo = batchInfo;
		this.id = id;
		this.payload = requestPayload;
	}

	public int getRequestType() {
		return requestType;
	}

	public long getSendTimestamp() {
		return sendTimestamp;
	}

	public void setSendTimestamp(long sendTimestamp) {
		this.sendTimestamp = sendTimestamp;
	}

	public void setRequestType(int requestType) {
		this.requestType = requestType;
	}

	public BatchInfo getBatchInfo() {
		return batchInfo;
	}

	public void setBatchInfo(BatchInfo batchInfo) {
		this.batchInfo = batchInfo;
	}

	public Map<String, Object> getPayload() {
		return payload;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
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

}
