package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class BaseRequest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8749175274463827625L;
	private String tenant;
	protected Map<String, Object> payload = new HashMap<>();
	protected Object bestCompleteResult;
	private int requestType;
	private long sendTimestamp = System.currentTimeMillis();

	private String id;
	protected Object previousEntity;
	protected String attribName;
	protected String datasetId;
	protected boolean deleteAll;
	protected boolean distributed;

	public BaseRequest() {

	}

	protected BaseRequest(String tenant, String id, Map<String, Object> requestPayload, int requestType) {
		super();
		this.tenant = tenant;
		this.requestType = requestType;
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

	public void setPreviousEntity(Object previousEntity) {
		this.previousEntity = previousEntity;
	}

	public Object getPreviousEntity() {
		return previousEntity;
	}

	public String getAttribName() {
		return attribName;
	}

	public String getDatasetId() {
		return datasetId;
	}

	public boolean deleteAll() {
		return deleteAll;
	}

	public boolean isDeleteAll() {
		return deleteAll;
	}

	public void setDeleteAll(boolean deleteAll) {
		this.deleteAll = deleteAll;
	}

	public void setAttribName(String attribName) {
		this.attribName = attribName;
	}

	public void setDatasetId(String datasetId) {
		this.datasetId = datasetId;
	}

	public boolean isDistributed() {
		return distributed;
	}

	public void setDistributed(boolean distributed) {
		this.distributed = distributed;
	}

	public Object getBestCompleteResult() {
		return bestCompleteResult;
	}

	public void setBestCompleteResult(Object bestCompleteResult) {
		this.bestCompleteResult = bestCompleteResult;
	}

}
