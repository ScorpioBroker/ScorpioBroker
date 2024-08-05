package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.Serializable;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;

@JsonSerialize
@JsonDeserialize
public class SyncMessage implements Serializable {
	
	public static final int NORMAL_SUB = 0;
	public static final int REG_SUB = 1;

	/**
	 *
	 */
	private static final long serialVersionUID = 1572704886071996626L;
	private String syncId;
	private SubscriptionRequest request;
	private int subType;

	public SyncMessage(String syncId, SubscriptionRequest request, int subType) {
		super();
		this.syncId = syncId;
		this.request = request;
		this.subType = subType;
	}

	SyncMessage() {
		// TODO Auto-generated constructor stub
	}

	public int getSubType() {
		return subType;
	}

	public void setSubType(int subType) {
		this.subType = subType;
	}

	public String getSyncId() {
		return syncId;
	}

	public void setSyncId(String syncId) {
		this.syncId = syncId;
	}

	public SubscriptionRequest getRequest() {
		return request;
	}

	public void setRequest(SubscriptionRequest request) {
		this.request = request;
	}

	@Override
	public String toString() {
		return "SyncMessage [syncId=" + syncId + ", request=" + request + ", subType=" + subType + "]";
	}
	
	

}
