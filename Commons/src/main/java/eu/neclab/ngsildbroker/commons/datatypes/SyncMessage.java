package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.Serializable;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

public class SyncMessage implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 1572704886071996626L;
	private String syncId;
	private SubscriptionRequest request;

	public SyncMessage(String syncId, SubscriptionRequest request) {
		super();
		this.syncId = syncId;
		this.request = request;
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

}
