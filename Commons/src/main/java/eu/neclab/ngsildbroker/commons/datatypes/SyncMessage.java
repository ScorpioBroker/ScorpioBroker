package eu.neclab.ngsildbroker.commons.datatypes;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection(ignoreNested = false, serialization = true)
public class SyncMessage {

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
