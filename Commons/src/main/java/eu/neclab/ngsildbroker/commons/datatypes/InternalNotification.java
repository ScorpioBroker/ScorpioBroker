package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import io.vertx.core.MultiMap;

public class InternalNotification extends Notification {

	private String tenantId;

	public InternalNotification() {
		// for serialzation
	}

	public InternalNotification(String id, String type, Long notifiedAt, String subscriptionId,
			List<Map<String, Object>> data, int triggerReason, List<Object> context, String tenantId,
			MultiMap headers) {
		super(id, type, notifiedAt, subscriptionId, data, triggerReason, context, headers);
		this.tenantId = tenantId;
	}

	public String getTenantId() {
		return tenantId;
	}

	public void setTenantId(String tenantId) {
		this.tenantId = tenantId;
	}

}
