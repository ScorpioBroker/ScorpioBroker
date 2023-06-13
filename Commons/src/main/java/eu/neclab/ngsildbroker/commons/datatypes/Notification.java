package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;
import java.util.Map;
import com.google.common.collect.ArrayListMultimap;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class Notification {
	private String id;
	private Long notifiedAt;
	private String subscriptionId;
	private List<Map<String, Object>> data;
	private int triggerReason;
	private List<Object> context;
	private String type;
	private ArrayListMultimap<String, String> headers;

	public Notification() {
		// for serialization
	}

	public Notification(String id, String type, Long notifiedAt, String subscriptionId, List<Map<String, Object>> data,
			int triggerReason, List<Object> context, ArrayListMultimap<String, String> headers) {
		super();
		this.id = id;
		this.notifiedAt = notifiedAt;
		this.subscriptionId = subscriptionId;
		this.data = data;
		this.triggerReason = triggerReason;
		this.type = type;
		this.context = context;
		this.headers = headers;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getNotifiedAt() {
		return notifiedAt;
	}

	public void setNotifiedAt(Long notifiedAt) {
		this.notifiedAt = notifiedAt;
	}

	public String getSubscriptionId() {
		return subscriptionId;
	}

	public void setSubscriptionId(String subscriptionId) {
		this.subscriptionId = subscriptionId;
	}

	public List<Map<String, Object>> getData() {
		return data;
	}

	public void setData(List<Map<String, Object>> data) {
		this.data = data;
	}


	public int getTriggerReason() {
		return triggerReason;
	}

	public void setTriggerReason(int triggerReason) {
		this.triggerReason = triggerReason;
	}

	public List<Object> getContext() {
		return context;
	}

	public void setContext(List<Object> context) {
		this.context = context;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public ArrayListMultimap<String, String> getHeaders() {
		return headers;
	}

	public void setHeaders(ArrayListMultimap<String, String> headers) {
		this.headers = headers;
	}

	public static Notification copy(Notification baseNotification) {
		Notification result = new Notification();
		result.id = baseNotification.id;
		if (baseNotification.context != null) {
			result.context = List.copyOf(baseNotification.context);
		}
		if (baseNotification.data != null) {
			result.data = List.copyOf(baseNotification.data);
		}
		if (baseNotification.headers != null) {
			result.headers = ArrayListMultimap.create(baseNotification.headers);
		}
		result.notifiedAt = baseNotification.notifiedAt;
		result.subscriptionId = baseNotification.subscriptionId;
		result.triggerReason = baseNotification.triggerReason;
		result.type = baseNotification.type;
		return result;
	}

}