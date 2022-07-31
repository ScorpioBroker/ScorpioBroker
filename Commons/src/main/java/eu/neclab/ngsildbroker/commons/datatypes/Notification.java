package eu.neclab.ngsildbroker.commons.datatypes;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.google.common.collect.ArrayListMultimap;

import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.TriggerReason;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

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

	public RestResponse<String> toCompactedJson() throws Exception {
		RestResponse<String> dataResponse = HttpUtils.generateNotification(headers, data, context, "location");
		StringBuilder notificationBody = new StringBuilder();
		notificationBody.append("{\n  \"id\": \"");
		notificationBody.append(id);
		notificationBody.append("\",\n  \"type\": \"");
		notificationBody.append(type);
		notificationBody.append("\",\n  \"subscriptionId\": \"");
		notificationBody.append(subscriptionId);
		notificationBody.append("\",\n  \"notifiedAt\": \"");
		notificationBody.append(SerializationTools.formatter.format(Instant.ofEpochMilli(notifiedAt)));
		notificationBody.append("\",\n  \"data\": ");
		notificationBody.append(dataResponse.getEntity().lines().map(t -> "  " + t).collect(Collectors.joining("\n")));
		switch (triggerReason) {
			case AppConstants.CREATE_REQUEST:
				notificationBody.append(",\n  \"triggerReason\": \"");
				notificationBody.append(TriggerReason.newlyMatching.toString());
				notificationBody.append("\"");
				break;
			case AppConstants.APPEND_REQUEST:
				notificationBody.append("\"\n  \"triggerReason\": \"");
				notificationBody.append(TriggerReason.updated.toString());
				notificationBody.append("\"");
				break;
			case AppConstants.UPDATE_REQUEST:
				notificationBody.append("\"\n  \"triggerReason\": ");
				notificationBody.append(TriggerReason.updated.toString());
				notificationBody.append("\"");
				break;
			case AppConstants.DELETE_REQUEST:
				notificationBody.append("\"\n  \"triggerReason\": ");
				notificationBody.append(TriggerReason.noLongerMatching.toString());
				notificationBody.append("\"");
				break;
			default:
				break;
		}
		notificationBody.append("\n}");
		ResponseBuilder<String> builder = RestResponseBuilderImpl.ok(notificationBody.toString());
		for (Entry<String, List<Object>> entry : dataResponse.getHeaders().entrySet()) {
			for (Object value : entry.getValue()) {
				builder = builder.header(entry.getKey(), value);
			}
		}
		return builder.build();
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

}