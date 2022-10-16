package eu.neclab.ngsildbroker.commons.datatypes;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.http.ResponseEntity;
import org.springframework.http.ResponseEntity.BodyBuilder;

import com.github.jsonldjava.core.JsonLdOptions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
	private String contentType;
	private static final JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	public Notification() {
		// for serialization
	}

	public Notification(String id, String type, Long notifiedAt, String subscriptionId, List<Map<String, Object>> data,
			int triggerReason, List<Object> context, ArrayListMultimap<String, String> headers, String contentType) {
		super();
		this.id = id;
		this.notifiedAt = notifiedAt;
		this.subscriptionId = subscriptionId;
		this.data = data;
		this.triggerReason = triggerReason;
		this.type = type;
		if (context != null) {
			this.context = Lists.newArrayList(Sets.newHashSet(context.toArray()).toArray());
		} else {
			this.context = null;
		}
		this.headers = headers;
		this.contentType = contentType;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
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

	public ResponseEntity<String> toCompactedJson() throws Exception {
		ResponseEntity<String> dataResponse = HttpUtils.generateNotification(headers, data, context, "location",
				contentType);
		StringBuilder notificationBody = new StringBuilder();
		notificationBody.append("{\n\t\"id\": \"");
		notificationBody.append(id);
		notificationBody.append("\",\n\t\"type\": \"");
		notificationBody.append(type);
		notificationBody.append("\",\n\t\"subscriptionId\": \"");
		notificationBody.append(subscriptionId);
		notificationBody.append("\",\n\t\"notifiedAt\": \"");
		notificationBody.append(SerializationTools.notifiedAt_formatter.format(Instant.ofEpochMilli(notifiedAt)));
		notificationBody.append("\",\n\t\"data\": ");
		notificationBody.append(dataResponse.getBody());
		switch (triggerReason) {
			case AppConstants.CREATE_REQUEST:
				notificationBody.append(",\n\t\"triggerReason\": \"");
				notificationBody.append(TriggerReason.newlyMatching.toString());
				notificationBody.append("\"");
				break;
			case AppConstants.APPEND_REQUEST:
				notificationBody.append("\"\n\t\"triggerReason\": \"");
				notificationBody.append(TriggerReason.updated.toString());
				notificationBody.append("\"");
				break;
			case AppConstants.UPDATE_REQUEST:
				notificationBody.append("\"\n\t\"triggerReason\": ");
				notificationBody.append(TriggerReason.updated.toString());
				notificationBody.append("\"");
				break;
			case AppConstants.DELETE_REQUEST:
				notificationBody.append("\"\n\t\"triggerReason\": ");
				notificationBody.append(TriggerReason.noLongerMatching.toString());
				notificationBody.append("\"");
				break;
			default:
				break;
		}
		notificationBody.append("\n}");

		BodyBuilder tmp = ResponseEntity.ok();
		for (Entry<String, List<String>> header : dataResponse.getHeaders().entrySet()) {
			String key = header.getKey();
			if (key.equalsIgnoreCase("Content-Type")) {
				continue;
			}
			if (key.equalsIgnoreCase("Content-Length")) {
				continue;
			}
			for (String value : header.getValue()) {
				tmp = tmp.header(key, value);
			}

		}
		if (contentType == null || contentType.isBlank()) {
			tmp = tmp.header("Content-Type", "application/json");
		} else {
			tmp = tmp.header("Content-Type", contentType);
		}
		// tmp.header("Content-Length", notificationBody.length() + "");
		return tmp.body(notificationBody.toString());
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
		if (context != null) {
			this.context = Lists.newArrayList(Sets.newHashSet(context.toArray()).toArray());
		} else {
			this.context = null;
		}
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
		result.contentType = baseNotification.contentType;
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