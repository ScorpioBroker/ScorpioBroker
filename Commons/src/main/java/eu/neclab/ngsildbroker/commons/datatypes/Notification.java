package eu.neclab.ngsildbroker.commons.datatypes;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.TriggerReason;
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
	private static final JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	public Notification() {
		// for serialization
	}
	public Notification(String id, String type, Long notifiedAt, String subscriptionId, List<Map<String, Object>> data,
			int triggerReason, List<Object> context) {
		super();
		this.id = id;
		this.notifiedAt = notifiedAt;
		this.subscriptionId = subscriptionId;
		this.data = data;
		this.triggerReason = triggerReason;
		this.type = type;
		this.context = context;
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

	public String toCompactedJsonString() throws Exception {
		return JsonUtils.toPrettyString(toCompactedJson());
	}

	public Map<String, Object> toCompactedJson() throws Exception {
		HashMap<String, Object> temp = new HashMap<String, Object>();
		temp.put("id", id);
		temp.put("type", type);
		temp.put("subscriptionId", subscriptionId);
		temp.put("notifiedAt", SerializationTools.formatter.format(Instant.ofEpochMilli(notifiedAt)));
		temp.put("data", data);
		switch (triggerReason) {
		case AppConstants.CREATE_REQUEST:
			temp.put("triggerReason", TriggerReason.newlyMatching.toString());
			break;
		case AppConstants.APPEND_REQUEST:
			temp.put("triggerReason", TriggerReason.updated.toString());
			break;
		case AppConstants.UPDATE_REQUEST:
			temp.put("triggerReason", TriggerReason.updated.toString());
			break;
		case AppConstants.DELETE_REQUEST:
			temp.put("triggerReason", TriggerReason.noLongerMatching.toString());
			break;
		default:
			break;
		}

		return JsonLdProcessor.compact(temp, context, opts);
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

}