package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.List;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class Notification extends QueryResult {
	private String id;
	private Long notifiedAt;
	private String subscriptionId;
	private List<Map<String, Object>> data;
	private final String type = "Notification";
	
	public Notification(String id, Long notifiedAt, String subscriptionId, List<Map<String, Object>> data) {
		super(null, null, null, -1, true);
		this.id = id;
		this.notifiedAt = notifiedAt;
		this.subscriptionId = subscriptionId;
		this.data = data;
	}

	public Notification(String id, Long notifiedAt, String subscriptionId, List<Map<String, Object>> data, String errorMsg,
			ErrorType errorType, int shortErrorMsg, boolean success) {
		super(null, errorMsg, errorType, shortErrorMsg, success);
		this.id = id;
		this.notifiedAt = notifiedAt;
		this.subscriptionId = subscriptionId;
		this.data = data;
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

	public String getType() {
		return type;
	}

	public List<Map<String, Object>> getData() {
		return data;
	}

	public void setData(List<Map<String, Object>> data) {
		this.data = data;
	}

	public void finalize() throws Throwable {

	}

}