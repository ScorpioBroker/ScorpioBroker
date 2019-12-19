package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.Date;
import java.util.List;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class Notification extends QueryResult {
	
	
	public Notification(URI id, Long notifiedAt, URI subscriptionId, List<Entity> data) {
		super(null, null, null, -1, true);
		this.id = id;
		this.notifiedAt = notifiedAt;
		this.subscriptionId = subscriptionId;
		this.data = data;
	}
	
	
	
	public Notification(URI id, Long notifiedAt, URI subscriptionId, List<Entity> data, String errorMsg, ErrorType errorType, int shortErrorMsg, boolean success) {
		super(null, errorMsg, errorType, shortErrorMsg, success);
		this.id = id;
		this.notifiedAt = notifiedAt;
		this.subscriptionId = subscriptionId;
		this.data = data;
	}



	private URI id;
	private Long notifiedAt;
	private URI subscriptionId;
	private List<Entity> data;
	private final String type = "Notification";

	

	public URI getId() {
		return id;
	}



	public void setId(URI id) {
		this.id = id;
	}



	public Long getNotifiedAt() {
		return notifiedAt;
	}



	public void setNotifiedAt(Long notifiedAt) {
		this.notifiedAt = notifiedAt;
	}



	public URI getSubscriptionId() {
		return subscriptionId;
	}



	public void setSubscriptionId(URI subscriptionId) {
		this.subscriptionId = subscriptionId;
	}



	public String getType() {
		return type;
	}


	public List<Entity> getData() {
		return data;
	}

	public void setData(List<Entity> data) {
		this.data = data;
	}


	public void finalize() throws Throwable {

	}

}