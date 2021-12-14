package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.Date;
import java.util.List;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.TriggerReason;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class CSourceNotification extends CSourceQueryResult {

	
	private URI id;
	private Date notifiedAt;
	private TriggerReason triggerReason;
	private String type = "ContextSource Notfication";
	private URI subscriptionId;
	
	
	public CSourceNotification(URI id, URI subscriptionId, Date notifiedAt, TriggerReason triggerReason, List<CSourceRegistration> data, String errorMsg, ErrorType errorType, int shortErrorMsg,
			boolean success) {
		super(data, errorMsg, errorType, shortErrorMsg, success);
		this.id = id;
		this.notifiedAt = notifiedAt;
		this.triggerReason = triggerReason;
		this.subscriptionId = subscriptionId;
	}


	
	

	

	public URI getSubscriptionId() {
		return subscriptionId;
	}







	public void setSubscriptionId(URI subscriptionId) {
		this.subscriptionId = subscriptionId;
	}







	public URI getId() {
		return id;
	}







	public void setId(URI id) {
		this.id = id;
	}







	public Date getNotifiedAt() {
		return notifiedAt;
	}







	public void setNotifiedAt(Date notifiedAt) {
		this.notifiedAt = notifiedAt;
	}







	public TriggerReason getTriggerReason() {
		return triggerReason;
	}







	public void setTriggerReason(TriggerReason triggerReason) {
		this.triggerReason = triggerReason;
	}







	public String getType() {
		return type;
	}







	public void setType(String type) {
		this.type = type;
	}







	public void finalize() throws Throwable {

	}

}