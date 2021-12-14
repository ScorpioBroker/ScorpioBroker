package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Date;
import java.util.List;

import eu.neclab.ngsildbroker.commons.enums.Format;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class NotificationParam {

	private List<String> attributeNames;
	private EndPoint endPoint;
	private Format format;
	
	private int timesSent = 0;
	private Date lastNotification;
	private Date lastSuccessfulNotification;
	private Date lastFailedNotification;

	
	
	
	public NotificationParam(){

	}

	
	
	public int getTimesSent() {
		return timesSent;
	}



	


	public Date getLastNotification() {
		return lastNotification;
	}

	public void setTimesSent(int timeSent) {
		this.timesSent = timeSent;
	}

	public void setLastNotification(Date lastNotification) {
		this.timesSent++;
		this.lastNotification = lastNotification;
	}



	public Date getLastSuccessfulNotification() {
		return lastSuccessfulNotification;
	}



	public void setLastSuccessfulNotification(Date lastSuccessfulNotification) {
		this.lastSuccessfulNotification = lastSuccessfulNotification;
	}



	public Date getLastFailedNotification() {
		return lastFailedNotification;
	}



	public void setLastFailedNotification(Date lastFailedNotification) {
		this.lastFailedNotification = lastFailedNotification;
	}



	public void finalize() throws Throwable {

	}

	public List<String> getAttributeNames() {
		return attributeNames;
	}

	public void setAttributeNames(List<String> attributeNames) {
		this.attributeNames = attributeNames;
	}

	public EndPoint getEndPoint() {
		return endPoint;
	}

	public void setEndPoint(EndPoint endPoint) {
		this.endPoint = endPoint;
	}

	public Format getFormat() {
		return format;
	}

	public void setFormat(Format format) {
		this.format = format;
	}



	@Override
	public String toString() {
		return "NotificationParam [attributeNames=" + attributeNames + ", endPoint=" + endPoint + ", format=" + format
				+ ", timesSent=" + timesSent + ", lastNotification=" + lastNotification
				+ ", lastSuccessfulNotification=" + lastSuccessfulNotification + ", lastFailedNotification="
				+ lastFailedNotification + "]";
	}
	

}