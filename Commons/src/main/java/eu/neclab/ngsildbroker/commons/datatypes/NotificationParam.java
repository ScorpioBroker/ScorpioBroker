package eu.neclab.ngsildbroker.commons.datatypes;

import com.google.common.collect.Sets;
import eu.neclab.ngsildbroker.commons.enums.Format;

import java.io.Serializable;
import java.util.Set;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */

public class NotificationParam implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -5749495213091903926L;
	private Set<String> attributeNames;
	private EndPoint endPoint;
	private Format format;
	private int timesSent = 0;
	private long lastNotification;
	private long lastSuccessfulNotification;
	private long lastFailedNotification;
	private Boolean showChanges = false;

	// duplicate
	public NotificationParam(NotificationParam notification) {
		if (notification.attributeNames != null) {
			this.attributeNames = Sets.newHashSet(notification.attributeNames);
		}
		this.endPoint = new EndPoint(notification.endPoint);
		this.format = notification.format;
		this.timesSent = notification.timesSent;
	}

	public NotificationParam() {
	}

	public NotificationParam update(NotificationParam notification) {
		if (notification.attributeNames != null) {
			this.attributeNames = Sets.newHashSet(notification.attributeNames);
		}
		if (notification.endPoint != null) {
			this.endPoint.update(notification.endPoint);
		}
		if (notification.format != null) {
			this.format = notification.format;
		}

		this.timesSent = notification.timesSent;

		return this;
	}

	public int getTimesSent() {
		return timesSent;
	}

	public long getLastNotification() {
		return lastNotification;
	}

	public void setTimesSent(int timeSent) {
		this.timesSent = timeSent;
	}

	public void setLastNotification(long lastNotification) {
		this.lastNotification = lastNotification;
	}

	public long getLastSuccessfulNotification() {
		return lastSuccessfulNotification;
	}

	public void setLastSuccessfulNotification(long lastSuccessfulNotification) {
		this.lastSuccessfulNotification = lastSuccessfulNotification;
	}

	public long getLastFailedNotification() {
		return lastFailedNotification;
	}

	public void setLastFailedNotification(long lastFailedNotification) {
		this.lastFailedNotification = lastFailedNotification;
	}

	public void finalize() throws Throwable {

	}

	public Set<String> getAttributeNames() {
		return attributeNames;
	}

	public void setAttributeNames(Set<String> attributeNames) {
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
		return "NotificationParam [attributeNames=" + attributeNames + ", endPoint=" + endPoint + ", format=" + format +",showChanges=" + showChanges
				+ ", timesSent=" + timesSent + ", lastNotification=" + lastNotification
				+ ", lastSuccessfulNotification=" + lastSuccessfulNotification + ", lastFailedNotification="
				+ lastFailedNotification + "]";
	}

	public Boolean getShowChanges() {
		return showChanges;
	}

	public void setShowChanges(Boolean showChanges) {
		this.showChanges = showChanges;
	}
}