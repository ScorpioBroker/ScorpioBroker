package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.enums.Format;

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
	private Date lastNotification;
	private Date lastSuccessfulNotification;
	private Date lastFailedNotification;

	// duplicate
	public NotificationParam(NotificationParam notification) {
		if (notification.attributeNames != null) {
			this.attributeNames = Sets.newHashSet(notification.attributeNames);
		}
		this.endPoint = new EndPoint(notification.endPoint);
		this.format = notification.format;
		this.timesSent = notification.timesSent;
		if (notification.lastNotification != null) {
			this.lastNotification = Date.from(notification.lastNotification.toInstant());
		}
		if (notification.lastSuccessfulNotification != null) {
			this.lastSuccessfulNotification = Date.from(notification.lastSuccessfulNotification.toInstant());
		}
		if (notification.lastFailedNotification != null) {
			this.lastFailedNotification = Date.from(notification.lastFailedNotification.toInstant());
		}
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

		if (notification.lastNotification != null) {
			this.lastNotification = Date.from(notification.lastNotification.toInstant());
		}
		if (notification.lastSuccessfulNotification != null) {
			this.lastSuccessfulNotification = Date.from(notification.lastSuccessfulNotification.toInstant());
		}
		if (notification.lastFailedNotification != null) {
			this.lastFailedNotification = Date.from(notification.lastFailedNotification.toInstant());
		}
		return this;
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
		return "NotificationParam [attributeNames=" + attributeNames + ", endPoint=" + endPoint + ", format=" + format
				+ ", timesSent=" + timesSent + ", lastNotification=" + lastNotification
				+ ", lastSuccessfulNotification=" + lastSuccessfulNotification + ", lastFailedNotification="
				+ lastFailedNotification + "]";
	}

}