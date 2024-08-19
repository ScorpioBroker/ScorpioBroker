package eu.neclab.ngsildbroker.commons.datatypes;

import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.enums.Format;

import java.io.Serializable;

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
	private AttrsQueryTerm attrs;
	private PickTerm pick;
	private OmitTerm omit;
	private int joinLevel = -1;
	private String join;
	private EndPoint endPoint;
	private Format format;
	private int timesSent = 0;
	private long lastNotification;
	private long lastSuccessfulNotification;
	private long lastFailedNotification;
	private Boolean showChanges = false;
	private Boolean sysAttrs = false;

	// duplicate
	public NotificationParam(NotificationParam notification) {
		if (notification.attrs != null) {
			this.attrs = notification.attrs;
		}
		this.endPoint = new EndPoint(notification.endPoint);
		this.format = notification.format;
		this.timesSent = notification.timesSent;
	}

	public NotificationParam() {
	}

	public NotificationParam update(NotificationParam notification) {
		if (notification.attrs != null) {
			this.attrs = notification.attrs;
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

	public Boolean getShowChanges() {
		return showChanges;
	}

	public void setShowChanges(Boolean showChanges) {
		this.showChanges = showChanges;
	}

	public Boolean getSysAttrs() {
		return sysAttrs;
	}

	public void setSysAttrs(Boolean sysAttrs) {
		this.sysAttrs = sysAttrs;
	}

	public AttrsQueryTerm getAttrs() {
		return attrs;
	}

	public void setAttrs(AttrsQueryTerm attrs) {
		this.attrs = attrs;
	}

	public PickTerm getPick() {
		return pick;
	}

	public void setPick(PickTerm pick) {
		this.pick = pick;
	}

	public OmitTerm getOmit() {
		return omit;
	}

	public void setOmit(OmitTerm omit) {
		this.omit = omit;
	}

	public int getJoinLevel() {
		return joinLevel;
	}

	public void setJoinLevel(int joinLevel) {
		this.joinLevel = joinLevel;
	}

	public String getJoin() {
		return join;
	}

	public void setJoin(String join) {
		this.join = join;
	}
	

}