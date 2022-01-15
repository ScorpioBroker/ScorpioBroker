package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:23
 */
public class Subscription {

	private String description;
	private Long expiresAt;
	private String id;
	private String subscriptionName;
	private NotificationParam notification;
	private String status = "active";
	private Integer throttling = 0;
	private Integer timeInterval = 0;
	private String type;
	private Boolean internal = false;
	private QueryTerm queryTerm;
	private boolean isActive = true;
	protected List<String> attributeNames;
	protected List<EntityInfo> entities = new ArrayList<EntityInfo>();
	protected String ldContext;
	protected LDGeoQuery ldGeoQuery;
	protected String ldQuery;
	protected QueryTerm csf;
	protected LDTemporalQuery ldTempQuery;
	protected List<URI> requestorList;

	public List<String> getAttributeNames() {
		return attributeNames;
	}

	public void setAttributeNames(List<String> attributeNames) {
		this.attributeNames = attributeNames;
	}

	public List<EntityInfo> getEntities() {
		return entities;
	}

	public void setEntities(List<EntityInfo> entities) {
		this.entities = entities;
	}

	public String getLdContext() {
		return ldContext;
	}

	public void setLdContext(String ldContext) {
		this.ldContext = ldContext;
	}

	public LDGeoQuery getLdGeoQuery() {
		return ldGeoQuery;
	}

	public void setLdGeoQuery(LDGeoQuery ldGeoQuery) {
		this.ldGeoQuery = ldGeoQuery;
	}

	public String getLdQuery() {
		return ldQuery;
	}

	public void setLdQuery(String ldQuery) {
		this.ldQuery = ldQuery;
	}

	public LDTemporalQuery getLdTempQuery() {
		return ldTempQuery;
	}

	public void setLdTempQuery(LDTemporalQuery ldTempQuery) {
		this.ldTempQuery = ldTempQuery;
	}

	public List<URI> getRequestorList() {
		return requestorList;
	}

	public void setRequestorList(List<URI> requestorList) {
		this.requestorList = requestorList;
	}

	public void addEntityInfo(EntityInfo entity) {
		this.entities.add(entity);
	}

	public void removeEntityInfo(EntityInfo entity) {
		this.entities.remove(entity);
	}

	public Boolean isInternal() {
		return internal;
	}

	public void setInternal(Boolean internal) {
		this.internal = internal;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Long getExpiresAt() {
		return expiresAt;
	}

	public void setExpiresAt(Long expiresAt) {
		this.expiresAt = expiresAt;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSubscriptionName() {
		return subscriptionName;
	}

	public void setSubscriptionName(String subscriptionName) {
		this.subscriptionName = subscriptionName;
	}

	public NotificationParam getNotification() {
		return notification;
	}

	public void setNotification(NotificationParam notification) {
		this.notification = notification;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Integer getThrottling() {
		return throttling;
	}

	public void setThrottling(Integer throttling) {
		this.throttling = throttling;
	}

	public Integer getTimeInterval() {
		return timeInterval;
	}

	public void setTimeInterval(Integer timeInterval) {
		this.timeInterval = timeInterval;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public QueryTerm getQueryTerm() {
		return queryTerm;
	}

	public void setQueryTerm(QueryTerm queryTerm) {
		this.queryTerm = queryTerm;
	}

	public void finalize() throws Throwable {

	}

	public boolean isActive() {
		return isActive;
	}

	public void setActive(boolean isActive) {
		this.isActive = isActive;
		if (isActive) {
			this.status = "active";
		} else {
			this.status = "paused";
		}
	}

	public QueryTerm getCsf() {
		return csf;
	}

	public void setCsf(QueryTerm csf) {
		this.csf = csf;
	}

	@Override
	public String toString() {
		return "Subscription [description=" + description + ", expiresAt=" + expiresAt + ", id=" + id
				+ ", subscriptionName=" + subscriptionName + ", notification=" + notification + ", status=" + status
				+ ", throttling=" + throttling + ", timeInterval=" + timeInterval + ", type=" + type + ", internal="
				+ internal + ", queryTerm=" + queryTerm + ", attributeNames=" + attributeNames + ", entities="
				+ entities + ", ldContext=" + ldContext + ", ldGeoQuery=" + ldGeoQuery + ", ldQuery=" + ldQuery
				+ ", ldTempQuery=" + ldTempQuery + ", requestorList=" + requestorList + "]";
	}

}