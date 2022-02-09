package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.jsonldjava.core.Context;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;

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
	private List<URI> requestorList;
	private Boolean isActive;
	private List<String> attributeNames;
	private List<EntityInfo> entities;
	private Context ldContext;
	private String ldQueryString;
	private String scopeQueryString;
	private String csfQueryString;
	private LDGeoQuery ldGeoQuery;
	private LDTemporalQuery ldTempQuery;

	@JsonIgnore
	private QueryTerm ldQuery;
	@JsonIgnore
	private QueryTerm csfQuery;
	@JsonIgnore
	private ScopeQueryTerm scopeQuery;

	// duplicate
	public Subscription(Subscription subscription) {
		this.description = subscription.description;
		if (this.expiresAt != null) {
			this.expiresAt = subscription.expiresAt.longValue();
		}
		this.id = subscription.id;
		this.subscriptionName = subscription.subscriptionName;
		this.notification = new NotificationParam(subscription.notification);
		this.status = subscription.status;
		this.throttling = subscription.throttling.intValue();
		this.timeInterval = subscription.timeInterval.intValue();
		this.type = subscription.type;
		if (requestorList != null) {
			this.requestorList = new ArrayList<URI>(subscription.requestorList);
		}
		this.isActive = subscription.isActive;
		if (attributeNames != null) {
			this.attributeNames = new ArrayList<String>(subscription.attributeNames);
		}
		if (subscription.entities != null) {
			this.entities = new ArrayList<EntityInfo>(subscription.entities);
		}
		this.ldContext = subscription.ldContext;
		this.ldQueryString = subscription.ldQueryString;
		this.csfQueryString = subscription.csfQueryString;
		this.ldGeoQuery = subscription.ldGeoQuery;
		this.ldTempQuery = subscription.ldTempQuery;
		this.ldQuery = subscription.ldQuery;
		this.csfQuery = subscription.csfQuery;
	}

	public Subscription() {
	}

	public void update(Subscription subscription) {
		if (subscription.description != null) {
			this.description = subscription.description;
		}
		if (subscription.expiresAt != null) {
			this.expiresAt = subscription.expiresAt;
		}
		if (subscription.id != null) {
			this.id = subscription.id;
		}
		if (subscription.subscriptionName != null) {
			this.subscriptionName = subscription.subscriptionName;
		}
		if (subscription.notification != null) {
			this.notification.update(subscription.notification);
		}
		if (subscription.status != null) {
			this.status = subscription.status;
		}
		if (subscription.throttling != null) {
			this.throttling = subscription.throttling;
		}
		if (subscription.timeInterval != null) {
			this.timeInterval = subscription.timeInterval;
		}
		if (subscription.type != null) {
			this.type = subscription.type;
		}
		if (subscription.requestorList != null) {
			this.requestorList = subscription.requestorList;
		}
		if (subscription.isActive != null) {
			this.isActive = subscription.isActive;
		}
		if (subscription.attributeNames != null) {
			this.attributeNames = subscription.attributeNames;
		}
		if (subscription.entities != null) {
			this.entities = subscription.entities;
		}
		if (subscription.ldContext != null) {
			this.ldContext = subscription.ldContext;
		}
		if (subscription.ldQueryString != null) {
			this.ldQueryString = subscription.ldQueryString;
		}
		if (subscription.csfQueryString != null) {
			this.csfQueryString = subscription.csfQueryString;
		}
		if (subscription.ldGeoQuery != null) {
			this.ldGeoQuery = subscription.ldGeoQuery;
		}
		if (subscription.ldTempQuery != null) {
			this.ldTempQuery = subscription.ldTempQuery;
		}
		if (subscription.ldQuery != null) {
			this.ldQuery = subscription.ldQuery;
		}
		if (subscription.csfQuery != null) {
			this.csfQuery = subscription.csfQuery;
		}
	}

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

	public Object getLdContext() {
		return ldContext;
	}

	public void setLdContext(Context ldContext) {
		this.ldContext = ldContext;
	}

	public LDGeoQuery getLdGeoQuery() {
		return ldGeoQuery;
	}

	public void setLdGeoQuery(LDGeoQuery ldGeoQuery) {
		this.ldGeoQuery = ldGeoQuery;
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
		if (this.entities == null) {
			this.entities = new ArrayList<EntityInfo>();
		}
		this.entities.add(entity);
	}

	public void removeEntityInfo(EntityInfo entity) {
		if (this.entities == null) {
			return;
		}
		this.entities.remove(entity);
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

	public QueryTerm getLdQuery() {
		return ldQuery;
	}

	public void finalize() throws Throwable {

	}

	public Boolean isActive() {
		return isActive;
	}

	public void setActive(Boolean isActive) {
		this.isActive = isActive;
		if (isActive != null) {
			if (isActive) {
				this.status = "active";
			} else {
				this.status = "paused";
			}
		}
	}

	public QueryTerm getCsf() {
		return csfQuery;
	}

	public String getScopeQueryString() {
		return scopeQueryString;
	}

	public void setScopeQueryString(String scopeQueryString) throws ResponseException {
		this.scopeQueryString = scopeQueryString;
		if (scopeQueryString != null) {
			this.scopeQuery = QueryParser.parseScopeQuery(scopeQueryString);
		} else {
			this.scopeQuery = null;
		}
	}

	public String getLdQueryString() {
		return ldQueryString;
	}

	public void setLdQueryString(String ldQueryString) throws ResponseException {
		this.ldQueryString = ldQueryString;
		if (ldQueryString != null) {
			this.ldQuery = QueryParser.parseQuery(ldQueryString, ldContext);
		} else {
			this.ldQuery = null;
		}
	}

	public String getCsfQueryString() {
		return csfQueryString;
	}

	public void setCsfQueryString(String csfQueryString) throws ResponseException {
		this.csfQueryString = csfQueryString;
		if (csfQueryString != null) {
			this.csfQuery = QueryParser.parseQuery(csfQueryString, ldContext);
		} else {
			this.csfQuery = null;
		}
	}


	@Override
	public String toString() {
		return "Subscription [description=" + description + ", expiresAt=" + expiresAt + ", id=" + id
				+ ", subscriptionName=" + subscriptionName + ", notification=" + notification + ", status=" + status
				+ ", throttling=" + throttling + ", timeInterval=" + timeInterval + ", type=" + type
				+ ", requestorList=" + requestorList + ", isActive=" + isActive + ", attributeNames=" + attributeNames
				+ ", entities=" + entities + ", ldContext=" + ldContext + ", ldQueryString=" + ldQueryString
				+ ", csfQueryString=" + csfQueryString + ", ldGeoQuery=" + ldGeoQuery + ", ldTempQuery=" + ldTempQuery
				+ ", ldQuery=" + ldQuery + ", csfQuery=" + csfQuery + "]";

	public Boolean getIsActive() {
		return isActive;
	}

	public QueryTerm getCsfQuery() {
		return csfQuery;
	}

	public ScopeQueryTerm getScopeQuery() {
		return scopeQuery;

	}

}
