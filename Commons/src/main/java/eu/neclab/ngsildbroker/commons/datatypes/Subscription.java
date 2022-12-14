package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.Format;
import eu.neclab.ngsildbroker.commons.enums.Geometry;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:23
 */
public class Subscription {

	static final JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
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

	public void setLdQueryString(String ldQueryString, Context ldContext) throws ResponseException {
		if (ldQueryString.strip().isBlank()) {
			ldQueryString = null;
		}
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

	public void setCsfQueryString(String csfQueryString, Context ldContext) throws ResponseException {
		if (csfQueryString.strip().isBlank()) {
			csfQueryString = null;
		}
		this.csfQueryString = csfQueryString;
		if (csfQueryString != null) {
			this.csfQuery = QueryParser.parseQuery(csfQueryString, ldContext);
		} else {
			this.csfQuery = null;
		}
	}

	public Boolean getIsActive() {
		return isActive;
	}

	public QueryTerm getCsfQuery() {
		return csfQuery;
	}

	public ScopeQueryTerm getScopeQuery() {
		return scopeQuery;
	}

	public Map<String, Object> toJson() {
		Map<String, Object> top = Maps.newHashMap();
		if (getId() != null) {
			top.put(NGSIConstants.JSON_LD_ID, getId());
		}
		top.put(NGSIConstants.JSON_LD_TYPE, Lists.newArrayList(getType()));
		List<Object> temp = Lists.newArrayList();
		if (getEntities() != null) {
			for (EntityInfo info : getEntities()) {
				Map<String, Object> entityObj = Maps.newHashMap();
				if (info.getId() != null) {
//					List<Object> temp2 = Lists.newArrayList();
//					temp2.add(info.getId().toString());
					entityObj.put(NGSIConstants.JSON_LD_ID, info.getId().toString());// temp2);
				}
				if (info.getType() != null) {
					List<Object> temp2 = Lists.newArrayList();
					temp2.add(info.getType());
					entityObj.put(NGSIConstants.JSON_LD_TYPE, temp2);
				}
				if (info.getIdPattern() != null) {
					List<Object> temp2 = Lists.newArrayList();
					Map<String, Object> tempObj = Maps.newHashMap();
					tempObj.put(NGSIConstants.JSON_LD_VALUE, info.getIdPattern());
					temp2.add(tempObj);
					entityObj.put(NGSIConstants.NGSI_LD_ID_PATTERN, temp2);
				}
				temp.add(entityObj);
			}
			if (temp.size() > 0) {
				top.put(NGSIConstants.NGSI_LD_ENTITIES, temp);
			}
		}
		if (getLdGeoQuery() != null) {
			temp = Lists.newArrayList();
			Map<String, Object> geoObj = Maps.newHashMap();
			List<Object> coordArray = Lists.newArrayList();
			for (Double coordinate : getLdGeoQuery().getCoordinates()) {
				Map<String, Object> tempObj = Maps.newHashMap();
				tempObj.put(NGSIConstants.JSON_LD_VALUE, coordinate);
				coordArray.add(tempObj);
			}
			geoObj.put(NGSIConstants.NGSI_LD_COORDINATES, coordArray);
			List<Object> temp2 = Lists.newArrayList();
			Map<String, Object> tempObj = Maps.newHashMap();
			tempObj.put(NGSIConstants.JSON_LD_VALUE, getLdGeoQuery().getGeometry().toString());
			temp2.add(tempObj);
			geoObj.put(NGSIConstants.NGSI_LD_GEOMETRY, temp2);
			if (getLdGeoQuery().getGeoRelation() != null) {
				temp2 = Lists.newArrayList();
				tempObj = Maps.newHashMap();
				tempObj.put(NGSIConstants.JSON_LD_VALUE, getLdGeoQuery().getGeoRelation().getABNFString());
				temp2.add(tempObj);
				geoObj.put(NGSIConstants.NGSI_LD_GEO_REL, temp2);
			}
			temp.add(geoObj);
			top.put(NGSIConstants.NGSI_LD_GEO_QUERY, temp);
		}
		temp = Lists.newArrayList();
		Map<String, Object> notificationObj = Maps.newHashMap();
		List<Object> attribs = Lists.newArrayList();
		Map<String, Object> tempObj;
		List<Object> tempArray;
		if (getNotification() != null) {
			NotificationParam notification = getNotification();
			if (notification.getAttributeNames() != null && !notification.getAttributeNames().isEmpty()) {
				for (String attrib : notification.getAttributeNames()) {
					tempObj = Maps.newHashMap();
					tempObj.put(NGSIConstants.JSON_LD_ID, attrib);
					attribs.add(tempObj);
				}
				notificationObj.put(NGSIConstants.NGSI_LD_ATTRIBUTES, attribs);
			}

			Map<String, Object> endPoint = Maps.newHashMap();
			List<Object> endPointArray = Lists.newArrayList();

			if (notification.getEndPoint() != null) {

				if (notification.getEndPoint().getAccept() != null) {
					tempArray = Lists.newArrayList();
					tempObj = Maps.newHashMap();
					tempObj.put(NGSIConstants.JSON_LD_VALUE, notification.getEndPoint().getAccept());
					tempArray.add(tempObj);
					endPoint.put(NGSIConstants.NGSI_LD_ACCEPT, tempArray);
				}
				if (notification.getEndPoint().getUri() != null) {
					tempArray = Lists.newArrayList();
					tempObj = Maps.newHashMap();
					tempObj.put(NGSIConstants.JSON_LD_VALUE, notification.getEndPoint().getUri().toString());
					tempArray.add(tempObj);
					endPoint.put(NGSIConstants.NGSI_LD_URI, tempArray);
				}
				// add endpoint notification notifierInfo for serialization
				if (notification.getEndPoint().getNotifierInfo() != null) {
					Map<String, Object> notifierEndPoint = Maps.newHashMap();
					List<Object> notifierEndPointArray = Lists.newArrayList();
					if (notification.getEndPoint().getNotifierInfo().get(NGSIConstants.MQTT_QOS) != null) {
						tempArray = Lists.newArrayList();
						tempObj = Maps.newHashMap();
						tempObj.put(NGSIConstants.JSON_LD_VALUE,
								notification.getEndPoint().getNotifierInfo().get(NGSIConstants.MQTT_QOS));
						tempArray.add(tempObj);
						notifierEndPoint.put(NGSIConstants.NGSI_LD_MQTT_QOS, tempArray);
					}
					if (notification.getEndPoint().getNotifierInfo().get(NGSIConstants.MQTT_VERSION) != null) {
						tempArray = Lists.newArrayList();
						tempObj = Maps.newHashMap();
						tempObj.put(NGSIConstants.JSON_LD_VALUE,
								notification.getEndPoint().getNotifierInfo().get(NGSIConstants.MQTT_VERSION));
						tempArray.add(tempObj);
						notifierEndPoint.put(NGSIConstants.NGSI_LD_MQTT_VERSION, tempArray);
					}

					notifierEndPointArray.add(notifierEndPoint);
					endPoint.put(NGSIConstants.NGSI_LD_NOTIFIERINFO, notifierEndPointArray);

				}
				if (notification.getEndPoint().getReceiverInfo() != null) {
					List<Object> receiverInfoArray = Lists.newArrayListWithCapacity(1);
					for (Entry<String, String> entry : notification.getEndPoint().getReceiverInfo().entries()) {
						Map<String, Object> receiverInfo = Maps.newHashMap();
						receiverInfo.put(entry.getKey(), entry.getValue());
						receiverInfoArray.add(receiverInfo);
					}
					endPoint.put(NGSIConstants.NGSI_LD_RECEIVERINFO, receiverInfoArray);
				}
				endPointArray.add(endPoint);
				notificationObj.put(NGSIConstants.NGSI_LD_ENDPOINT, endPointArray);

			}
			if (notification.getFormat() != null) {
				tempArray = Lists.newArrayList();
				tempObj = Maps.newHashMap();
				tempObj.put(NGSIConstants.JSON_LD_VALUE, notification.getFormat().toString());
				tempArray.add(tempObj);
				notificationObj.put(NGSIConstants.NGSI_LD_FORMAT, tempArray);
			}
			if (notification.getLastFailedNotification() != null) {
//				tempArray = Lists.newArrayList();
//				tempObj = Maps.newHashMap();
//				tempObj.put(NGSIConstants.JSON_LD_VALUE,
//						SerializationTools.formatter.format(notification.getLastFailedNotification().toInstant()));
//				tempObj.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
//				tempArray.add(tempObj);
				notificationObj.put(NGSIConstants.NGSI_LD_LAST_FAILURE,
						SerializationTools.formatter.format(notification.getLastFailedNotification().toInstant()));
			}
			if (notification.getLastNotification() != null) {
				tempArray = Lists.newArrayList();
				tempObj = Maps.newHashMap();
				tempObj.put(NGSIConstants.JSON_LD_VALUE,
						SerializationTools.formatter.format(notification.getLastNotification().toInstant()));
				tempObj.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
				tempArray.add(tempObj);
				notificationObj.put(NGSIConstants.NGSI_LD_LAST_NOTIFICATION, tempArray);
			}
			if (notification.getLastSuccessfulNotification() != null) {
				tempArray = Lists.newArrayList();
				tempObj = Maps.newHashMap();
				tempObj.put(NGSIConstants.JSON_LD_VALUE,
						SerializationTools.formatter.format(notification.getLastSuccessfulNotification().toInstant()));
				tempObj.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
				tempArray.add(tempObj);
				notificationObj.put(NGSIConstants.NGSI_LD_LAST_SUCCESS, tempArray);
			}
			if (notification.getTimesSent() > 0) {
				notificationObj.put(NGSIConstants.NGSI_LD_TIMES_SEND,
						SerializationTools.getValueArray(notification.getTimesSent()));
			}
			// {
			// "https://uri.etsi.org/ngsi-ld/lastSuccess": [
			// {
			// "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
			// "@value": "2020-04-04T12:03:04Z"
			// }
			// ]
			// }
			//
			// {
			// "https://uri.etsi.org/ngsi-ld/timesSent": [
			// {
			// "@value": "2020-04-04T12:03:04Z"
			// }
			// ]
			// }
			temp.add(notificationObj);
			top.put(NGSIConstants.NGSI_LD_NOTIFICATION, temp);
		}
		if (getLdQueryString() != null) {
			tempArray = Lists.newArrayList();
			tempObj = Maps.newHashMap();
			tempObj.put(NGSIConstants.JSON_LD_VALUE, getLdQueryString());
			tempArray.add(tempObj);
			top.put(NGSIConstants.NGSI_LD_QUERY, tempArray);
		}
		if (getScopeQueryString() != null) {
			tempArray = Lists.newArrayList();
			tempObj = Maps.newHashMap();
			tempObj.put(NGSIConstants.JSON_LD_VALUE, getScopeQueryString());
			tempArray.add(tempObj);
			top.put(NGSIConstants.NGSI_LD_SCOPE_Q, tempArray);
		}

		attribs = Lists.newArrayList();
		if (getAttributeNames() != null) {
			for (String attrib : getAttributeNames()) {
				tempObj = Maps.newHashMap();
				tempObj.put(NGSIConstants.JSON_LD_ID, attrib);
				attribs.add(tempObj);
			}
		}
		if (attribs.size() > 0) {
			top.put(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES, attribs);
		}
		if (getThrottling() != null && getThrottling() > 0) {
			top.put(NGSIConstants.NGSI_LD_THROTTLING, SerializationTools.getValueArray(getThrottling()));
		}
		if (getTimeInterval() != null && getTimeInterval() != 0) {
			top.put(NGSIConstants.NGSI_LD_TIME_INTERVAL, SerializationTools.getValueArray(getTimeInterval()));
		}
		if (getExpiresAt() != null) {
			// top.add(NGSIConstants.NGSI_LD_EXPIRES, SerializationTools
			// .getValueArray(SerializationTools.formatter.format(Instant.ofEpochMilli(getExpiresAt()))));
			List<Object> array = Lists.newArrayListWithCapacity(1);
			Map<String, Object> tmp = Maps.newHashMap();
			tmp.put(NGSIConstants.JSON_LD_VALUE,
					SerializationTools.formatter.format(Instant.ofEpochMilli(getExpiresAt())));
			tmp.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
			top.put(NGSIConstants.NGSI_LD_EXPIRES, array);
		}
		if (getStatus() != null) {
			top.put(NGSIConstants.NGSI_LD_STATUS, SerializationTools.getValueArray(getStatus()));
		}
		if (getDescription() != null) {
			top.put(NGSIConstants.NGSI_LD_DESCRIPTION, SerializationTools.getValueArray(getDescription()));
		}
		if (getSubscriptionName() != null) {
			top.put(NGSIConstants.NGSI_LD_SUBSCRIPTION_NAME, SerializationTools.getValueArray(getSubscriptionName()));
		}
		if (isActive() != null) {
			top.put(NGSIConstants.NGSI_LD_IS_ACTIVE, SerializationTools.getValueArray(isActive()));
		}

		return top;

	}

	public String toJsonString() {
		try {
			return JsonUtils.toPrettyString(toJson());
		} catch (IOException e) {
			return "";
		}
	}

	public static Subscription fromJsonString(String value, Context context, boolean update) throws ResponseException {
		Map<String, Object> tmp;
		try {
			tmp = (Map<String, Object>) JsonUtils.fromString(value);
		} catch (IOException e) {
			throw new ResponseException(ErrorType.InvalidRequest, "Failed to parse Subscription");
		}

		return expandSubscription(tmp, context, update);
	}

	@SuppressWarnings("unchecked")
	public static Subscription expandSubscription(Map<String, Object> body, Context context, boolean update)
			throws ResponseException {
		Subscription subscription = new Subscription();

		for (Entry<String, Object> mapEntry : body.entrySet()) {
			String key = mapEntry.getKey();
			Object mapValue = mapEntry.getValue();

			switch (key) {
				case NGSIConstants.JSON_LD_ID:
					subscription.setId((String) mapValue);
					break;
				case NGSIConstants.JSON_LD_TYPE:
					if (mapValue instanceof String) {
						subscription.setType((String) mapValue);
					} else if (mapValue instanceof List) {
						subscription.setType(((List<String>) mapValue).get(0));
					}
					break;
				case NGSIConstants.NGSI_LD_ENTITIES:
					List<EntityInfo> entities = new ArrayList<EntityInfo>();
					List<Map<String, Object>> list = (List<Map<String, Object>>) mapValue;
					boolean hasType;
					for (Map<String, Object> entry : list) {
						EntityInfo entityInfo = new EntityInfo();
						hasType = false;
						for (Entry<String, Object> entitiesEntry : entry.entrySet()) {
							switch (entitiesEntry.getKey()) {
								case NGSIConstants.JSON_LD_ID:
									try {
										entityInfo.setId(new URI((String) entitiesEntry.getValue()));
									} catch (URISyntaxException e) {
										// Left empty intentionally is already checked
									}
									break;
								case NGSIConstants.JSON_LD_TYPE:
									hasType = true;
									entityInfo.setType(((List<String>) entitiesEntry.getValue()).get(0));
									break;
								case NGSIConstants.NGSI_LD_ID_PATTERN:
									entityInfo.setIdPattern(
											(String) ((List<Map<String, Object>>) entitiesEntry.getValue()).get(0)
													.get(NGSIConstants.JSON_LD_VALUE));
									break;
								default:
									throw new ResponseException(ErrorType.BadRequestData, "Unknown entry for entities");
							}
						}
						if (!hasType) {
							throw new ResponseException(ErrorType.BadRequestData, "Entities entry needs type");
						}
						entities.add(entityInfo);
					}
					subscription.setEntities(entities);
					break;
				case NGSIConstants.NGSI_LD_GEO_QUERY:
					try {
						LDGeoQuery ldGeoQuery = getGeoQuery(((List<Map<String, Object>>) mapValue).get(0), context);
						subscription.setLdGeoQuery(ldGeoQuery);
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData, "Failed to parse geoQ");
					}
					break;
				case NGSIConstants.NGSI_LD_NOTIFICATION:
					try {
						NotificationParam notification = getNotificationParam(
								((List<Map<String, Object>>) mapValue).get(0), context);
						subscription.setNotification(notification);
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData,
								"Failed to parse notification parameter.\n" + e.getMessage());
					}
					break;
				case NGSIConstants.NGSI_LD_QUERY:
					try {
						subscription.setLdQueryString(
								(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE),
								context);
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData, "Failed to parse q");
					}
					break;
				case NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES:
					try {
						subscription.setAttributeNames(getAttribs((List<Map<String, Object>>) mapValue));
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData,
								"Failed to parse watched attributes " + mapValue);
					}
					break;
				case NGSIConstants.NGSI_LD_THROTTLING:
					try {
						subscription.setThrottling((Integer) ((List<Map<String, Object>>) mapValue).get(0)
								.get(NGSIConstants.JSON_LD_VALUE));
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData, "Failed to parse throtteling");
					}
					break;
				case NGSIConstants.NGSI_LD_TIME_INTERVAL:
					try {
						subscription.setTimeInterval((Integer) ((List<Map<String, Object>>) mapValue).get(0)
								.get(NGSIConstants.JSON_LD_VALUE));
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData, "Failed to parse timeinterval");
					}
					break;
				case NGSIConstants.NGSI_LD_EXPIRES:
					try {
						subscription.setExpiresAt(
								SerializationTools.date2Long((String) ((List<Map<String, Object>>) mapValue).get(0)
										.get(NGSIConstants.JSON_LD_VALUE)));
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData, "Failed to parse expiresAt");
					}
					break;
				case NGSIConstants.NGSI_LD_STATUS:
					try {
						subscription.setStatus((String) ((List<Map<String, Object>>) mapValue).get(0)
								.get(NGSIConstants.JSON_LD_VALUE));
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData, "Failed to parse status");
					}
					break;
				case NGSIConstants.NGSI_LD_DESCRIPTION:
					try {
						subscription.setDescription((String) ((List<Map<String, Object>>) mapValue).get(0)
								.get(NGSIConstants.JSON_LD_VALUE));
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData, "Failed to parse status");
					}
					break;
				case NGSIConstants.NGSI_LD_IS_ACTIVE:
					try {
						subscription.setActive((Boolean) ((List<Map<String, Object>>) mapValue).get(0)
								.get(NGSIConstants.JSON_LD_VALUE));
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData, "Failed to parse active state");
					}
					break;
				case NGSIConstants.NGSI_LD_SUBSCRIPTION_NAME:
					try {
						subscription.setSubscriptionName((String) ((List<Map<String, Object>>) mapValue).get(0)
								.get(NGSIConstants.JSON_LD_VALUE));
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData, "Failed to parse active state");
					}
					break;
				case NGSIConstants.NGSI_LD_CSF:
					try {
						subscription.setCsfQueryString(
								(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE),
								context);
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData, "Failed to parse csfQ");
					}

					break;
				case NGSIConstants.NGSI_LD_SCOPE_Q:
					try {
						subscription.setScopeQueryString((String) ((List<Map<String, Object>>) mapValue).get(0)
								.get(NGSIConstants.JSON_LD_VALUE));
					} catch (Exception e) {
						throw new ResponseException(ErrorType.BadRequestData, "Failed to parse scopeQ");
					}
					break;

				default:
					break;
			}
		}
		validateSub(subscription, update);
		return subscription;
	}

	private static void validateSub(Subscription subscription, boolean update) throws ResponseException {
		if (subscription.getThrottling() > 0 && subscription.getTimeInterval() > 0) {
			throw new ResponseException(ErrorType.BadRequestData, "throttling  and timeInterval cannot both be set");
		}
		if (subscription.getTimeInterval() > 0) {
			if (subscription.getAttributeNames() == null || subscription.getAttributeNames().isEmpty()) {
				return;
			}
			throw new ResponseException(ErrorType.BadRequestData,
					"watchedAttributes  and timeInterval cannot both be set");
		}
		if (update && subscription.getNotification() != null && subscription.getNotification().getEndPoint() == null) {
			throw new ResponseException(ErrorType.BadRequestData, "A subscription needs a notification endpoint entry");
		}
		if (!update
				&& (subscription.getNotification() == null || subscription.getNotification().getEndPoint() == null)) {
			throw new ResponseException(ErrorType.BadRequestData, "A subscription needs a notification endpoint entry");
		}

	}

	@SuppressWarnings("unchecked")
	public static NotificationParam getNotificationParam(Map<String, Object> map, Context context) throws Exception {
		// Default accept
		String accept = AppConstants.NGB_APPLICATION_JSONLD;
		Format format = Format.normalized;
		List<String> watchedAttribs = new ArrayList<String>();
		String mqttVersion = null;
		Integer qos = null;
		NotificationParam notifyParam = new NotificationParam();
		Map<String, String> notifierInfo = new HashMap<String, String>();
		for (Entry<String, Object> entry : map.entrySet()) {
			switch (entry.getKey()) {
				case NGSIConstants.NGSI_LD_ATTRIBUTES:
					watchedAttribs = getAttribs((List<Map<String, Object>>) entry.getValue());
					notifyParam.setAttributeNames(watchedAttribs);
					break;
				case NGSIConstants.NGSI_LD_ENDPOINT:
					EndPoint endPoint = new EndPoint();
					for (Entry<String, Object> endPointEntry : ((List<Map<String, Object>>) entry.getValue()).get(0)
							.entrySet()) {
						switch (endPointEntry.getKey()) {
							case NGSIConstants.NGSI_LD_ACCEPT:
								accept = ((List<Map<String, String>>) endPointEntry.getValue()).get(0)
										.get(NGSIConstants.JSON_LD_VALUE);
								break;
							case NGSIConstants.NGSI_LD_URI:
								URI endPointURI = validateSubEndpoint(
										((List<Map<String, String>>) endPointEntry.getValue()).get(0)
												.get(NGSIConstants.JSON_LD_VALUE));
								endPoint.setUri(endPointURI);
								break;

							case NGSIConstants.NGSI_LD_NOTIFIERINFO:

								for (Entry<String, Object> endPointNotifier : ((List<Map<String, Object>>) endPointEntry
										.getValue()).get(0).entrySet()) {
									switch (endPointNotifier.getKey()) {
										case NGSIConstants.NGSI_LD_MQTT_VERSION:
											mqttVersion = validateSubNotifierInfoMqttVersion(
													((List<Map<String, String>>) endPointNotifier.getValue()).get(0)
															.get(NGSIConstants.JSON_LD_VALUE));
											notifierInfo.put(NGSIConstants.MQTT_VERSION, mqttVersion);
											break;
										case NGSIConstants.NGSI_LD_MQTT_QOS:
											qos = validateSubNotifierInfoQos(
													((List<Map<String, Integer>>) endPointNotifier.getValue()).get(0)
															.get(NGSIConstants.JSON_LD_VALUE));
											notifierInfo.put(NGSIConstants.MQTT_QOS, String.valueOf(qos));
											break;
										default:
											notifierInfo.put(NGSIConstants.MQTT_VERSION,
													NGSIConstants.DEFAULT_MQTT_VERSION);
											notifierInfo.put(NGSIConstants.MQTT_QOS,
													String.valueOf(NGSIConstants.DEFAULT_MQTT_QOS));
									}
								}
								endPoint.setNotifierInfo(notifierInfo);
								break;
							case NGSIConstants.NGSI_LD_RECEIVERINFO:
								HashMultimap<String, String> receiverInfo = HashMultimap.create();
								Map<String, Object> compacted = JsonLdProcessor.compact(endPointEntry.getValue(), null,
										context, opts, 999);
								
								if(!compacted.containsKey(JsonLdConsts.GRAPH)) {
									List<Map<String, Object>> ls = new ArrayList<>();
									ls.add(compacted);
									compacted = new HashMap<String, Object>();
									compacted.put("@graph", ls);
								}
								
								for (Map<String, Object> headerEntry : (List<Map<String, Object>>) compacted
										.get(JsonLdConsts.GRAPH)) {
									headerEntry.forEach((t, u) -> {
										receiverInfo.put(t, u.toString());
									});

								}
								endPoint.setReceiverInfo(receiverInfo);
								break;

							default:
								throw new ResponseException(ErrorType.BadRequestData, "Unkown entry for endpoint");
						}
					}
					endPoint.setAccept(accept);
					// endPoint.setNotifierInfo(notifierInfo);
					notifyParam.setEndPoint(endPoint);
					break;
				case NGSIConstants.NGSI_LD_FORMAT:
					String formatString = (String) ((List<Map<String, Object>>) entry.getValue()).get(0)
							.get(NGSIConstants.JSON_LD_VALUE);
					if (formatString.equalsIgnoreCase("keyvalues")) {
						format = Format.keyValues;
					}
					break;
				case NGSIConstants.NGSI_LD_TIMES_SEND:
					notifyParam.setTimesSent(
							((List<Map<String, Integer>>) entry.getValue()).get(0).get(NGSIConstants.JSON_LD_VALUE));
					break;
				case NGSIConstants.NGSI_LD_LAST_FAILURE:
					if (entry.getValue() instanceof List) {
						notifyParam.setLastFailedNotification(Date.from(LocalDateTime
								.parse(((List<Map<String, String>>) entry.getValue()).get(0)
										.get(NGSIConstants.JSON_LD_VALUE), SerializationTools.informatter)
								.toInstant(ZoneOffset.UTC)));
					} else {
						notifyParam.setLastFailedNotification(
								Date.from(LocalDateTime.parse((String) entry.getValue(), SerializationTools.informatter)
										.toInstant(ZoneOffset.UTC)));
					}
					break;
				case NGSIConstants.NGSI_LD_LAST_SUCCESS:
					notifyParam.setLastSuccessfulNotification(Date.from(LocalDateTime
							.parse(((List<Map<String, String>>) entry.getValue()).get(0)
									.get(NGSIConstants.JSON_LD_VALUE), SerializationTools.informatter)
							.toInstant(ZoneOffset.UTC)));
				case NGSIConstants.NGSI_LD_LAST_NOTIFICATION:
					notifyParam.setLastNotification(Date.from(LocalDateTime
							.parse(((List<Map<String, String>>) entry.getValue()).get(0)
									.get(NGSIConstants.JSON_LD_VALUE), SerializationTools.informatter)
							.toInstant(ZoneOffset.UTC)));
					break;
				default:
					throw new ResponseException(ErrorType.BadRequestData,
							"Unkown entry for notification " + entry.getKey());
			}

		}
		notifyParam.setFormat(format);
		return notifyParam;
	}

	@SuppressWarnings("unchecked")
	public static LDGeoQuery getGeoQuery(Map<String, Object> map, Context context) throws Exception {
		LDGeoQuery geoQuery = new LDGeoQuery();
		Object geoProperty = map.get(NGSIConstants.NGSI_LD_GEOPROPERTY_GEOQ_ATTRIB);
		if (geoProperty != null) {
			geoQuery.setGeoProperty(
					context.expandIri(((List<Map<String, String>>) geoProperty).get(0).get(NGSIConstants.JSON_LD_VALUE),
							false, true, null, null));
		}
		List<Map<String, Object>> jsonCoordinates = (List<Map<String, Object>>) map
				.get(NGSIConstants.NGSI_LD_COORDINATES);

		geoQuery.setCoordinates(getCoordinates(jsonCoordinates));
		String geometry = (String) ((List<Map<String, Object>>) map.get(NGSIConstants.NGSI_LD_GEOMETRY)).get(0)
				.get(NGSIConstants.JSON_LD_VALUE);
		if (geometry.equalsIgnoreCase("point")) {
			geoQuery.setGeometry(Geometry.Point);
		} else if (geometry.equalsIgnoreCase("polygon")) {
			geoQuery.setGeometry(Geometry.Polygon);
		} else if (geometry.equalsIgnoreCase("linestring")) {
			geoQuery.setGeometry(Geometry.LineString);
		}
		String geoRelString = (String) ((List<Map<String, Object>>) map.get(NGSIConstants.NGSI_LD_GEO_REL)).get(0)
				.get(NGSIConstants.JSON_LD_VALUE);
		String[] relSplit = geoRelString.split(";");
		GeoRelation geoRel = new GeoRelation();
		geoRel.setRelation(relSplit[0]);
		for (int i = 1; i < relSplit.length; i++) {
			String[] temp = relSplit[i].split("==");
			Object distance;
			try {
				distance = Integer.parseInt(temp[1]);
			} catch (NumberFormatException e) {
				distance = Double.parseDouble(temp[1]);
			}
			if (temp[0].equalsIgnoreCase("maxDistance")) {

				geoRel.setMaxDistance(distance);
			} else if (temp[0].equalsIgnoreCase("minDistance")) {
				geoRel.setMinDistance(distance);
			}
		}
		geoQuery.setGeoRelation(geoRel);
		return geoQuery;
	}

	@SuppressWarnings("unchecked")
	public static ArrayList<Double> getCoordinates(List<Map<String, Object>> jsonCoordinates) {
		ArrayList<Double> result = new ArrayList<Double>();
		boolean lon = true;
		for (Map<String, Object> entry : jsonCoordinates) {
			for (Entry<String, Object> entry1 : entry.entrySet()) {
				String key = entry1.getKey();
				Object value = entry1.getValue();
				if (key.equals(NGSIConstants.JSON_LD_VALUE)) {
					double myValue = 0;
					if (value instanceof Double) {
						myValue = (Double) value;
					} else if (value instanceof Integer) {
						myValue = ((Integer) value).doubleValue();
					} else if (value instanceof Long) {
						myValue = ((Long) value).doubleValue();
					}
					if (lon) {
						myValue = SerializationTools.getProperLon(myValue);
					} else {
						myValue = SerializationTools.getProperLat(myValue);
					}
					result.add(myValue);
					lon = !lon;
				} else if (key.equals(NGSIConstants.JSON_LD_LIST)) {
					result.addAll(getCoordinates((List<Map<String, Object>>) value));
				}
			}
		}
		return result;
	}

	public static List<String> getAttribs(List<Map<String, Object>> entry) throws ResponseException {
		ArrayList<String> watchedAttribs = new ArrayList<String>();
		for (Map<String, Object> attribEntry : entry) {
			String temp = (String) attribEntry.get(NGSIConstants.JSON_LD_ID);
			if (temp.matches(NGSIConstants.NGSI_LD_FORBIDDEN_KEY_CHARS_REGEX)) {
				throw new ResponseException(ErrorType.BadRequestData, "Invalid character in attribute names");
			}
			watchedAttribs.add(temp);
		}
		if (watchedAttribs.isEmpty()) {
			throw new ResponseException(ErrorType.BadRequestData, "Empty watched attributes entry");
		}
		return watchedAttribs;
	}

	private static String validateSubNotifierInfoMqttVersion(String string) throws ResponseException {
		try {
			if (!Arrays.asList(NGSIConstants.VALID_MQTT_VERSION).contains(string)) {
				throw new ResponseException(ErrorType.BadRequestData, "Unsupport Mqtt version");
			}
		} catch (Exception e) {
			throw new ResponseException(ErrorType.BadRequestData, "Unsupport Mqtt version");
		}
		return string;
	}

	private static int validateSubNotifierInfoQos(Integer qos) throws ResponseException {
		try {
			if (!Arrays.asList(NGSIConstants.VALID_QOS).contains(qos)) {
				throw new ResponseException(ErrorType.BadRequestData, "Unsupport Qos");
			}
		} catch (Exception e) {
			throw new ResponseException(ErrorType.BadRequestData, "Unsupport Qos");
		}
		return qos;
	}

	private static URI validateSubEndpoint(String string) throws ResponseException {
		URI uri;
		try {
			uri = new URI(string);
			if (Arrays.binarySearch(NGSIConstants.VALID_SUB_ENDPOINT_SCHEMAS, uri.getScheme()) == -1) {
				throw new ResponseException(ErrorType.BadRequestData, "Unsupport endpoint scheme");
			}
		} catch (URISyntaxException e) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid endpoint");
		}
		return uri;
	}

}
