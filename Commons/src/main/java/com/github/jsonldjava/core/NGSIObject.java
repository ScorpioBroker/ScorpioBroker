package com.github.jsonldjava.core;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

//known structures
@SuppressWarnings("unchecked")
class NGSIObject {

	private Object element;
	// entity stuff
	private boolean isProperty = false;
	private boolean isGeoProperty = false;
	private boolean isRelationship = false;
	private boolean isDateTime = false;
	private boolean hasValue = false;
	private boolean hasAtId = false;
	private boolean hasObject = false;
	private boolean hasAtType = false;
	private boolean isArray = false;
	private boolean isLdKeyWord = false;
	private boolean isScalar = false;
	// subscription stuff
	private boolean isGeoQ = false;
	private boolean isEntities = false;
	private boolean isNotificationEntry = false;
	private boolean isTemporalQ = false;
	private boolean isEndpoint = false;
	private boolean isNotifierInfo = false;
	private boolean isReceiverInfo = false;
	private boolean hasAtContext = false;
	private boolean atContextRequired = false;

	private HashSet<String> datasetIds = new HashSet<String>();
	private String id;
	private String expandedProperty;
	private NGSIObject parent;

	private HashSet<String> types = new HashSet<String>();
	private boolean fromHasValue;

	NGSIObject(Object element, NGSIObject parent) {
		super();
		this.element = element;
		this.parent = parent;

	}

	public Object getElement() {
		return element;
	}

	public String getExpandedProperty() {
		return expandedProperty;
	}

	public void setExpandedProperty(String expandedProperty) {
		this.expandedProperty = expandedProperty;
	}

	public NGSIObject setElement(Object element) throws ResponseException {
		this.element = element;
		return this;
	}

	public boolean isProperty() {
		return isProperty;
	}

	public boolean isGeoProperty() {
		return isGeoProperty;
	}

	public boolean isScalar() {
		return isScalar;
	}

	public NGSIObject setScalar(boolean isScalar) {
		this.isScalar = isScalar;
		return this;
	}

	public boolean isRelationship() {
		return isRelationship;
	}

	public boolean isHasAtValue() {
		return hasValue;
	}

	public NGSIObject setHasAtValue(boolean hasAtValue) {
		this.hasValue = hasAtValue;
		return this;
	}

	public boolean isHasAtId() {
		return hasAtId;
	}

	public NGSIObject setHasAtId(boolean hasAtId) {
		this.hasAtId = hasAtId;
		return this;
	}

	public boolean isDateTime() {
		return isDateTime;
	}

	NGSIObject setDateTime(boolean isDateTime) {
		this.isDateTime = isDateTime;
		return this;
	}

	public boolean isHasAtType() {
		return hasAtType;
	}

	NGSIObject setHasAtType(boolean hasAtType) {
		this.hasAtType = hasAtType;
		return this;
	}

	NGSIObject setArray(boolean isArray) {
		this.isArray = isArray;
		return this;
	}

	NGSIObject setLdKeyWord(boolean isLdKeyWord) {
		this.isLdKeyWord = isLdKeyWord;
		return this;
	}

	HashSet<String> getDatasetIds() {
		return datasetIds;
	}

	NGSIObject setId(String id) {
		this.id = id;
		return this;
	}

	NGSIObject setHasAtObject(boolean hasAtObject) {
		this.hasObject = hasAtObject;
		return this;
	}

	NGSIObject addType(String type) {
		this.types.add(type);
		if (NGSIConstants.NGSI_LD_PROPERTY.equals(type)) {
			this.isProperty = true;
		} else if (NGSIConstants.NGSI_LD_RELATIONSHIP.equals(type)) {
			this.isRelationship = true;
		} else if (NGSIConstants.NGSI_LD_GEOPROPERTY.equals(type)) {
			this.isGeoProperty = true;
		} else if (NGSIConstants.NGSI_LD_DATE_TIME.equals(type)) {
			this.isDateTime = true;
		}
		return this;
	}

	void validate(int payloadType, String activeProperty, String expandedProperty, JsonLdApi api)
			throws ResponseException {
		if (activeProperty == null && (atContextRequired ^ hasAtContext)) {
			throw new ResponseException(ErrorType.BadRequestData, "@Context entry is needed");
		}
		switch (payloadType) {
			case AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD:
			case AppConstants.TEMP_ENTITY_CREATE_PAYLOAD:
			case AppConstants.ENTITY_RETRIEVED_PAYLOAD:
			case AppConstants.ENTITY_CREATE_PAYLOAD:
				if (activeProperty == null) {
					// we are in root
					if (!hasAtId) {
						throw new ResponseException(ErrorType.BadRequestData, "An entity id is mandatory");
					}
					if (!hasAtType) {
						throw new ResponseException(ErrorType.BadRequestData, "An entity type is mandatory");
					}

				} else {
					validateAttribute(payloadType, expandedProperty, activeProperty, api);
				}
				break;
			case AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD:
			case AppConstants.ENTITY_UPDATE_PAYLOAD:
				if (activeProperty == null) {
					// we are in root
//				if (hasAtId) {
//					throw new ResponseException(ErrorType.BadRequestData, "An entity id is not allowed");
//				}
//				if (hasAtType) {
//					throw new ResponseException(ErrorType.BadRequestData, "An entity type is not allowed");
//				}
				} else {
					validateAttribute(payloadType, expandedProperty, activeProperty, api);
				}
				break;
			case AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD:
				if (activeProperty != null) {
					// no top level validation here needed
					validateAttribute(payloadType, expandedProperty, activeProperty, api);
				}
				break;
			case AppConstants.SUBSCRIPTION_CREATE_PAYLOAD:
				if (activeProperty == null) {
					if (!hasAtType) {
						throw new ResponseException(ErrorType.BadRequestData,
								"A subscription needs type which is Subscription");
					}
					if (!types.contains(NGSIConstants.NGSI_LD_SUBSCRIPTION)) {
						throw new ResponseException(ErrorType.BadRequestData,
								"A subscription needs type which is Subscription");
					}
					Object notification = ((Map<String, Object>) element).get(NGSIConstants.NGSI_LD_NOTIFICATION);
					if (notification == null) {
						throw new ResponseException(ErrorType.BadRequestData,
								"A subscription needs a notification entry");
					}
					validateNotificationEntry(((List<Map<String, Object>>) notification).get(0));
					Object entities = ((Map<String, Object>) element).get(NGSIConstants.NGSI_LD_ENTITIES);
					if (entities == null || ((List<Object>) entities).isEmpty()) {
						throw new ResponseException(ErrorType.BadRequestData, "A subscription needs an entities entry");
					}
				} else {
					validateSubscription(expandedProperty, activeProperty, api, payloadType);
				}
				break;
			case AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD:
				if (activeProperty != null) {
					validateSubscription(expandedProperty, activeProperty, api, payloadType);
				} else {
					Object notification = ((Map<String, Object>) element).get(NGSIConstants.NGSI_LD_NOTIFICATION);
					if (notification != null) {
						validateNotificationEntry(((List<Map<String, Object>>) notification).get(0));
					}
				}
				break;
			case AppConstants.CSOURCE_REG_CREATE_PAYLOAD:
				if (activeProperty == null) {
					if (!hasAtType) {
						throw new ResponseException(ErrorType.BadRequestData,
								"A registration needs type which is CSourceRegistration");
					}
					if (!types.contains(NGSIConstants.NGSI_LD_CSOURCE_REGISTRATION)) {
						throw new ResponseException(ErrorType.InvalidRequest,
								"A registration needs type which is CSourceRegistration");
					}
					if (!((Map<String, Object>) element).containsKey(NGSIConstants.NGSI_LD_INFORMATION)) {
						throw new ResponseException(ErrorType.BadRequestData,
								"A CSource registration needs a information entry");
					}
					if (((List<Object>) ((Map<String, Object>) element).get(NGSIConstants.NGSI_LD_INFORMATION))
							.isEmpty()) {
						throw new ResponseException(ErrorType.BadRequestData, "Information is empty!");
					}
				} else {
					validateRegistration(payloadType, expandedProperty, activeProperty, api);
				}
				break;
			case AppConstants.CSOURCE_REG_UPDATE_PAYLOAD:
				if (activeProperty != null) {
					validateRegistration(payloadType, expandedProperty, activeProperty, api);
				}
				break;
			default:
				break;
		}
	}

	private void validateNotificationEntry(Map<String, Object> notificationEntry) throws ResponseException {
		Object endPoint = notificationEntry.get(NGSIConstants.NGSI_LD_ENDPOINT);
		if (endPoint == null) {
			throw new ResponseException(ErrorType.BadRequestData, "endpoint entry is mandatory");
		}
		if (!(((List<Map<String, Object>>) endPoint).get(0).containsKey(NGSIConstants.NGSI_LD_URI))) {
			throw new ResponseException(ErrorType.BadRequestData, "endpoint requires a valid uri entry");
		}

	}

	private void validateRegistration(int payloadType, String expandedProperty, String activeProperty, JsonLdApi api)
			throws ResponseException {
		switch (expandedProperty) {
			case NGSIConstants.NGSI_LD_INFORMATION:
				// think of error scenario
				break;
			case NGSIConstants.NGSI_LD_MANAGEMENTINTERVAL:
				// think of error scenario
				break;
			case NGSIConstants.NGSI_LD_OBSERVATIONINTERVAL:
				// think of error scenario
				break;
			case NGSIConstants.NGSI_LD_LOCATION:
				// compactAndValidateGeoProperty(api);
				break;
			case NGSIConstants.NGSI_LD_TIME_STAMP:
				//
				break;
			case NGSIConstants.NGSI_LD_EXPIRES:
				validateDateTime(activeProperty);
				checkIfDataTimeIsFuture(activeProperty);
				break;
			default:
				// validateAttribute(payloadType, expandedProperty, activeProperty, api);
				break;
		}
	}

	private void checkIfDataTimeIsFuture(String activeProperty) throws ResponseException {
		Long date;
		try {
			date = SerializationTools.date2Long((String) ((Map<String, Object>) this.element).get(JsonLdConsts.VALUE));
		} catch (Exception e) {
			throw new ResponseException(ErrorType.BadRequestData, "failed to parse date time");
		}
		if (date < System.currentTimeMillis()) {
			throw new ResponseException(ErrorType.BadRequestData, activeProperty + " is in the past");
		}

	}

	private void validateSubscription(String expandedProperty, String activeProperty, JsonLdApi api, int payloadType)
			throws ResponseException {
		if (isScalar) {
			switch (expandedProperty) {
				case NGSIConstants.NGSI_LD_TIME_INTERVAL:
					if (!(this.element instanceof Map) || !(((Map<String, Object>) this.element)
							.get(NGSIConstants.JSON_LD_VALUE) instanceof Integer)) {
						throw new ResponseException(ErrorType.BadRequestData,
								"invalid entry for timeInterval. Please provide an integer");
					}
					return;
				case NGSIConstants.NGSI_LD_ID_PATTERN:
					if (!checkForEntities()) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					return;
				case NGSIConstants.NGSI_LD_EXPIRES:
					validateDateTime(activeProperty);
					checkIfDataTimeIsFuture(activeProperty);
					return;
				case NGSIConstants.NGSI_LD_COORDINATES:
					NGSIObject temp = parent;
					while (temp.isArray && temp.parent != null) {
						temp = temp.parent;
					}
					if (temp.parent == null || !temp.parent.isGeoQ) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					return;
				case NGSIConstants.NGSI_LD_GEOMETRY:
					if (this.parent == null || this.parent.parent == null || !this.parent.parent.isGeoQ) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					validateGeometry((String) ((Map<String, Object>) this.element).get(NGSIConstants.JSON_LD_VALUE));
					return;
				case NGSIConstants.NGSI_LD_GEO_REL:
					if (this.parent == null || this.parent.parent == null || !this.parent.parent.isGeoQ) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					validateGeoRel();
					return;
				case NGSIConstants.NGSI_LD_ACCEPT:
					if (this.parent == null || this.parent.parent == null || !this.parent.parent.isEndpoint) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					validateAccept();
					return;
				case NGSIConstants.NGSI_LD_URI:
					if (this.parent == null || this.parent.parent == null || !this.parent.parent.isEndpoint) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					validateEndpoint();
					return;
				case NGSIConstants.NGSI_LD_FORMAT:
					if (this.parent == null || this.parent.parent == null || !this.parent.parent.isNotificationEntry) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					validateFormat();
					return;
				case NGSIConstants.NGSI_LD_ATTRIBUTES:
					if (this.parent == null || this.parent.parent == null || this.parent.parent.parent == null
							|| !this.parent.parent.parent.isNotificationEntry) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					validateFormat();
					return;
				case NGSIConstants.NGSI_LD_END_TIME_AT:
				case NGSIConstants.NGSI_LD_TIME_AT:
					if (this.parent == null || this.parent.parent == null || !this.parent.parent.isTemporalQ) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					validateDateTime(activeProperty);
					return;
				case NGSIConstants.NGSI_LD_TIME_POPERTY:
					if (this.parent == null || this.parent.parent == null || !this.parent.parent.isTemporalQ) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					validateTimeProperty();
					return;
				case NGSIConstants.NGSI_LD_TIME_REL:
					if (this.parent == null || this.parent.parent == null || !this.parent.parent.isTemporalQ) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					validateTimeProperty();
					return;
				case NGSIConstants.NGSI_LD_GEOPROPERTY_GEOQ_ATTRIB:
				case NGSIConstants.NGSI_LD_GEOPROPERTY:
					if (this.parent == null || this.parent.parent == null || !this.parent.parent.isGeoQ) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					validateGeoproperty((String) ((Map<String, Object>) this.element).get(NGSIConstants.JSON_LD_VALUE));
					return;
				case NGSIConstants.NGSI_LD_MQTT_VERSION:
					if (this.parent == null || this.parent.parent == null || !this.parent.parent.isNotifierInfo) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					validateMQTTVersion();
					return;
				case NGSIConstants.NGSI_LD_MQTT_QOS:
					if (this.parent == null || this.parent.parent == null || !this.parent.parent.isNotifierInfo) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					validateMQTTQOS();
					return;

				default:
					if (parent != null && parent.parent != null && parent.parent.isArray && parent.parent.parent != null
							&& (parent.parent.parent.isReceiverInfo || parent.parent.parent.isNotifierInfo)) {
						// custom entries are allowed in receiver and notifier info
						return;
					}
					if (!Constants.allowedScalars.get(payloadType).contains(expandedProperty)) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					return;
			}

		} else {
			if (checkForEntities()) {
				return;
			}
			if (this.parent != null) {
				if (this.parent.isGeoQ && this.parent.parent == null) {
					return;
				}
				if (this.parent.isEndpoint && this.parent.parent != null && this.parent.parent.isNotificationEntry
						&& this.parent.parent.parent == null) {
					return;
				}
				if (this.parent.isNotificationEntry && this.parent.parent == null) {
					return;
				}
				if (this.parent.isTemporalQ && this.parent.parent == null) {
					return;
				}
				if (this.parent.parent != null
						&& (this.parent.parent.isNotifierInfo || this.parent.parent.isReceiverInfo)
						&& this.parent.parent.parent != null && parent.parent.parent.isEndpoint) {
					return;
				}

			}
			if (expandedProperty.equals(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES)) {
				if (!(this.element instanceof List) || ((List) this.element).isEmpty()) {
					throw new ResponseException(ErrorType.BadRequestData,
							"watchedAttributes has to be either a String or an array of Strings.");
				}
			}
			throw new ResponseException(ErrorType.BadRequestData,
					"The key " + activeProperty + " is an invalid entry.");
		}

	}

	private void validateGeometry(String geometry) throws ResponseException {
		switch (geometry) {
			case NGSIConstants.GEO_TYPE_POINT:
				break;
			case NGSIConstants.GEO_TYPE_LINESTRING:
				break;
			case NGSIConstants.GEO_TYPE_POLYGON:
				break;
			case NGSIConstants.GEO_TYPE_MULTI_POLYGON:
				break;
			default:
				throw new ResponseException(ErrorType.BadRequestData, "Unsupported geometry type: " + geometry);
		}

	}

	private void validateGeoproperty(String geoproperty) {
		// TODO Auto-generated method stub

	}

	private void validateTimeProperty() {
		// TODO Auto-generated method stub

	}

	private void validateFormat() {
		// TODO Auto-generated method stub

	}

	private void validateEndpoint() {
		// TODO Auto-generated method stub

	}

	private void validateAccept() {
		// TODO Auto-generated method stub

	}

	private void validateGeoRel() {
		// TODO Auto-generated method stub

	}

	private void validateMQTTQOS() {
		// TODO Auto-generated method stub

	}

	private void validateMQTTVersion() {
		// TODO Auto-generated method stub

	}

	private boolean checkForEntities() throws ResponseException {
		if (this.parent != null) {
			if (this.parent.isArray && this.parent.parent != null && this.parent.parent.isEntities) {
				if (!this.hasAtType) {
					throw new ResponseException(ErrorType.BadRequestData,
							"Entities entry in subscriptions need a type");
				}
				return true;
			} else if (this.parent.parent != null && this.parent.parent.isArray && this.parent.parent.parent != null
					&& this.parent.parent.parent.isEntities) {
				/*
				 * if (!this.parent.hasAtType) { throw new
				 * ResponseException(ErrorType.BadRequestData,
				 * "Entities entry in subscriptions need a type"); }
				 */
				return true;
			} else {
				return false;
			}

		}
		return false;

	}

	private void validateAttribute(int payloadType, String expandedProperty, String activeProperty, JsonLdApi api)
			throws ResponseException {
		if (fromHasValue) {
			return;
		}
		if (isScalar) {
			if (Constants.allowedDateTimes.get(payloadType).contains(expandedProperty)) {
				validateDateTime(activeProperty);
				return;
			}
			if (NGSIConstants.NGSI_LD_DATA_SET_ID.equals(expandedProperty)) {
				validateAndAddDatasetId();
				return;
			}
			if (Constants.allowedUrls.get(payloadType).contains(expandedProperty)) {
				validateUri();
				return;
			}
			if (!Constants.allowedScalars.get(payloadType).contains(expandedProperty)) {
				throw new ResponseException(ErrorType.BadRequestData,
						"The key " + activeProperty + " is an invalid entry.");
			}
		} else if (isArray) {
			validateArray();
		} else {
			if (isLdKeyWord && parent == null && !isProperty && !isRelationship && !isGeoProperty && !isDateTime) {
				return;
			}
			if (!isProperty && !isRelationship && !isGeoProperty && !isDateTime) {
				throw new ResponseException(ErrorType.BadRequestData,
						"The key " + activeProperty + " is an invalid entry.");
			}
			if (isProperty && !hasValue) {
				throw new ResponseException(ErrorType.BadRequestData, "You can't have properties without a value");
			}
			if ((isRelationship && !hasObject)) {
				throw new ResponseException(ErrorType.BadRequestData, "You can't have relationships without an object");
			}
			if ((isRelationship && hasValue)) {
				throw new ResponseException(ErrorType.BadRequestData, "You can't have relationships with a value");
			}
			if (isProperty && hasObject) {
				throw new ResponseException(ErrorType.BadRequestData, "You can't have properties with an object");
			}
			if (isGeoProperty) {
				if (hasObject) {
					throw new ResponseException(ErrorType.BadRequestData,
							"You can't have geoproperties with an object");
				}
				if (!hasValue) {
					throw new ResponseException(ErrorType.BadRequestData,
							"You can't have geo properties without a value");
				} else {
					handleStringGeoProperty(api);
				}
			}
		}

	}

	private void validateUri() {
		// TODO Auto-generated method stub

	}

	private void validateArray() throws ResponseException {
		if (isProperty && isRelationship) {
			throw new ResponseException(ErrorType.BadRequestData,
					"Multi value with Relationship and Property mixed is not allowed");
		}
		if (!isProperty && !isRelationship) {
			return;
		}
		HashSet<String> tmp = new HashSet<String>();
		tmp.addAll(datasetIds);
		if (tmp.size() != datasetIds.size()) {
			throw new ResponseException(ErrorType.BadRequestData,
					"Duplicated datasetId or multiple entries with no datasetId found");
		}
	}

	private void validateAndAddDatasetId() throws ResponseException {
		String dataSetId = ((Map<String, String>) element).get(JsonLdConsts.ID);
		if (dataSetId.indexOf(':') == -1) {
			throw new ResponseException(ErrorType.BadRequestData, "datasetId must be a URI");
		}
		this.datasetIds.add(dataSetId);

	}

	private void validateDateTime(String propertyName) throws ResponseException {
		String dateString = (String) ((Map<String, Object>) this.element).get(JsonLdConsts.VALUE);
		if (!AppConstants.DATE_TIME_MATCHER.matcher(dateString).matches()) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid date time format found on " + propertyName);
		}

	}

	private void handleStringGeoProperty(JsonLdApi api) throws ResponseException {
		Map<String, Object> geoPropMap = (Map<String, Object>) element;
		Map<String, Object> geoJsonValue = ((List<Map<String, Object>>) geoPropMap.get(NGSIConstants.NGSI_LD_HAS_VALUE))
				.get(0);
		Object atValue = geoJsonValue.get(NGSIConstants.JSON_LD_VALUE);
		if (atValue != null) {
			if (!(atValue instanceof String)) {
				throw new ResponseException(ErrorType.BadRequestData, "Invalid value for GeoProperty");
			}
			try {
				geoPropMap.put(NGSIConstants.NGSI_LD_HAS_VALUE,
						Arrays.asList(api.expandWithCoreContext(JsonUtils.fromString((String) atValue))));
			} catch (IOException e) {
				throw new ResponseException(ErrorType.BadRequestData, "Invalid value for GeoProperty");
			}
		}
	}

	private void validateMultiPolygon(Object geoValue) throws ResponseException {
		if (!(geoValue instanceof List)) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid multi polygon definition");
		}
		List<Object> tempList = (List<Object>) geoValue;
		for (Object entry : tempList) {
			validatePolygon(entry);
		}
	}

	private void validatePolygon(Object geoValue) throws ResponseException {
		if (!(geoValue instanceof List)) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid polygon definition");
		}
		List<Object> tempList = (List<Object>) geoValue;
		if (tempList.size() != 1) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid polygon definition");
		}
		tempList = (List<Object>) tempList.get(0);
		Object first = null, last = null;
		for (Object entry : tempList) {
			if (first == null) {
				first = entry;
			}
			last = entry;
			validatePoint(entry);
		}
		if (!first.equals(last)) {
			throw new ResponseException(ErrorType.BadRequestData, "Polygon does not close");
		}

	}

	private void validateLineString(Object geoValue) throws ResponseException {
		if (!(geoValue instanceof List)) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid line string definition");
		}
		List<Object> tempList = (List<Object>) geoValue;
		for (Object entry : tempList) {
			validatePoint(entry);
		}

	}

	private void validatePoint(Object geoValue) throws ResponseException {
		if (!(geoValue instanceof List)) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid longitude latitude pair definition");
		}
		List<Object> tempList = (List<Object>) geoValue;
		if (tempList.size() != 2 || !(tempList.get(0) instanceof Double) || !(tempList.get(1) instanceof Double)) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid longitude latitude pair definition");
		}
	}

	@Override
	public String toString() {
		return "NGSIObject [element=" + element + ", isProperty=" + isProperty + ", isGeoProperty=" + isGeoProperty
				+ ", isRelationship=" + isRelationship + ", isDateTime=" + isDateTime + ", hasAtValue=" + hasValue
				+ ", hasAtId=" + hasAtId + ", hasAtObject=" + hasObject + ", hasAtType=" + hasAtType + ", isArray="
				+ isArray + ", datasetIds=" + datasetIds + ", id=" + id + ", types=" + types + "]";
	}

	public NGSIObject setFromHasValue(boolean fromHasValue) {
		this.fromHasValue = fromHasValue;
		return this;

	}

	public boolean isFromHasValue() {
		return fromHasValue;
	}

	public boolean isGeoQ() {
		return isGeoQ;
	}

	public void setGeoQ(boolean isGeoQ) {
		this.isGeoQ = isGeoQ;
	}

	public boolean isEntities() {
		return isEntities;
	}

	public void setEntities(boolean isEntities) {
		this.isEntities = isEntities;
	}

	public boolean isNotificationEntry() {
		return isNotificationEntry;
	}

	public void setNotificationEntry(boolean isNotificationEntry) {
		this.isNotificationEntry = isNotificationEntry;
	}

	public boolean isTemporalQ() {
		return isTemporalQ;
	}

	public void setTemporalQ(boolean isTemporalQ) {
		this.isTemporalQ = isTemporalQ;
	}

	public void fillUpForArray(NGSIObject ngsiV) {
		this.isProperty = this.isProperty || ngsiV.isProperty;
		this.isGeoProperty = this.isGeoProperty || ngsiV.isGeoProperty;
		this.isRelationship = this.isRelationship || ngsiV.isRelationship;
		this.isDateTime = this.isDateTime || ngsiV.isDateTime;
		this.hasValue = this.hasValue || ngsiV.hasValue;
		this.hasAtId = this.hasAtId || ngsiV.hasAtId;
		this.hasObject = this.hasObject || ngsiV.hasObject;
		this.hasAtType = this.hasAtType || ngsiV.hasAtType;
		this.isArray = this.isArray || ngsiV.isArray;
		this.isLdKeyWord = this.isLdKeyWord || ngsiV.isLdKeyWord;
		this.isScalar = this.isScalar || ngsiV.isScalar;
		if ((ngsiV.isRelationship || ngsiV.isProperty)) {
			if (ngsiV.datasetIds.isEmpty()) {
				this.datasetIds.add(NGSIConstants.DEFAULT_DATA_SET_ID);
			} else {
				this.datasetIds.addAll(ngsiV.datasetIds);
			}
		}
	}

	public void resetSubscriptionVars() {
		isGeoQ = false;
		isEntities = false;
		isNotificationEntry = false;
		isTemporalQ = false;
		isEndpoint = false;
		isNotifierInfo = false;
		isReceiverInfo = false;
	}

	public boolean isNotifierInfo() {
		return isNotifierInfo;
	}

	public void setNotifierInfo(boolean isNotifierInfo) {
		this.isNotifierInfo = isNotifierInfo;
	}

	public boolean isReceiverInfo() {
		return isReceiverInfo;
	}

	public void setReceiverInfo(boolean isReceiverInfo) {
		this.isReceiverInfo = isReceiverInfo;
	}

	public boolean isEndpoint() {
		return isEndpoint;
	}

	public void setEndpoint(boolean isEndpoint) {
		this.isEndpoint = isEndpoint;
	}

	public boolean isHasAtContext() {
		return hasAtContext;
	}

	public void setHasAtContext(boolean hasAtContext) {
		this.hasAtContext = hasAtContext;
	}

	public boolean isAtContextRequired() {
		return atContextRequired;
	}

	public void setAtContextRequired(boolean atContextRequired) {
		this.atContextRequired = atContextRequired;
	}

}