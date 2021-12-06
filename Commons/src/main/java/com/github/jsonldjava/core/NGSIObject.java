package com.github.jsonldjava.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class NGSIObject {

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

	private ArrayList<String> datasetIds = new ArrayList<String>();
	private String id;
	private String expandedProperty;
	private NGSIObject parent;

	private ArrayList<String> types = new ArrayList<String>();
	private boolean fromHasValue;

	public NGSIObject(Object element, NGSIObject parent) {
		super();
		this.element = element;
		this.parent = parent;

	}

	/*
	 * public NGSIObject duplicateSettings(Object newElement) { NGSIObject result =
	 * new NGSIObject(newElement); result.isProperty = isProperty;
	 * result.isGeoProperty = isGeoProperty; result.isRelationship = isRelationship;
	 * result.isDateTime = isDateTime; result.hasValue = hasValue; result.hasAtId =
	 * hasAtId; result.hasObject = hasObject; result.hasAtType = hasAtType;
	 * result.isArray = isArray; result.datasetIds = datasetIds; result.id = id;
	 * result.types = types; return result; }
	 */

	public Object getElement() {
		return element;
	}

	public String getExpandedProperty() {
		return expandedProperty;
	}

	public void setExpandedProperty(String expandedProperty) {
		this.expandedProperty = expandedProperty;
	}

	public NGSIObject setElement(Object element) {
		this.element = element;
		return this;
	}

	public boolean isProperty() {
		return isProperty;
	}

	public NGSIObject setProperty(boolean isProperty) {
		this.isProperty = isProperty;
		return this;
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

	public NGSIObject setGeoProperty(boolean isGeoProperty) {
		this.isGeoProperty = isGeoProperty;
		return this;
	}

	public boolean isRelationship() {
		return isRelationship;
	}

	public NGSIObject setRelationship(boolean isRelationship) {
		this.isRelationship = isRelationship;
		return this;
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

	public NGSIObject setDateTime(boolean isDateTime) {
		this.isDateTime = isDateTime;
		return this;
	}

	public boolean isHasAtType() {
		return hasAtType;
	}

	public NGSIObject setHasAtType(boolean hasAtType) {
		this.hasAtType = hasAtType;
		return this;
	}

	public boolean isArray() {
		return isArray;
	}

	public NGSIObject setArray(boolean isArray) {
		this.isArray = isArray;
		return this;
	}

	public boolean isLdKeyWord() {
		return isLdKeyWord;
	}

	public NGSIObject setLdKeyWord(boolean isLdKeyWord) {
		this.isLdKeyWord = isLdKeyWord;
		return this;
	}

	public ArrayList<String> getDatasetIds() {
		return datasetIds;
	}

	public NGSIObject addDatasetId(String datasetId) {
		this.datasetIds.add(datasetId);
		return this;
	}

	public String getId() {
		return id;
	}

	public NGSIObject setId(String id) {
		this.id = id;
		return this;
	}

	public ArrayList<String> getTypes() {
		return types;
	}

	public boolean isHasAtObject() {
		return hasObject;
	}

	public NGSIObject setHasAtObject(boolean hasAtObject) {
		this.hasObject = hasAtObject;
		return this;
	}

	public NGSIObject addType(String type) {
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

	public void validate(int payloadType, String activeProperty, String expandedProperty, JsonLdApi api)
			throws ResponseException {
		switch (payloadType) {
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
		case AppConstants.ENTITY_UPDATE_PAYLOAD:
			if (activeProperty == null) {
				// we are in root
				if (hasAtId) {
					throw new ResponseException(ErrorType.BadRequestData, "An entity id is allowed");
				}
				if (hasAtType) {
					throw new ResponseException(ErrorType.BadRequestData, "An entity type is not allowed");
				}
			} else {
				validateAttribute(payloadType, expandedProperty, activeProperty, api);
			}
			break;
		case AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD:
			validateAttribute(payloadType, expandedProperty, activeProperty, api);
			break;
		case AppConstants.SUBSCRIPTION_CREATE_PAYLOAD:
			if (true) {
				if (activeProperty == null) {
					if (!hasAtId) {
						throw new ResponseException(ErrorType.BadRequestData, "A subscription id is mandatory");
					}
					if (!hasAtType || !types.contains(NGSIConstants.NGSI_LD_SUBSCRIPTION)) {
						throw new ResponseException(ErrorType.BadRequestData,
								"A subscription needs type which is Subscription");
					}
				} else {
					if (isScalar) {
						switch (expandedProperty) {
						case NGSIConstants.NGSI_LD_ID_PATTERN:
							if (!checkForEntities()) {
								throw new ResponseException(ErrorType.BadRequestData,
										"The key " + activeProperty + " is an invalid entry.");
							}
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
							validateGeometry();
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
							if (this.parent == null || this.parent.parent == null
									|| !this.parent.parent.isNotificationEntry) {
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
							validateDateTime();
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
						default:
							if (parent != null && parent.parent != null && parent.parent.isArray
									&& parent.parent.parent != null
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
							if (this.parent.isEndpoint && this.parent.parent != null
									&& this.parent.parent.isNotificationEntry && this.parent.parent.parent == null) {
								return;
							}
							if (this.parent.isNotificationEntry && this.parent.parent == null) {
								return;
							}
							if (this.parent.isTemporalQ && this.parent.parent == null) {
								return;
							}
							if (this.parent.isArray && parent.parent != null
									&& (parent.parent.isNotifierInfo || parent.parent.isReceiverInfo)
									&& parent.parent.parent != null && parent.parent.parent.isEndpoint) {
								return;
							}

						}
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
				}
			}
			break;
		case AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD:
			break;
		case AppConstants.CSOURCE_REG_CREATE_PAYLOAD:
			break;
		case AppConstants.CSOURCE_REG_UPDATE_PAYLOAD:
			break;
		case AppConstants.TEMP_ENTITY_CREATE_PAYLOAD:
			break;
		case AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD:
			break;
		case AppConstants.TEMP_ENTITY_RETRIEVED_PAYLOAD:
			break;
		default:
			break;
		}
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

	private void validateGeometry() {
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
				validateDateTime();
				return;
			}
			if (NGSIConstants.NGSI_LD_DATA_SET_ID.equals(expandedProperty)) {
				validateAndAddDatasetId();
				return;
			}
			if (!Constants.allowedScalars.get(payloadType).contains(expandedProperty)) {
				throw new ResponseException(ErrorType.BadRequestData,
						"The key " + activeProperty + " is an invalid entry.");
			}
		} else if (isArray) {
			validateArray();
		} else {
			if (isLdKeyWord && !isProperty && !isRelationship && !isGeoProperty && !isDateTime) {
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
			if (isGeoProperty) {
				if (!hasValue) {
					throw new ResponseException(ErrorType.BadRequestData,
							"You can't have geo properties without a value");
				} else {
					compactAndValidateGeoProperty(api);
				}
			}
		}

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

	private void validateDateTime() throws ResponseException {
		String dateString = (String) ((Map<String, Object>) this.element).get(JsonLdConsts.VALUE);
		if (!AppConstants.DATE_TIME_MATCHER.matcher(dateString).matches()) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid date time found");
		}

	}

	private void compactAndValidateGeoProperty(JsonLdApi api) throws ResponseException {
		Map<String, Object> geoPropMap = (Map<String, Object>) element;
		Object geoJsonValue = geoPropMap.get(NGSIConstants.NGSI_LD_HAS_VALUE);
		Object potentialStringValue = ((Map<String, Object>) geoPropMap).get(NGSIConstants.JSON_LD_VALUE);
		Map<String, Object> compacted;
		if (potentialStringValue != null) {
			if (!(potentialStringValue instanceof String)) {
				throw new ResponseException(ErrorType.BadRequestData, "Invalid value for GeoProperty");
			}
			try {
				compacted = (Map<String, Object>) JsonUtils.fromString((String) potentialStringValue);
			} catch (IOException e) {
				throw new ResponseException(ErrorType.BadRequestData, "Invalid value for GeoProperty");
			}
		} else {
			compacted = api.compactWithCoreContext(geoJsonValue);
		}
		Object geometryType = compacted.get(NGSIConstants.GEO_JSON_TYPE);
		if (geometryType == null) {
			throw new ResponseException(ErrorType.BadRequestData, "No geometry type provided");
		}
		if (!(geometryType instanceof String) || !NGSIConstants.ALLOWED_GEOMETRIES.contains((String) geometryType)) {
			throw new ResponseException(ErrorType.BadRequestData,
					"Unsupported geometry type: " + geometryType.toString());
		}
		Object geoValue = compacted.get(NGSIConstants.CSOURCE_COORDINATES);
		switch ((String) geometryType) {
		case NGSIConstants.GEO_TYPE_POINT:
			validatePoint(geoValue);
			break;
		case NGSIConstants.GEO_TYPE_LINESTRING:
			validateLineString(geoValue);
			break;
		case NGSIConstants.GEO_TYPE_POLYGON:
			validatePolygon(geoValue);
			break;
		case NGSIConstants.GEO_TYPE_MULTI_POLYGON:
			validateMultiPolygon(geoValue);
			break;

		default:
			throw new ResponseException(ErrorType.BadRequestData,
					"Unsupported geometry type: " + geometryType.toString());
		}
		HashMap<String, Object> temp = new HashMap<String, Object>();
		try {
			temp.put(NGSIConstants.JSON_LD_VALUE, JsonUtils.toString(compacted));
		} catch (IOException e) {
			// Should never happen
			e.printStackTrace();
		}
		ArrayList<Object> temp1 = new ArrayList<Object>();
		temp1.add(temp);
		geoPropMap.put(NGSIConstants.NGSI_LD_HAS_VALUE, temp1);

	}

	private void validateMultiPolygon(Object geoValue) throws ResponseException {
		if (!(geoValue instanceof List)) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid multi polygon definition");
		}
		List tempList = (List) geoValue;
		for (Object entry : tempList) {
			validatePolygon(entry);
		}
	}

	private void validatePolygon(Object geoValue) throws ResponseException {
		if (!(geoValue instanceof List)) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid polygon definition");
		}
		List tempList = (List) geoValue;
		if (tempList.size() != 1) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid polygon definition");
		}
		tempList = (List) tempList.get(0);
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
		List tempList = (List) geoValue;
		for (Object entry : tempList) {
			validatePoint(entry);
		}

	}

	private void validatePoint(Object geoValue) throws ResponseException {
		if (!(geoValue instanceof List)) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid longitude latitude pair definition");
		}
		List tempList = (List) geoValue;
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

}