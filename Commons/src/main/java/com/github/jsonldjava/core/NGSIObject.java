package com.github.jsonldjava.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class NGSIObject {

	private Object element;
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

	private ArrayList<String> allowedScalarsEntity = new ArrayList<String>();
	private ArrayList<String> datasetIds = new ArrayList<String>();
	private String id;
	private ArrayList<String> types = new ArrayList<String>();
	private boolean fromHasValue;

	public NGSIObject(Object element) {
		super();
		this.element = element;
		this.allowedScalarsEntity.add(NGSIConstants.NGSI_LD_HAS_OBJECT);
		this.allowedScalarsEntity.add(NGSIConstants.NGSI_LD_HAS_VALUE);
		this.allowedScalarsEntity.add(NGSIConstants.NGSI_LD_COORDINATES);
		this.allowedScalarsEntity.add(NGSIConstants.NGSI_LD_OBSERVED_AT);
		this.allowedScalarsEntity.add(NGSIConstants.NGSI_LD_UNIT_CODE);
		this.allowedScalarsEntity.add(NGSIConstants.NGSI_LD_DATA_SET_ID);
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
		case -1:
		case AppConstants.FULL_ENTITY:
			if (activeProperty == null) {
				// we are in root
				if(element instanceof Map) {
					Map<String, Object> tmpMap = (Map<String, Object>) element;
					if(!tmpMap.containsKey(JsonLdConsts.ID)) {
						throw new ResponseException(ErrorType.BadRequestData, "An entity id is mandatory");
					}
					if(!tmpMap.containsKey(JsonLdConsts.TYPE)) {
						throw new ResponseException(ErrorType.BadRequestData, "An entity type is mandatory");
					}
				}
			} else {
				if (fromHasValue) {
					return;
				}
				if (isScalar) {
					if (!allowedScalarsEntity.contains(expandedProperty)) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
				} else {
					if (isLdKeyWord && !isProperty && !isRelationship && !isGeoProperty && !isDateTime) {
						return;
					}
					if (!isProperty && !isRelationship && !isGeoProperty && !isDateTime) {
						throw new ResponseException(ErrorType.BadRequestData,
								"The key " + activeProperty + " is an invalid entry.");
					}
					if (isProperty && !hasValue) {
						throw new ResponseException(ErrorType.BadRequestData,
								"You can't have properties without a value");
					}
					if ((isRelationship && !hasObject)) {
						throw new ResponseException(ErrorType.BadRequestData,
								"You can't have relationships without an object");
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
			break;

		default:
			break;
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
	

}
