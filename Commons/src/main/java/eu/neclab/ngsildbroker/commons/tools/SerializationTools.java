package eu.neclab.ngsildbroker.commons.tools;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.github.filosganga.geogson.model.Coordinates;
import com.github.filosganga.geogson.model.Geometry;
import com.github.filosganga.geogson.model.LineString;
import com.github.filosganga.geogson.model.Point;
import com.github.filosganga.geogson.model.Polygon;
import com.github.filosganga.geogson.model.positions.AreaPositions;
import com.github.filosganga.geogson.model.positions.LinearPositions;
import com.github.filosganga.geogson.model.positions.SinglePosition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.GeoPropertyEntry;
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.datatypes.PropertyEntry;
import eu.neclab.ngsildbroker.commons.datatypes.Relationship;
import eu.neclab.ngsildbroker.commons.datatypes.RelationshipEntry;
import eu.neclab.ngsildbroker.commons.datatypes.TypedValue;

public class SerializationTools {
//	public static SimpleDateFormat formatter = new SimpleDateFormat(NGSIConstants.DEFAULT_DATE_FORMAT);
	public static DateTimeFormatter informatter = DateTimeFormatter
			.ofPattern(NGSIConstants.ALLOWED_IN_DEFAULT_DATE_FORMAT).withZone(ZoneId.of("Z"));// .withZone(ZoneId.systemDefault());
	public static DateTimeFormatter formatter = DateTimeFormatter
			.ofPattern(NGSIConstants.ALLOWED_OUT_DEFAULT_DATE_FORMAT).withZone(ZoneId.of("Z"));// systemDefault());
	public static DateTimeFormatter notifiedAt_formatter = DateTimeFormatter
			.ofPattern(NGSIConstants.ALLOWED_OUT_DEFAULT_DATE_FORMAT_NOTIFIEDAT).withZone(ZoneId.of("Z"));// systemDefault());
//	public static SimpleDateFormat forgivingFormatter = new SimpleDateFormat(
//			NGSIConstants.DEFAULT_FORGIVING_DATE_FORMAT);

	public static Gson geojsonGson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory())
			.create();

	@SuppressWarnings("unchecked")
	public static Property parseProperty(List<Map<String, Object>> topLevelArray, String key) {
		Property prop = new Property();
		prop.setId(key);
		prop.setType(NGSIConstants.NGSI_LD_PROPERTY);
		Iterator<Map<String, Object>> it = topLevelArray.iterator();
		HashMap<String, PropertyEntry> entries = new HashMap<String, PropertyEntry>();
		while (it.hasNext()) {
			Map<String, Object> next = it.next();
			ArrayList<Property> properties = new ArrayList<Property>();
			ArrayList<Relationship> relationships = new ArrayList<Relationship>();
			Long createdAt = null, observedAt = null, modifiedAt = null;
			Object propValue = null;
			String dataSetId = null;
			String unitCode = null;
			String name = null;
			for (Entry<String, Object> entry : next.entrySet()) {
				String propKey = entry.getKey();
				Object value = entry.getValue();
				if (propKey.equals(NGSIConstants.NGSI_LD_HAS_VALUE)) {
					propValue = getHasValue(value);

				} else if (propKey.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
					try {
						observedAt = date2Long(
								(String) ((List<Map<String, Object>>) value).get(0).get(NGSIConstants.JSON_LD_VALUE));
					} catch (Exception e) {
						throw new JsonParseException(e);
					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_CREATED_AT)) {
					try {
						createdAt = date2Long(
								(String) ((List<Map<String, Object>>) value).get(0).get(NGSIConstants.JSON_LD_VALUE));
					} catch (Exception e) {
						throw new JsonParseException(e);
					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
					try {
						modifiedAt = date2Long(
								(String) ((List<Map<String, Object>>) value).get(0).get(NGSIConstants.JSON_LD_VALUE));
					} catch (Exception e) {
						throw new JsonParseException(e);
					}
				} else if (propKey.equals(NGSIConstants.JSON_LD_TYPE)) {
					continue;
				} else if (propKey.equals(NGSIConstants.NGSI_LD_INSTANCE_ID)) {
					continue;
				} else if (propKey.equals(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
					dataSetId = getDataSetId((List<Map<String, Object>>) value);
				} else if (propKey.equals(NGSIConstants.NGSI_LD_NAME)) {
					name = getValue((List<Map<String, Object>>) value);
				} else if (propKey.equals(NGSIConstants.NGSI_LD_UNIT_CODE)) {
					unitCode = getValue((List<Map<String, Object>>) value);
				} else {
					List<Map<String, Object>> subLevelArray = (List<Map<String, Object>>) value;
					Map<String, Object> objValue = subLevelArray.get(0);
					if (objValue.containsKey(NGSIConstants.JSON_LD_TYPE)) {
						String valueType = ((List<String>) objValue.get(NGSIConstants.JSON_LD_TYPE)).get(0);
						if (valueType.equals(NGSIConstants.NGSI_LD_PROPERTY)) {
							properties.add(parseProperty(subLevelArray, propKey));
						} else if (valueType.equals(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
							relationships.add(parseRelationship(subLevelArray, propKey));
						}
					} else {
						throw new JsonParseException(
								"cannot determine type of sub attribute. please provide a valid type");
					}
				}

			}
			if (propValue == null) {
				throw new JsonParseException("Values cannot be null");
			}
			PropertyEntry propEntry = new PropertyEntry(dataSetId, propValue);
			propEntry.setProperties(properties);
			propEntry.setRelationships(relationships);
			propEntry.setCreatedAt(createdAt);
			propEntry.setObservedAt(observedAt);
			propEntry.setModifiedAt(modifiedAt);
			propEntry.setName(name);
			propEntry.setUnitCode(unitCode);
			entries.put(propEntry.getDataSetId(), propEntry);

		}
		prop.setEntries(entries);
		return prop;
	}

	private static String getValue(List<Map<String, Object>> value) {
		return (String) value.get(0).get(NGSIConstants.JSON_LD_VALUE);
	}

	@SuppressWarnings("unchecked")
	private static Object getHasValue(Object element) {
		if (element instanceof List) {
			List<Object> array = (List<Object>) element;
			ArrayList<Object> result = new ArrayList<Object>();
			array.forEach(new Consumer<Object>() {
				@Override
				public void accept(Object t) {
					result.add(getHasValue(t));

				}
			});
			return result;
		} else if (element instanceof Map) {
			Map<String, Object> jsonObj = (Map<String, Object>) element;
			if (jsonObj.containsKey(NGSIConstants.JSON_LD_VALUE) && jsonObj.containsKey(NGSIConstants.JSON_LD_TYPE)) {
				Object atValue = jsonObj.get(NGSIConstants.JSON_LD_VALUE);
				if (atValue == null) {
					throw new JsonParseException("Values cannot be null");
				}

				return new TypedValue((String) jsonObj.get(NGSIConstants.JSON_LD_TYPE), atValue);
			}
			if (jsonObj.containsKey(NGSIConstants.JSON_LD_VALUE)) {
				Object atValue = jsonObj.get(NGSIConstants.JSON_LD_VALUE);
				if (atValue == null) {
					throw new JsonParseException("Values cannot be null");
				}
				return atValue;
			} else {
				HashMap<String, List<Object>> result = new HashMap<String, List<Object>>();
				for (Entry<String, Object> entry : jsonObj.entrySet()) {
					result.put(entry.getKey(), (List<Object>) getHasValue(entry.getValue()));
				}
				return result;
			}

		} else {
			// should never be the case... but just in case store the element as string
			// representation
			ArrayList<String> result = new ArrayList<String>();
			result.add((String) element);
			return result;
		}

	}

	public static List<Object> getValueArray(Object value) {
		List<Object> result = Lists.newArrayList();
		Map<String, Object> temp = Maps.newHashMap();
		temp.put(NGSIConstants.JSON_LD_VALUE, value);
		result.add(temp);
		return result;
	}

	public static Long date2Long(String dateString) throws Exception {
		return Instant.from(informatter.parse(dateString)).toEpochMilli();

	}

	private static Long getTimestamp(List<Map<String, Object>> value) throws Exception {
		return date2Long((String) value.get(0).get(NGSIConstants.JSON_LD_VALUE));

	}

	@SuppressWarnings("unchecked")
	public static Relationship parseRelationship(List<Map<String, Object>> topLevelArray, String key) {
		Relationship relationship = new Relationship();
		relationship.setType(NGSIConstants.NGSI_LD_RELATIONSHIP);
		HashMap<String, RelationshipEntry> entries = new HashMap<String, RelationshipEntry>();
		relationship.setId(key);
		Iterator<Map<String, Object>> it = topLevelArray.iterator();
		while (it.hasNext()) {
			Map<String, Object> next = it.next();
			ArrayList<Property> properties = new ArrayList<Property>();
			ArrayList<Relationship> relationships = new ArrayList<Relationship>();
			Long createdAt = null, observedAt = null, modifiedAt = null;
			URI relObj = null;
			String dataSetId = null;
			String name = null;
			for (Entry<String, Object> entry : next.entrySet()) {
				String propKey = entry.getKey();
				Object value = entry.getValue();

				if (propKey.equals(NGSIConstants.NGSI_LD_HAS_OBJECT)) {
					if (((List<Object>) value).size() != 1) {
						throw new JsonParseException("Relationships have to have exactly one object");
					}
					try {
						relObj = new URI(
								(String) ((List<Map<String, Object>>) value).get(0).get(NGSIConstants.JSON_LD_ID));
					} catch (URISyntaxException e) {
						throw new JsonParseException("Relationships have to be a URI");
					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)) {

					try {
						observedAt = getTimestamp((List<Map<String, Object>>) value);
					} catch (Exception e) {
						throw new JsonParseException(e);
					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_CREATED_AT)) {
					try {
						createdAt = getTimestamp((List<Map<String, Object>>) value);
					} catch (Exception e) {
						throw new JsonParseException(e);
					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
					try {
						modifiedAt = getTimestamp((List<Map<String, Object>>) value);
					} catch (Exception e) {
						throw new JsonParseException(e);
					}

				} else if (propKey.equals(NGSIConstants.JSON_LD_TYPE)) {
					continue;
				} else if (propKey.equals(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
					dataSetId = getDataSetId((List<Map<String, Object>>) value);
				} else if (propKey.equals(NGSIConstants.NGSI_LD_NAME)) {
					name = getValue((List<Map<String, Object>>) value);
				} else {
					List<Map<String, Object>> subLevelArray = (List<Map<String, Object>>) value;
					Map<String, Object> objValue = subLevelArray.get(0);
					if (objValue.containsKey(NGSIConstants.JSON_LD_TYPE)) {
						String valueType = ((List<String>) objValue.get(NGSIConstants.JSON_LD_TYPE)).get(0);
						if (valueType.equals(NGSIConstants.NGSI_LD_PROPERTY)) {
							properties.add(parseProperty(subLevelArray, propKey));
						} else if (valueType.equals(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
							relationships.add(parseRelationship(subLevelArray, propKey));
						}
					} else {
						throw new JsonParseException(
								"cannot determine type of sub attribute. please provide a valid type");
					}
				}

			}
			if (relObj == null) {
				throw new JsonParseException("Relationships have to have exactly one object");
			}
			RelationshipEntry object = new RelationshipEntry(dataSetId, relObj);
			object.setProperties(properties);
			object.setRelationships(relationships);
			object.setCreatedAt(createdAt);
			object.setObservedAt(observedAt);
			object.setModifiedAt(modifiedAt);
			object.setName(name);
			entries.put(object.getDataSetId(), object);
		}
		relationship.setObjects(entries);
		return relationship;
	}

	private static String getDataSetId(List<Map<String, Object>> value) {
		return (String) value.get(0).get(NGSIConstants.JSON_LD_ID);
	}

	/**
	 * 
	 * @param timestamp
	 * @param context
	 * @param type      - to indicate which type of serialization is required For ex
	 *                  (in Entity payload).
	 * 
	 *                  createdAt must be serialized as :
	 *                  "https://uri.etsi.org/ngsi-ld/createdAt": [{ "@type":
	 *                  ["https://uri.etsi.org/ngsi-ld/Property"],
	 *                  "https://uri.etsi.org/ngsi-ld/hasValue": [{ "@value":
	 *                  "2017-07-29T12:00:04" }] }]
	 *
	 *                  whereas observedAt must be serialized as : "http:
	 *                  //uri.etsi.org/ngsi-ld/observedAt": [{
	 * @value: "2017-07-29T12:00:04",
	 * @type: "https://uri.etsi.org/ngsi-ld/DateTime" }]
	 *
	 *        although both are same(Long/Timestamp) but they need to serialize
	 *        differently. serializaton mst be of type :
	 *
	 * @return JsonElement
	 */
	// TODO : How type will be decided from Entity class variables.

	public static JsonElement getJson(Property property, JsonSerializationContext context) {
		JsonArray result = new JsonArray();
		HashMap<String, PropertyEntry> entries = property.getEntries();
		for (PropertyEntry entry : entries.values()) {
			JsonObject top = new JsonObject();
			JsonArray type = new JsonArray();
			type.add(new JsonPrimitive(entry.getType()));
			top.add(NGSIConstants.JSON_LD_TYPE, type);

			top.add(NGSIConstants.NGSI_LD_HAS_VALUE, getJson(entry.getValue(), context));
			if (entry.getObservedAt() > 0) {
				top.add(NGSIConstants.NGSI_LD_OBSERVED_AT, getJson(entry.getObservedAt(), context));
			}
			if (entry.getCreatedAt() > 0) {
				top.add(NGSIConstants.NGSI_LD_CREATED_AT, getJson(entry.getCreatedAt(), context));
			}
			if (entry.getModifiedAt() > 0) {
				top.add(NGSIConstants.NGSI_LD_MODIFIED_AT, getJson(entry.getModifiedAt(), context));
			}
			if (entry.getUnitCode() != null && !entry.getUnitCode().isEmpty()) {
				top.add(NGSIConstants.NGSI_LD_UNIT_CODE, getJson(entry.getUnitCode(), context));
			}
			for (Property propOfProp : entry.getProperties()) {
				top.add(propOfProp.getId().toString(), getJson(propOfProp, context));
			}
			for (Relationship relaOfProp : entry.getRelationships()) {
				top.add(relaOfProp.getId().toString(), getJson(relaOfProp, context));
			}
			result.add(top);
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	private static JsonElement getJson(Object value, JsonSerializationContext context) {
		JsonElement result;
		if (value instanceof Map) {
			return getComplexValue((Map<String, List<Object>>) value, context);
		} else if (value instanceof List) {
			List<Object> myList = (List<Object>) value;
			JsonArray myArray = new JsonArray();
			for (Object object : myList) {
				myArray.add(getJson(object, context));
			}
			result = myArray;
		} else {

			if (value instanceof TypedValue) {
				return context.serialize(value);
			}
			result = new JsonObject();
			((JsonObject) result).add(NGSIConstants.JSON_LD_VALUE, context.serialize(value));

		}

		return result;
	}

	private static JsonObject getComplexValue(Map<String, List<Object>> value, JsonSerializationContext context) {
		JsonObject top = new JsonObject();
		for (Entry<String, List<Object>> entry : value.entrySet()) {
			top.add(entry.getKey(), getJson(entry.getValue(), context));
		}
		return top;
	}

	public static JsonElement getJson(Relationship relationship, JsonSerializationContext context) {
		JsonArray result = new JsonArray();

		for (RelationshipEntry entry : relationship.getEntries().values()) {
			JsonObject top = new JsonObject();
			JsonArray type = new JsonArray();
			type.add(new JsonPrimitive(entry.getType()));
			top.add(NGSIConstants.JSON_LD_TYPE, type);
			JsonArray value = new JsonArray();
			JsonObject objValue = new JsonObject();
			objValue.add(NGSIConstants.JSON_LD_ID, context.serialize(entry.getObject()));
			value.add(objValue);
			top.add(NGSIConstants.NGSI_LD_HAS_OBJECT, value);
			JsonArray datasetIdvalue = new JsonArray();
			JsonObject datasetobjValue = new JsonObject();
			datasetobjValue.add(NGSIConstants.JSON_LD_ID, context.serialize(entry.getDataSetId()));
			datasetIdvalue.add(datasetobjValue);
			top.add(NGSIConstants.NGSI_LD_DATA_SET_ID, datasetIdvalue);
			if (entry.getObservedAt() > 0) {
				top.add(NGSIConstants.NGSI_LD_OBSERVED_AT, getJson(entry.getObservedAt(), context));
			}
			if (entry.getCreatedAt() > 0) {
				top.add(NGSIConstants.NGSI_LD_CREATED_AT, getJson(entry.getCreatedAt(), context));
			}
			if (entry.getModifiedAt() > 0) {
				top.add(NGSIConstants.NGSI_LD_MODIFIED_AT, getJson(entry.getModifiedAt(), context));
			}
			for (Property propOfProp : entry.getProperties()) {
				top.add(propOfProp.getId().toString(), getJson(propOfProp, context));
			}
			for (Relationship relaOfProp : entry.getRelationships()) {
				top.add(relaOfProp.getId().toString(), getJson(relaOfProp, context));
			}
			result.add(top);

		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public static GeoProperty parseGeoProperty(List<Map<String, Object>> topLevelArray, String key) {
		GeoProperty prop = new GeoProperty();
		prop.setId(key);
		prop.setType(NGSIConstants.NGSI_LD_GEOPROPERTY);
		Iterator<Map<String, Object>> it = topLevelArray.iterator();
		HashMap<String, GeoPropertyEntry> entries = new HashMap<String, GeoPropertyEntry>();
		while (it.hasNext()) {
			Map<String, Object> next = it.next();
			ArrayList<Property> properties = new ArrayList<Property>();
			ArrayList<Relationship> relationships = new ArrayList<Relationship>();
			Long createdAt = null, observedAt = null, modifiedAt = null;

			Geometry<?> geoValue = null;
			String dataSetId = null;
			String name = null;
			boolean realGeoProperty = ((List<String>) next.get(NGSIConstants.JSON_LD_TYPE)).get(0)
					.equals(NGSIConstants.NGSI_LD_GEOPROPERTY);
			if (realGeoProperty) {
				for (Entry<String, Object> entry : next.entrySet()) {
					String propKey = entry.getKey();
					Object value = entry.getValue();
					if (propKey.equals(NGSIConstants.NGSI_LD_HAS_VALUE)) {
						Map<String, Object> propValue = ((List<Map<String, Object>>) value).get(0);
						geoValue = getGeoValue(propValue);
					} else if (propKey.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
						try {
							observedAt = date2Long((String) ((List<Map<String, Object>>) value).get(0)
									.get(NGSIConstants.JSON_LD_VALUE));
						} catch (Exception e) {
							throw new JsonParseException(e);
						}
					} else if (propKey.equals(NGSIConstants.NGSI_LD_CREATED_AT)) {
						try {
							createdAt = date2Long((String) ((List<Map<String, Object>>) value).get(0)
									.get(NGSIConstants.JSON_LD_VALUE));
						} catch (Exception e) {
							throw new JsonParseException(e);
						}
					} else if (propKey.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
						try {
							modifiedAt = date2Long((String) ((List<Map<String, Object>>) value).get(0)
									.get(NGSIConstants.JSON_LD_VALUE));
						} catch (Exception e) {
							throw new JsonParseException(e);
						}

					} else if (propKey.equals(NGSIConstants.JSON_LD_TYPE)) {
						continue;
					} else if (propKey.equals(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						dataSetId = getDataSetId((List<Map<String, Object>>) value);
					} else if (propKey.equals(NGSIConstants.NGSI_LD_NAME)) {
						name = getValue((List<Map<String, Object>>) value);
					} else {
						List<Map<String, Object>> subLevelArray = (List<Map<String, Object>>) value;
						Map<String, Object> objValue = subLevelArray.get(0);
						if (objValue.containsKey(NGSIConstants.JSON_LD_TYPE)) {
							String valueType = ((List<String>) objValue.get(NGSIConstants.JSON_LD_TYPE)).get(0);
							if (valueType.equals(NGSIConstants.NGSI_LD_PROPERTY)) {
								properties.add(parseProperty(subLevelArray, propKey));
							} else if (valueType.equals(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
								relationships.add(parseRelationship(subLevelArray, propKey));
							}
						} else {
							throw new JsonParseException(
									"cannot determine type of sub attribute. please provide a valid type");
						}
					}

				}
			} else {
				geoValue = getGeoValue(next);
			}
			GeoPropertyEntry geoPropEntry = new GeoPropertyEntry(dataSetId, geoValue);
			geoPropEntry.setProperties(properties);
			geoPropEntry.setRelationships(relationships);
			geoPropEntry.setCreatedAt(createdAt);
			geoPropEntry.setObservedAt(observedAt);
			geoPropEntry.setModifiedAt(modifiedAt);
			geoPropEntry.setName(name);
			entries.put(geoPropEntry.getDataSetId(), geoPropEntry);
		}
		prop.setEntries(entries);
		return prop;
	}

	@SuppressWarnings("unchecked")
	private static Geometry<?> getGeoValue(Map<String, Object> propValue) {
		String geometry = ((List<String>) propValue.get(NGSIConstants.JSON_LD_TYPE)).get(0);
		List<Map<String, Object>> coordinates = (List<Map<String, Object>>) propValue
				.get(NGSIConstants.NGSI_LD_COORDINATES);
		switch (geometry) {
			case NGSIConstants.NGSI_LD_POINT:
				return new Point(getSingeLePosition(coordinates));
			case NGSIConstants.NGSI_LD_POLYOGN:
				return new Polygon(getAreaPositions(coordinates));
			case NGSIConstants.NGSI_LD_LINESTRING:
				return new LineString(getLinearPositions(coordinates));
			default:
				return null;
		}

	}

	@SuppressWarnings("unchecked")
	private static LinearPositions getLinearPositions(List<Map<String, Object>> coordinates) {
		ArrayList<SinglePosition> coordinateList = new ArrayList<SinglePosition>();
		List<Map<String, Object>> list = (List<Map<String, Object>>) coordinates.get(0).get(NGSIConstants.JSON_LD_LIST);
		for (Map<String, Object> entry : list) {
			coordinateList.add(getSingeLePosition((List<Map<String, Object>>) entry.get(NGSIConstants.JSON_LD_LIST)));
		}
		return new LinearPositions(ImmutableList.copyOf(coordinateList));
	}

	@SuppressWarnings("unchecked")
	private static AreaPositions getAreaPositions(List<Map<String, Object>> coordinates) {
		ArrayList<LinearPositions> coordinateList = new ArrayList<LinearPositions>();
		List<Map<String, Object>> list = (List<Map<String, Object>>) coordinates.get(0).get(NGSIConstants.JSON_LD_LIST);
		for (Map<String, Object> entry : list) {
			coordinateList.add(getLinearPositions((List<Map<String, Object>>) entry.get(NGSIConstants.JSON_LD_LIST)));
		}
		return new AreaPositions(coordinateList);
	}

	@SuppressWarnings("unchecked")
	private static SinglePosition getSingeLePosition(List<Map<String, Object>> coordinates) {
		List<Map<String, Object>> list = (List<Map<String, Object>>) coordinates.get(0).get(NGSIConstants.JSON_LD_LIST);
		return new SinglePosition(Coordinates.of(
				getProperLon(Double.parseDouble(list.get(0).get(NGSIConstants.JSON_LD_VALUE).toString())),
				getProperLat(Double.parseDouble(list.get(1).get(NGSIConstants.JSON_LD_VALUE).toString()))));
	}

	public static double getProperLat(double lat) {
		while (lat > 90) {
			lat = lat - 180;
		}
		while (lat < -90) {
			lat = lat + 180;
		}
		return lat;
	}

	public static double getProperLon(double lon) {
		while (lon > 180) {
			lon = lon - 360;
		}
		while (lon < -180) {
			lon = lon + 360;
		}
		return lon;
	}

}
