package eu.neclab.ngsildbroker.commons.tools;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.filosganga.geogson.gson.GeometryAdapterFactory;
import com.github.filosganga.geogson.model.Geometry;
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
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;

public class SerializationTools {
//	public static SimpleDateFormat formatter = new SimpleDateFormat(NGSIConstants.DEFAULT_DATE_FORMAT);
	private static DateTimeFormatter informatter = DateTimeFormatter
			.ofPattern(NGSIConstants.ALLOWED_IN_DEFAULT_DATE_FORMAT).withZone(ZoneId.of("Z"));//.withZone(ZoneId.systemDefault());
	public static DateTimeFormatter formatter = DateTimeFormatter
			.ofPattern(NGSIConstants.ALLOWED_OUT_DEFAULT_DATE_FORMAT).withZone(ZoneId.of("Z"));//systemDefault());
//	public static SimpleDateFormat forgivingFormatter = new SimpleDateFormat(
//			NGSIConstants.DEFAULT_FORGIVING_DATE_FORMAT);

	public static Gson geojsonGson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory())
			.create();

	public static GeoProperty parseGeoProperty(JsonArray topLevelArray, String key) {
		GeoProperty prop = new GeoProperty();
		try {
			prop.setId(new URI(key));
		} catch (URISyntaxException e) {
			throw new JsonParseException(e);
		}
		prop.setType(NGSIConstants.NGSI_LD_GEOPROPERTY);
		Iterator<JsonElement> it = topLevelArray.iterator();
		HashMap<String, GeoPropertyEntry> entries = new HashMap<String, GeoPropertyEntry>();
		while (it.hasNext()) {
			JsonObject next = (JsonObject) it.next();
			ArrayList<Property> properties = new ArrayList<Property>();
			ArrayList<Relationship> relationships = new ArrayList<Relationship>();
			Long createdAt = null, observedAt = null, modifiedAt = null;
			String geoValueStr = null;
			Geometry<?> geoValue = null;
			String dataSetId = null;
			String unitCode = null;
			String name = null;
			for (Entry<String, JsonElement> entry : next.entrySet()) {
				String propKey = entry.getKey();
				JsonElement value = entry.getValue();
				if (propKey.equals(NGSIConstants.NGSI_LD_HAS_VALUE)) {
					JsonElement propValue = value.getAsJsonArray().get(0);
					if (propValue.isJsonPrimitive()) {
						JsonPrimitive primitive = propValue.getAsJsonPrimitive();
						if (primitive.isString()) {
							geoValueStr = primitive.getAsString();
							geoValue = DataSerializer.getGeojsonGeometry(primitive.getAsString());
						}
					} else {
						JsonElement atValue = propValue.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE);
						if (atValue != null) {
							if (atValue.isJsonPrimitive()) {
								JsonPrimitive primitive = atValue.getAsJsonPrimitive();
								if (primitive.isString()) {
									geoValueStr = primitive.getAsString();
									geoValue = DataSerializer.getGeojsonGeometry(primitive.getAsString());
								}

							} else {
								geoValueStr = atValue.getAsString();
								geoValue = DataSerializer.getGeojsonGeometry(atValue.getAsString());
							}
						} else {
							geoValueStr = propValue.toString();
							geoValue = DataSerializer.getGeojsonGeometry(propValue.toString());
						}

					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
					try {
						observedAt = date2Long(value.getAsJsonArray().get(0).getAsJsonObject()
								.get(NGSIConstants.JSON_LD_VALUE).getAsString());
					} catch (Exception e) {
						throw new JsonParseException(e);
					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_CREATED_AT)) {
					try {
						createdAt = date2Long(value.getAsJsonArray().get(0).getAsJsonObject()
								.get(NGSIConstants.JSON_LD_VALUE).getAsString());
					} catch (Exception e) {
						throw new JsonParseException(e);
					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
					try {
						modifiedAt = date2Long(value.getAsJsonArray().get(0).getAsJsonObject()
								.get(NGSIConstants.JSON_LD_VALUE).getAsString());
					} catch (Exception e) {
						throw new JsonParseException(e);
					}

				} else if (propKey.equals(NGSIConstants.JSON_LD_TYPE)) {
					continue;
				} else if (propKey.equals(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
					dataSetId = getDataSetId(value);
				} else if (propKey.equals(NGSIConstants.NGSI_LD_NAME)) {
					name = getName(value);
				} else {
					JsonArray subLevelArray = value.getAsJsonArray();
					JsonObject objValue = subLevelArray.get(0).getAsJsonObject();
					if (objValue.has(NGSIConstants.JSON_LD_TYPE)) {
						String valueType = objValue.get(NGSIConstants.JSON_LD_TYPE).getAsJsonArray().get(0)
								.getAsString();
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
			GeoPropertyEntry geoPropEntry = new GeoPropertyEntry(dataSetId, geoValueStr, geoValue);
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
	public static Property parseProperty(JsonArray topLevelArray, String key) {
		Property prop = new Property();
		try {
			prop.setId(new URI(key));
		} catch (URISyntaxException e) {
			throw new JsonParseException(e);
		}
		prop.setType(NGSIConstants.NGSI_LD_PROPERTY);
		Iterator<JsonElement> it = topLevelArray.iterator();
		HashMap<String, PropertyEntry> entries = new HashMap<String, PropertyEntry>();
		while (it.hasNext()) {
			JsonObject next = (JsonObject) it.next();
			ArrayList<Property> properties = new ArrayList<Property>();
			ArrayList<Relationship> relationships = new ArrayList<Relationship>();
			Long createdAt = null, observedAt = null, modifiedAt = null;
			Object propValue = null;
			String dataSetId = null;
			String unitCode = null;
			String name = null;
			for (Entry<String, JsonElement> entry : next.entrySet()) {
				String propKey = entry.getKey();
				JsonElement value = entry.getValue();
				if (propKey.equals(NGSIConstants.NGSI_LD_HAS_VALUE)) {
					propValue = getHasValue(value);

				} else if (propKey.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
					try {
						observedAt = date2Long(value.getAsJsonArray().get(0).getAsJsonObject()
								.get(NGSIConstants.JSON_LD_VALUE).getAsString());
					} catch (Exception e) {
						throw new JsonParseException(e);
					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_CREATED_AT)) {
					try {
						createdAt = date2Long(value.getAsJsonArray().get(0).getAsJsonObject()
								.get(NGSIConstants.JSON_LD_VALUE).getAsString());
					} catch (Exception e) {
						throw new JsonParseException(e);
					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
					try {
						modifiedAt = date2Long(value.getAsJsonArray().get(0).getAsJsonObject()
								.get(NGSIConstants.JSON_LD_VALUE).getAsString());
					} catch (Exception e) {
						throw new JsonParseException(e);
					}
				} else if (propKey.equals(NGSIConstants.JSON_LD_TYPE)) {
					continue;
				} else if (propKey.equals(NGSIConstants.NGSI_LD_INSTANCE_ID)) {
					continue;
				} else if (propKey.equals(NGSIConstants.NGSI_LD_UNIT_CODE)) {
					unitCode = getUnitCode(value);
				} else if (propKey.equals(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
					dataSetId = getDataSetId(value);
				} else if (propKey.equals(NGSIConstants.NGSI_LD_NAME)) {
					name = getName(value);
				} else {
					JsonArray subLevelArray = value.getAsJsonArray();
					JsonObject objValue = subLevelArray.get(0).getAsJsonObject();
					if (objValue.has(NGSIConstants.JSON_LD_TYPE)) {
						String valueType = objValue.get(NGSIConstants.JSON_LD_TYPE).getAsJsonArray().get(0)
								.getAsString();
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

	private static String getName(JsonElement value) {
		// TODO Auto-generated method stub
		return null;
	}

	private static String getUnitCode(JsonElement value) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	private static Object getHasValue(JsonElement element) {
		if (element.isJsonArray()) {
			JsonArray array = element.getAsJsonArray();
			ArrayList<Object> result = new ArrayList<Object>();
			array.forEach(new Consumer<JsonElement>() {
				@Override
				public void accept(JsonElement t) {
					result.add(getHasValue(t));

				}
			});
			return result;
		} else if (element.isJsonObject()) {
			JsonObject jsonObj = element.getAsJsonObject();
			if (jsonObj.has(NGSIConstants.JSON_LD_VALUE) && jsonObj.has(NGSIConstants.JSON_LD_TYPE)) {
				Object objValue;
				JsonPrimitive atValue = jsonObj.get(NGSIConstants.JSON_LD_VALUE).getAsJsonPrimitive();
				if (atValue.isJsonNull()) {
					throw new JsonParseException("Values cannot be null");
				}
				if (atValue.isBoolean()) {
					objValue = atValue.getAsBoolean();
				} else if (atValue.isNumber()) {
					objValue = atValue.getAsDouble();
				} else if (atValue.isString()) {
					objValue = atValue.getAsString();
				} else {
					objValue = jsonObj.get(NGSIConstants.JSON_LD_VALUE).getAsString();
				}

				return new TypedValue(jsonObj.get(NGSIConstants.JSON_LD_TYPE).getAsString(), objValue);
			}
			if (jsonObj.has(NGSIConstants.JSON_LD_VALUE)) {
				JsonPrimitive atValue = jsonObj.get(NGSIConstants.JSON_LD_VALUE).getAsJsonPrimitive();
				if (atValue.isJsonNull()) {
					throw new JsonParseException("Values cannot be null");
				}
				if (atValue.isBoolean()) {
					return atValue.getAsBoolean();
				}
				if (atValue.isNumber()) {
					return atValue.getAsDouble();
				}
				if (atValue.isString()) {
					return atValue.getAsString();
				}

				return jsonObj.get(NGSIConstants.JSON_LD_VALUE).getAsString();
			} else {
				HashMap<String, List<Object>> result = new HashMap<String, List<Object>>();
				for (Entry<String, JsonElement> entry : jsonObj.entrySet()) {
					result.put(entry.getKey(), (List<Object>) getHasValue(entry.getValue()));
				}
				return result;
			}

		} else {
			// should never be the case... but just in case store the element as string
			// representation
			ArrayList<String> result = new ArrayList<String>();
			result.add(element.getAsString());
			return result;
		}

	}

	public static JsonArray getValueArray(Integer value) {
		return getValueArray(new JsonPrimitive(value));
	}

	public static JsonArray getValueArray(String value) {
		return getValueArray(new JsonPrimitive(value));
	}

	public static JsonArray getValueArray(Long value) {
		return getValueArray(new JsonPrimitive(value));
	}

	public static JsonArray getValueArray(Double value) {
		return getValueArray(new JsonPrimitive(value));
	}

	public static JsonArray getValueArray(Float value) {
		return getValueArray(new JsonPrimitive(value));
	}

	public static JsonArray getValueArray(Boolean value) {
		return getValueArray(new JsonPrimitive(value));
	}

	private static JsonArray getValueArray(JsonElement value) {
		JsonArray result = new JsonArray();
		JsonObject temp = new JsonObject();
		temp.add(NGSIConstants.JSON_LD_VALUE, value);
		result.add(temp);
		return result;
	}

	public static Long date2Long(String dateString) throws Exception {
		return Instant.from(informatter.parse(dateString)).toEpochMilli();

	}

	private static Long getTimestamp(JsonElement value) throws Exception {
		return date2Long(
				value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());

	}

	public static Relationship parseRelationship(JsonArray topLevelArray, String key) {
		Relationship relationship = new Relationship();
		relationship.setType(NGSIConstants.NGSI_LD_RELATIONSHIP);
		HashMap<String, RelationshipEntry> entries = new HashMap<String, RelationshipEntry>();
		try {
			relationship.setId(new URI(key));
		} catch (URISyntaxException e) {
			throw new JsonParseException("The Id has to be a URI");
		}

		Iterator<JsonElement> it = topLevelArray.iterator();
		while (it.hasNext()) {
			JsonObject next = (JsonObject) it.next();
			ArrayList<Property> properties = new ArrayList<Property>();
			ArrayList<Relationship> relationships = new ArrayList<Relationship>();
			Long createdAt = null, observedAt = null, modifiedAt = null;
			URI relObj = null;
			String dataSetId = null;
			String name = null;
			for (Entry<String, JsonElement> entry : next.entrySet()) {
				String propKey = entry.getKey();
				JsonElement value = entry.getValue();

				if (propKey.equals(NGSIConstants.NGSI_LD_HAS_OBJECT)) {
					if (value.getAsJsonArray().size() != 1) {
						throw new JsonParseException("Relationships have to have exactly one object");
					}
					try {
						relObj = new URI(value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_ID)
								.getAsString());
					} catch (URISyntaxException e) {
						throw new JsonParseException("Relationships have to be a URI");
					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)) {

					try {
						observedAt = getTimestamp(value);
					} catch (Exception e) {
						throw new JsonParseException(e);
					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_CREATED_AT)) {
					try {
						createdAt = getTimestamp(value);
					} catch (Exception e) {
						throw new JsonParseException(e);
					}
				} else if (propKey.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
					try {
						modifiedAt = getTimestamp(value);
					} catch (Exception e) {
						throw new JsonParseException(e);
					}

				} else if (propKey.equals(NGSIConstants.JSON_LD_TYPE)) {
					continue;
				} else if (propKey.equals(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
					dataSetId = getDataSetId(value);
				} else if (propKey.equals(NGSIConstants.NGSI_LD_NAME)) {
					name = getName(value);
				} else {
					JsonArray subLevelArray = value.getAsJsonArray();
					JsonObject objValue = subLevelArray.get(0).getAsJsonObject();
					if (objValue.has(NGSIConstants.JSON_LD_TYPE)) {
						String valueType = objValue.get(NGSIConstants.JSON_LD_TYPE).getAsJsonArray().get(0)
								.getAsString();
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

	private static String getDataSetId(JsonElement value) {
		// TODO Auto-generated method stub
		return null;
	}

	public static JsonElement getJson(Long timestamp, JsonSerializationContext context) {
		JsonArray observedArray = new JsonArray();
		JsonObject observedObj = new JsonObject();
		observedObj.add(NGSIConstants.JSON_LD_VALUE,
				context.serialize(formatter.format(Instant.ofEpochMilli(timestamp))));
		observedObj.add(NGSIConstants.JSON_LD_TYPE, context.serialize(NGSIConstants.NGSI_LD_DATE_TIME));
		observedArray.add(observedObj);
		return observedArray;
	}

	public static JsonElement getJson(Geometry<?> geojsonGeometry) {
		return new JsonPrimitive(geojsonGson.toJson(geojsonGeometry));
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

	public static JsonElement getJson(GeoProperty property, JsonSerializationContext context) {
		JsonArray result = new JsonArray();
		for (GeoPropertyEntry entry : property.getEntries().values()) {
			JsonObject top = new JsonObject();
			JsonArray type = new JsonArray();
			type.add(new JsonPrimitive(entry.getType()));
			top.add(NGSIConstants.JSON_LD_TYPE, type);
			JsonArray value = new JsonArray();
			JsonObject objValue = new JsonObject();
			objValue.add(NGSIConstants.JSON_LD_VALUE, context.serialize(entry.getValue()));
			value.add(objValue);
			top.add(NGSIConstants.NGSI_LD_HAS_VALUE, value);
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

	/*
	 * public static JsonElement getJsonForCSource(GeoProperty geoProperty,
	 * JsonSerializationContext context) { Gson gson = new Gson(); return
	 * gson.fromJson(geoProperty.getValue(), JsonElement.class); }
	 */
	public static JsonNode parseJson(ObjectMapper objectMapper, String payload) throws ResponseException {
		JsonNode json = null;
		try {
			json = objectMapper.readTree(payload);
			if (json.isNull()) {
				throw new ResponseException(ErrorType.InvalidRequest);
			}
		} catch (JsonParseException e) {
			throw new ResponseException(ErrorType.InvalidRequest);
		} catch (IOException e) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		return json;
	}

}
