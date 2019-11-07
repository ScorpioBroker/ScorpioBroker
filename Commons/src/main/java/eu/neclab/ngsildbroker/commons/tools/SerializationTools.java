package eu.neclab.ngsildbroker.commons.tools;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.datatypes.Relationship;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;

public class SerializationTools {
	public static SimpleDateFormat formatter = new SimpleDateFormat(NGSIConstants.DEFAULT_DATE_FORMAT);

	public static SimpleDateFormat forgivingFormatter = new SimpleDateFormat(
			NGSIConstants.DEFAULT_FORGIVING_DATE_FORMAT);

	public static Gson geojsonGson = new GsonBuilder().registerTypeAdapterFactory(new GeometryAdapterFactory())
			.create();

	public static GeoProperty parseGeoProperty(JsonObject jsonProp, String key) {
		GeoProperty prop = new GeoProperty();
		prop.setName(key);
		ArrayList<Property> properties = new ArrayList<Property>();
		ArrayList<Relationship> relationships = new ArrayList<Relationship>();
		prop.setProperties(properties);
		prop.setRelationships(relationships);
		try {
			prop.setId(new URI(key));
		} catch (URISyntaxException e) {
			throw new JsonParseException(e);
		}
		prop.setType(NGSIConstants.NGSI_LD_GEOPROPERTY);
		for (Entry<String, JsonElement> entry : jsonProp.entrySet()) {
			String propKey = entry.getKey();
			JsonElement value = entry.getValue();
			if (propKey.equals(NGSIConstants.NGSI_LD_HAS_VALUE)) {
				JsonElement propValue = value.getAsJsonArray().get(0);
				if (propValue.isJsonPrimitive()) {
					JsonPrimitive primitive = propValue.getAsJsonPrimitive();
					if (primitive.isString()) {
						prop.setValue(primitive.getAsString());
						prop.setGeoValue(DataSerializer.getGeojsonGeometry(primitive.getAsString()));
					}
				} else {
					JsonElement atValue = propValue.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE);
					if (atValue != null) {
						if (atValue.isJsonPrimitive()) {
							JsonPrimitive primitive = atValue.getAsJsonPrimitive();
							if (primitive.isString()) {
								prop.setValue(primitive.getAsString());
								prop.setGeoValue(DataSerializer.getGeojsonGeometry(primitive.getAsString()));
							}

						} else {
							prop.setValue(atValue.getAsString());
							prop.setGeoValue(DataSerializer.getGeojsonGeometry(atValue.getAsString()));
						}
					} else {
						prop.setValue(propValue.toString());
						prop.setGeoValue(DataSerializer.getGeojsonGeometry(propValue.getAsString()));
					}

				}
			} else if (propKey.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
				Long timestamp = null;
				try {
					timestamp = date2Long(
							value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
				if (timestamp != null) {
					prop.setObservedAt(timestamp);
				}

			} else if (propKey.equals(NGSIConstants.NGSI_LD_CREATED_AT)) {
				Long timestamp = null;
				try {
					timestamp = date2Long(
							value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
				if (timestamp != null) {
					prop.setCreatedAt(timestamp);
				}

			} else if (propKey.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				Long timestamp = null;
				try {
					timestamp = date2Long(
							value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
				if (timestamp != null) {
					prop.setModifiedAt(timestamp);
				}

			} else if (propKey.equals(NGSIConstants.JSON_LD_TYPE)) {
				continue;
			} else {
				JsonObject objValue = value.getAsJsonArray().get(0).getAsJsonObject();
				if (objValue.has(NGSIConstants.JSON_LD_TYPE)) {
					String valueType = objValue.get(NGSIConstants.JSON_LD_TYPE).getAsJsonArray().get(0).getAsString();
					if (valueType.equals(NGSIConstants.NGSI_LD_PROPERTY)) {
						properties.add(parseProperty(objValue, propKey));
					} else if (valueType.equals(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
						relationships.add(parseRelationship(objValue, propKey));
					}
				}
			}

		}
		return prop;
	}

	@SuppressWarnings("unchecked")
	public static Property parseProperty(JsonObject jsonProp, String key) {
		Property prop = new Property();
		prop.setName(key);
		ArrayList<Property> properties = new ArrayList<Property>();
		ArrayList<Relationship> relationships = new ArrayList<Relationship>();
		prop.setProperties(properties);
		prop.setRelationships(relationships);
		try {
			prop.setId(new URI(key));
		} catch (URISyntaxException e) {
			throw new JsonParseException(e);
		}
		prop.setType(NGSIConstants.NGSI_LD_PROPERTY);
		for (Entry<String, JsonElement> entry : jsonProp.entrySet()) {
			String propKey = entry.getKey();
			JsonElement value = entry.getValue();
			if (propKey.equals(NGSIConstants.NGSI_LD_HAS_VALUE)) {
				// You can ignore the warning here since the first layer is always a list in
				// ngsi-ld/json-ld
				prop.setValue((List<Object>) getHasValue(value));

			} else if (propKey.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
				Long timestamp = null;
				try {
					timestamp = date2Long(
							value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
				if (timestamp != null) {
					prop.setObservedAt(timestamp);
				}

			} else if (propKey.equals(NGSIConstants.NGSI_LD_CREATED_AT)) {
				Long timestamp = null;
				try {
					timestamp = date2Long(
							value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
				if (timestamp != null) {
					prop.setCreatedAt(timestamp);
				}

			} else if (propKey.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				Long timestamp = null;
				try {
					timestamp = date2Long(
							value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
				if (timestamp != null) {
					prop.setModifiedAt(timestamp);
				}

			} else if (propKey.equals(NGSIConstants.JSON_LD_TYPE)) {
				continue;
			} else {
				JsonObject objValue = value.getAsJsonArray().get(0).getAsJsonObject();
				if (objValue.has(NGSIConstants.JSON_LD_TYPE)) {
					String valueType = objValue.get(NGSIConstants.JSON_LD_TYPE).getAsJsonArray().get(0).getAsString();
					if (valueType.equals(NGSIConstants.NGSI_LD_PROPERTY)) {
						properties.add(parseProperty(objValue, propKey));
					} else if (valueType.equals(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
						relationships.add(parseRelationship(objValue, propKey));
					}
				}
			}

		}
		return prop;
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
			if (jsonObj.has(NGSIConstants.JSON_LD_VALUE)) {
				JsonPrimitive atValue = jsonObj.get(NGSIConstants.JSON_LD_VALUE).getAsJsonPrimitive();
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

		Date date;

		date = (Date) formatter.parse(dateString);
		return date.getTime();

	}

	private static Long getTimestamp(JsonElement value) throws Exception {
		return date2Long(
				value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());

	}

	public static Relationship parseRelationship(JsonObject jsonRelationship, String key) {
		Relationship relationship = new Relationship();
		relationship.setName(key);
		ArrayList<Property> properties = new ArrayList<Property>();
		ArrayList<Relationship> relationships = new ArrayList<Relationship>();
		relationship.setProperties(properties);
		relationship.setRelationships(relationships);
		try {
			relationship.setId(new URI(key));
		} catch (URISyntaxException e) {
			throw new JsonParseException(e);
		}
		relationship.setType(NGSIConstants.NGSI_LD_RELATIONSHIP);
		for (Entry<String, JsonElement> entry : jsonRelationship.entrySet()) {
			String propKey = entry.getKey();
			JsonElement value = entry.getValue();
			if (propKey.equals(NGSIConstants.NGSI_LD_OBJECT)) {
				try {
					relationship.setObject(new URI(value.getAsJsonArray().get(0).getAsJsonObject()
							.get(NGSIConstants.JSON_LD_ID).getAsString()));
				} catch (URISyntaxException e) {
					throw new JsonParseException(e);
				}
			} else if (propKey.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
				Long timestamp;
				try {
					timestamp = getTimestamp(value);
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
				if (timestamp != null) {
					relationship.setObservedAt(timestamp);
				}

			} else if (propKey.equals(NGSIConstants.NGSI_LD_CREATED_AT)) {
				Long timestamp;
				try {
					timestamp = getTimestamp(value);
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
				if (timestamp != null) {
					relationship.setCreatedAt(timestamp);
				}

			} else if (propKey.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				Long timestamp;
				try {
					timestamp = getTimestamp(value);
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
				if (timestamp != null) {
					relationship.setModifiedAt(timestamp);
				}

			} else if (propKey.equals(NGSIConstants.JSON_LD_TYPE)) {
				continue;
			} else {
				JsonObject objValue = value.getAsJsonArray().get(0).getAsJsonObject();
				if (objValue.has(NGSIConstants.JSON_LD_TYPE)) {
					String valueType = objValue.get(NGSIConstants.JSON_LD_TYPE).getAsJsonArray().get(0).getAsString();
					if (valueType.equals(NGSIConstants.NGSI_LD_PROPERTY)) {
						properties.add(parseProperty(objValue, propKey));
					} else if (valueType.equals(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
						relationships.add(parseRelationship(objValue, propKey));
					}
				}
			}

		}
		return relationship;
	}

	public static JsonElement getJson(Long timestamp, JsonSerializationContext context) {
		JsonArray observedArray = new JsonArray();
		JsonObject observedObj = new JsonObject();
		observedObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(formatter.format(new Date(timestamp))));
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
	 * @param type
	 *            - to indicate which type of serialization is required For ex (in
	 *            Entity payload).
	 * 
	 *            createdAt must be serialized as :
	 *            "https://uri.etsi.org/ngsi-ld/createdAt": [{ "@type":
	 *            ["https://uri.etsi.org/ngsi-ld/Property"],
	 *            "https://uri.etsi.org/ngsi-ld/hasValue": [{ "@value":
	 *            "2017-07-29T12:00:04" }] }]
	 *
	 *            whereas observedAt must be serialized as : "http:
	 *            //uri.etsi.org/ngsi-ld/observedAt": [{
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
		JsonObject top = new JsonObject();
		JsonArray type = new JsonArray();
		type.add(new JsonPrimitive(property.getType()));
		top.add(NGSIConstants.JSON_LD_TYPE, type);

		top.add(NGSIConstants.NGSI_LD_HAS_VALUE, getJson(property.getValue(), context));
		if (property.getObservedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_OBSERVED_AT, getJson(property.getObservedAt(), context));
		}
		if (property.getCreatedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_CREATED_AT, getJson(property.getCreatedAt(), context));
		}
		if (property.getModifiedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_MODIFIED_AT, getJson(property.getModifiedAt(), context));
		}
		for (Property propOfProp : property.getProperties()) {
			top.add(propOfProp.getName(), getJson(propOfProp, context));
		}
		for (Relationship relaOfProp : property.getRelationships()) {
			top.add(relaOfProp.getName(), getJson(relaOfProp, context));
		}
		result.add(top);
		return result;
	}

	@SuppressWarnings("unchecked")
	private static JsonElement getJson(List<Object> values, JsonSerializationContext context) {
		JsonArray top = new JsonArray();
		for (Object value : values) {
			JsonObject obj;
			if (value instanceof Map) {
				obj = getComplexValue((Map<String, List<Object>>) value, context);
			}
			// else if(value instanceof List) {
			// should never happen
			// }
			else {
				obj = new JsonObject();
				obj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(value));

			}
			top.add(obj);
		}
		return top;
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
		JsonObject top = new JsonObject();
		JsonArray type = new JsonArray();
		type.add(new JsonPrimitive(relationship.getType()));
		top.add(NGSIConstants.JSON_LD_TYPE, type);
		JsonArray value = new JsonArray();
		JsonObject objValue = new JsonObject();
		objValue.add(NGSIConstants.JSON_LD_ID, context.serialize(relationship.getObject()));
		value.add(objValue);
		top.add(NGSIConstants.NGSI_LD_OBJECT, value);
		if (relationship.getObservedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_OBSERVED_AT, getJson(relationship.getObservedAt(), context));
		}
		if (relationship.getCreatedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_CREATED_AT, getJson(relationship.getCreatedAt(), context));
		}
		if (relationship.getModifiedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_MODIFIED_AT, getJson(relationship.getModifiedAt(), context));
		}
		for (Property propOfProp : relationship.getProperties()) {
			top.add(propOfProp.getName(), getJson(propOfProp, context));
		}
		for (Relationship relaOfProp : relationship.getRelationships()) {
			top.add(relaOfProp.getName(), getJson(relaOfProp, context));
		}
		result.add(top);
		return result;
	}

	public static JsonElement getJson(GeoProperty property, JsonSerializationContext context) {
		JsonArray result = new JsonArray();
		JsonObject top = new JsonObject();
		JsonArray type = new JsonArray();
		type.add(new JsonPrimitive(property.getType()));
		top.add(NGSIConstants.JSON_LD_TYPE, type);
		JsonArray value = new JsonArray();
		JsonObject objValue = new JsonObject();
		objValue.add(NGSIConstants.JSON_LD_VALUE, context.serialize(property.getValue()));
		value.add(objValue);
		top.add(NGSIConstants.NGSI_LD_HAS_VALUE, value);
		if (property.getObservedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_OBSERVED_AT, getJson(property.getObservedAt(), context));
		}
		if (property.getCreatedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_CREATED_AT, getJson(property.getCreatedAt(), context));
		}
		if (property.getModifiedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_MODIFIED_AT, getJson(property.getModifiedAt(), context));
		}
		for (Property propOfProp : property.getProperties()) {
			top.add(propOfProp.getName(), getJson(propOfProp, context));
		}
		for (Relationship relaOfProp : property.getRelationships()) {
			top.add(relaOfProp.getName(), getJson(relaOfProp, context));
		}
		result.add(top);
		return result;
	}

	public static JsonElement getJsonForCSource(GeoProperty geoProperty, JsonSerializationContext context) {
		Gson gson = new Gson();
		return gson.fromJson(geoProperty.getValue(), JsonElement.class);
	}

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
