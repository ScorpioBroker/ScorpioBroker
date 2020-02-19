package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map.Entry;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.datatypes.Relationship;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class EntityGsonAdapter implements JsonDeserializer<Entity>, JsonSerializer<Entity> {

	private boolean allowIncomplete;

	public EntityGsonAdapter(boolean allowIncomplete) {
		this.allowIncomplete = allowIncomplete;
	}

	@Override
	public JsonElement serialize(Entity entity, Type type, JsonSerializationContext context) {
		JsonObject top = new JsonObject();
		top.add(NGSIConstants.JSON_LD_ID, context.serialize(entity.getId()));
		top.add(NGSIConstants.JSON_LD_TYPE, context.serialize(entity.getType()));
		if (entity.getProperties() != null) {
			for (Property property : entity.getProperties()) {
				top.add(property.getName(), SerializationTools.getJson(property, context));
			}
		}
		if (entity.getRelationships() != null) {
			for (Relationship relationship : entity.getRelationships()) {
				top.add(relationship.getName(), SerializationTools.getJson(relationship, context));
			}
		}
		if (entity.getGeoProperties() != null) {
			for (GeoProperty geoProperty : entity.getGeoProperties()) {
				top.add(geoProperty.getName(), SerializationTools.getJson(geoProperty, context));
			}
		}
		if (entity.getCreatedAt() != null && entity.getCreatedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_CREATED_AT, SerializationTools.getJson(entity.getCreatedAt(), context));
		}
		if (entity.getModifiedAt() != null && entity.getModifiedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_MODIFIED_AT, SerializationTools.getJson(entity.getModifiedAt(), context));
		}
		if (entity.getLocation() != null) {
			top.add(entity.getLocation().getName(), SerializationTools.getJson(entity.getLocation(), context));
		}
		if (entity.getObservationSpace() != null) {
			top.add(entity.getObservationSpace().getName(),
					SerializationTools.getJson(entity.getObservationSpace(), context));
		}
		if (entity.getOperationSpace() != null) {
			top.add(entity.getOperationSpace().getName(),
					SerializationTools.getJson(entity.getOperationSpace(), context));
		}
		return top;
	}

	@Override
	public Entity deserialize(JsonElement json, Type classType, JsonDeserializationContext context)
			throws JsonParseException {
		JsonObject top = json.getAsJsonObject();

		URI id = null;
		if (top.has(NGSIConstants.JSON_LD_ID)) {
			try {
				id = new URI(top.get(NGSIConstants.JSON_LD_ID).getAsString());
			} catch (URISyntaxException e1) {
				if (!allowIncomplete) {
					throw new JsonParseException("ID field is not a valid URI");
				}
			}
		} else {
			if (!allowIncomplete) {
				throw new JsonParseException("ID field is mandertory");
			}
		}
		String type = null;
		if (top.has(NGSIConstants.JSON_LD_TYPE)) {
			type = top.get(NGSIConstants.JSON_LD_TYPE).getAsString();
		} else {
			if (!allowIncomplete) {
				throw new JsonParseException("type field is mandertory");
			}
		}

		GeoProperty location = null;
		GeoProperty observationSpace = null;
		GeoProperty operationSpace = null;
		ArrayList<Property> properties = new ArrayList<Property>();
		ArrayList<Relationship> relationships = new ArrayList<Relationship>();
		ArrayList<GeoProperty> geoproperties = new ArrayList<GeoProperty>();

		String refToAccessControl = null;
		for (Entry<String, JsonElement> entry : top.entrySet()) {
			String key = entry.getKey();
			if (key.equals(NGSIConstants.NGSI_LD_LOCATION) || key.equals(NGSIConstants.NGSI_LD_OPERATION_SPACE)
					|| key.equals(NGSIConstants.NGSI_LD_OBSERVATION_SPACE)
					|| key.equals(NGSIConstants.NGSI_LD_CREATED_AT) || key.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)
					|| key.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
				continue;
			}
			JsonElement value = entry.getValue();
			if (value.isJsonArray()) {
				value = value.getAsJsonArray().get(0);
			}
			if (value.isJsonObject()) {
				JsonObject objValue = value.getAsJsonObject();
				if (objValue.has(NGSIConstants.JSON_LD_TYPE)) {
					String valueType = objValue.get(NGSIConstants.JSON_LD_TYPE).getAsJsonArray().get(0).getAsString();
					if (valueType.equals(NGSIConstants.NGSI_LD_PROPERTY)) {
						Property property = SerializationTools.parseProperty(objValue, key);
						properties.add(property);
					} else if (valueType.equals(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
						Relationship relationship = SerializationTools.parseRelationship(objValue, key);
						relationships.add(relationship);
					} else if (valueType.equals(NGSIConstants.NGSI_LD_GEOPROPERTY)) {
						GeoProperty geoproperty = SerializationTools.parseGeoProperty(objValue, key);
						geoproperties.add(geoproperty);
					} else {
						throw new JsonParseException("Unknown top level entry provided " + key);
					}
				}
			}
		}

		if (top.has(NGSIConstants.NGSI_LD_LOCATION)) {
			JsonObject objValue = top.getAsJsonArray(NGSIConstants.NGSI_LD_LOCATION).get(0).getAsJsonObject();
			location = SerializationTools.parseGeoProperty(objValue, NGSIConstants.NGSI_LD_LOCATION);
		}
		if (top.has(NGSIConstants.NGSI_LD_OPERATION_SPACE)) {
			JsonObject objValue = top.getAsJsonArray(NGSIConstants.NGSI_LD_OPERATION_SPACE).get(0).getAsJsonObject();
			operationSpace = SerializationTools.parseGeoProperty(objValue, NGSIConstants.NGSI_LD_OPERATION_SPACE);
		}
		if (top.has(NGSIConstants.NGSI_LD_OBSERVATION_SPACE)) {
			JsonObject objValue = top.getAsJsonArray(NGSIConstants.NGSI_LD_OBSERVATION_SPACE).get(0).getAsJsonObject();
			observationSpace = SerializationTools.parseGeoProperty(objValue, NGSIConstants.NGSI_LD_OBSERVATION_SPACE);
		}
		Entity result = new Entity(id, location, observationSpace, operationSpace, properties, refToAccessControl,
				relationships, type, geoproperties);
		if (top.has(NGSIConstants.NGSI_LD_CREATED_AT)) {
			Long timestamp = null;
			try {
				timestamp = SerializationTools.date2Long(top.getAsJsonArray(NGSIConstants.NGSI_LD_CREATED_AT).get(0)
						.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
			} catch (Exception e) {
				throw new JsonParseException(e);
			}
			if (timestamp != null) {
				result.setCreatedAt(timestamp);
			}

		}
		if (top.has(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
			Long timestamp = null;
			try {
				timestamp = SerializationTools.date2Long(top.getAsJsonArray(NGSIConstants.NGSI_LD_MODIFIED_AT).get(0)
						.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
			} catch (Exception e) {
				throw new JsonParseException(e);
			}

			if (timestamp != null) {
				result.setModifiedAt(timestamp);
			}

		}

		return result;
	}

}
