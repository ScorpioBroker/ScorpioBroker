package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
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
				top.add(property.getId().toString(), SerializationTools.getJson(property, context));
			}
		}
		if (entity.getRelationships() != null) {
			for (Relationship relationship : entity.getRelationships()) {
				top.add(relationship.getId().toString(), SerializationTools.getJson(relationship, context));
			}
		}
		if (entity.getGeoProperties() != null) {
			for (GeoProperty geoProperty : entity.getGeoProperties()) {
				top.add(geoProperty.getId().toString(), SerializationTools.getJson(geoProperty, context));
			}
		}
		if (entity.getCreatedAt() != null && entity.getCreatedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_CREATED_AT, SerializationTools.getJson(entity.getCreatedAt(), context));
		}
		if (entity.getModifiedAt() != null && entity.getModifiedAt() > 0) {
			top.add(NGSIConstants.NGSI_LD_MODIFIED_AT, SerializationTools.getJson(entity.getModifiedAt(), context));
		}
		if (entity.getLocation() != null) {
			top.add(entity.getLocation().getId().toString(), SerializationTools.getJson(entity.getLocation(), context));
		}
		if (entity.getObservationSpace() != null) {
			top.add(entity.getObservationSpace().getId().toString(),
					SerializationTools.getJson(entity.getObservationSpace(), context));
		}
		if (entity.getOperationSpace() != null) {
			top.add(entity.getOperationSpace().getId().toString(),
					SerializationTools.getJson(entity.getOperationSpace(), context));
		}
		if (entity.getName() != null) {
			top.add(NGSIConstants.NGSI_LD_NAME, new JsonPrimitive(entity.getName()));
		}
		return top;
	}

	@Override
	public Entity deserialize(JsonElement json, Type classType, JsonDeserializationContext context)
			throws JsonParseException {
		JsonObject top = json.getAsJsonObject();
		URI id = null;
		String type = null;
		String name = null;
		GeoProperty location = null;
		GeoProperty observationSpace = null;
		GeoProperty operationSpace = null;
		ArrayList<Property> properties = new ArrayList<Property>();
		ArrayList<Relationship> relationships = new ArrayList<Relationship>();
		ArrayList<GeoProperty> geoproperties = new ArrayList<GeoProperty>();
		Long createdAt = null, observedAt = null, modifiedAt = null;
		String refToAccessControl = null;
		for (Entry<String, JsonElement> entry : top.entrySet()) {
			String key = entry.getKey();
			JsonObject objValue = null;
			switch (key) {
			case NGSIConstants.JSON_LD_ID:
				try {
					id = new URI(entry.getValue().getAsString());
				} catch (URISyntaxException e1) {
					throw new JsonParseException("ID field is not a valid URI");
				}
				break;
			case NGSIConstants.JSON_LD_TYPE:
				type = entry.getValue().getAsString();
				break;
			case NGSIConstants.NGSI_LD_LOCATION:
				location = SerializationTools.parseGeoProperty(entry.getValue().getAsJsonArray(),
						NGSIConstants.NGSI_LD_LOCATION);
				break;
			case NGSIConstants.NGSI_LD_OPERATION_SPACE:
				operationSpace = SerializationTools.parseGeoProperty(entry.getValue().getAsJsonArray(),
						NGSIConstants.NGSI_LD_OPERATION_SPACE);
				break;
			case NGSIConstants.NGSI_LD_OBSERVATION_SPACE:
				observationSpace = SerializationTools.parseGeoProperty(entry.getValue().getAsJsonArray(),
						NGSIConstants.NGSI_LD_OBSERVATION_SPACE);
				break;
			case NGSIConstants.NGSI_LD_CREATED_AT:
				try {
					createdAt = SerializationTools.date2Long(entry.getValue().getAsJsonArray().get(0).getAsJsonObject()
							.get(NGSIConstants.JSON_LD_VALUE).getAsString());
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
				break;
			case NGSIConstants.NGSI_LD_MODIFIED_AT:
				try {
					modifiedAt = SerializationTools.date2Long(entry.getValue().getAsJsonArray().get(0).getAsJsonObject()
							.get(NGSIConstants.JSON_LD_VALUE).getAsString());
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
				break;
			case NGSIConstants.NGSI_LD_OBSERVED_AT:
				try {
					observedAt = SerializationTools.date2Long(entry.getValue().getAsJsonArray().get(0).getAsJsonObject()
							.get(NGSIConstants.JSON_LD_VALUE).getAsString());
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
				break;
			case NGSIConstants.NGSI_LD_NAME:
				if (!entry.getValue().getAsJsonArray().get(0).getAsJsonObject().has(NGSIConstants.JSON_LD_TYPE)) {
					name = entry.getValue().getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE)
							.getAsString();
					break;
				}
			default:

				JsonArray topLevelArray = entry.getValue().getAsJsonArray();
				objValue = topLevelArray.get(0).getAsJsonObject();

				if (objValue.has(NGSIConstants.JSON_LD_TYPE)) {
					String valueType = objValue.get(NGSIConstants.JSON_LD_TYPE).getAsJsonArray().get(0).getAsString();
					if (valueType.equals(NGSIConstants.NGSI_LD_PROPERTY)) {
						Property property = SerializationTools.parseProperty(topLevelArray, key);
						properties.add(property);
					} else if (valueType.equals(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
						Relationship relationship = SerializationTools.parseRelationship(topLevelArray, key);
						relationships.add(relationship);
					} else if (valueType.equals(NGSIConstants.NGSI_LD_GEOPROPERTY)) {
						GeoProperty geoproperty = SerializationTools.parseGeoProperty(topLevelArray, key);
						geoproperties.add(geoproperty);
					} else {
						throw new JsonParseException("Unknown top level entry provided " + key);
					}
				} else {
					throw new JsonParseException("Unknown top level entry provided " + key);
				}

				break;
			}

		}
		if (id == null && !allowIncomplete) {
			throw new JsonParseException("ID field is mandertory");
		}
		if (type == null && !allowIncomplete) {
			throw new JsonParseException("Type field is mandertory");
		}
		Entity result = new Entity(id, location, observationSpace, operationSpace, properties, refToAccessControl,
				relationships, type, geoproperties);
		if (createdAt != null) {
			result.setCreatedAt(createdAt);
		}
		if (modifiedAt != null) {
			result.setModifiedAt(modifiedAt);
		}
		if (observedAt != null) {
			result.setObservedAt(observedAt);
		}
		if (name != null) {
			result.setName(name);
		}
		return result;
	}

}
