package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import javax.sql.rowset.serial.SerialException;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.Information;
import eu.neclab.ngsildbroker.commons.datatypes.TimeInterval;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

// TODO : complete serializer - include other fields also i.e. location,name etc.
public class CSourceRegistrationGsonAdapter
		implements JsonDeserializer<CSourceRegistration>, JsonSerializer<CSourceRegistration> {
	
	

	@Override
	public JsonElement serialize(CSourceRegistration src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject top = new JsonObject();
		top.add(NGSIConstants.JSON_LD_ID, context.serialize(src.getId().toString()));
		
		JsonArray jsonArray = new JsonArray();
		jsonArray.add(src.getType());
		top.add(NGSIConstants.JSON_LD_TYPE, jsonArray);
		
		jsonArray = new JsonArray();
		JsonObject jsonObject = new JsonObject();
		jsonObject.add(NGSIConstants.JSON_LD_VALUE, context.serialize(src.getEndpoint().toString()));
		jsonArray.add(jsonObject);
		top.add(NGSIConstants.NGSI_LD_ENDPOINT, jsonArray);

		jsonArray = new JsonArray();
		jsonObject = new JsonObject();
		jsonObject.add(NGSIConstants.JSON_LD_VALUE, context.serialize(src.isInternal()));
		jsonArray.add(jsonObject);
		top.add(NGSIConstants.NGSI_LD_INTERNAL, jsonArray);
					
		jsonArray = new JsonArray();
		if (src.getInformation() != null) {
			for (Information info : src.getInformation()) {
				JsonObject infoObject = new JsonObject();
				Set<String> properties = info.getProperties();
				Set<String> relationsships = info.getRelationships();
				List<EntityInfo> entities = info.getEntities();

				JsonArray attribs = new JsonArray();
				for (String property : properties) {
					JsonObject tempObj = new JsonObject();
					tempObj.add(NGSIConstants.JSON_LD_ID, context.serialize(property));
					attribs.add(tempObj);
				}
				infoObject.add(NGSIConstants.NGSI_LD_PROPERTIES, attribs);

				attribs = new JsonArray();
				for (String relation : relationsships) {
					JsonObject tempObj = new JsonObject();
					tempObj.add(NGSIConstants.JSON_LD_ID, context.serialize(relation));
					attribs.add(tempObj);
				}
				infoObject.add(NGSIConstants.NGSI_LD_RELATIONSHIPS, attribs);

				attribs = new JsonArray();
				JsonArray tempArray = new JsonArray();
				if (entities != null) {
					for (EntityInfo entityInfo : entities) {
						JsonObject entityObj = new JsonObject();
						if (entityInfo.getId() != null) {
							entityObj.add(NGSIConstants.JSON_LD_ID, context.serialize(entityInfo.getId().toString()));
						}
						if (entityInfo.getType() != null) {
							JsonArray temp2 = new JsonArray();
							temp2.add(entityInfo.getType());
							entityObj.add(NGSIConstants.JSON_LD_TYPE, temp2);
						}
						if (entityInfo.getIdPattern() != null) {
							JsonArray temp2 = new JsonArray();
							jsonObject = new JsonObject();
							jsonObject.add(NGSIConstants.JSON_LD_VALUE, context.serialize(entityInfo.getIdPattern()));
							temp2.add(jsonObject);
							entityObj.add(NGSIConstants.NGSI_LD_ID_PATTERN, temp2);
						}
						tempArray.add(entityObj);
					}
					if (tempArray.size() > 0) {
						infoObject.add(NGSIConstants.NGSI_LD_ENTITIES, tempArray);
					}
				}
				jsonArray.add(infoObject);
				top.add(NGSIConstants.NGSI_LD_INFORMATION, jsonArray);
			}
		}
		if (src.getTimestamp() != null) {
			jsonArray = new JsonArray();
			JsonObject timestampObject = new JsonObject();
			if (src.getTimestamp().getStart() != null) {
				
				timestampObject.add(NGSIConstants.NGSI_LD_TIMESTAMP_START, SerializationTools.getJson(src.getTimestamp().getStart(), context));
			}
			if (src.getTimestamp().getStop() != null) {
				
				timestampObject.add(NGSIConstants.NGSI_LD_TIMESTAMP_END, SerializationTools.getJson(src.getTimestamp().getStop(), context));
			}
			jsonArray.add(timestampObject);

			top.add(NGSIConstants.NGSI_LD_TIME_STAMP, jsonArray);
		}
		
		if (src.getLocation() != null) {
			jsonArray = new JsonArray();
			jsonObject = new JsonObject();						
			jsonObject.add(NGSIConstants.JSON_LD_VALUE, SerializationTools.getJson(src.getLocation()));
			jsonArray.add(jsonObject);
			top.add(NGSIConstants.NGSI_LD_LOCATION, jsonArray);			
		}
		
		if(src.getExpires()!=null) {
			top.add(NGSIConstants.NGSI_LD_EXPIRES, SerializationTools.getJson(src.getExpires(), context));
		}
		
		return top;
	}

	@Override
	public CSourceRegistration deserialize(JsonElement json, Type type, JsonDeserializationContext context)
			throws JsonParseException {

		JsonObject top = json.getAsJsonObject();
		CSourceRegistration result = new CSourceRegistration();

		for (Entry<String, JsonElement> entry : top.entrySet()) {
			String key = entry.getKey();
			JsonElement value = entry.getValue();
			if (key.equals(NGSIConstants.JSON_LD_ID)) {
				try {
					result.setId(new URI(value.getAsString()));
				} catch (URISyntaxException e) {
					throw new JsonParseException("Invalid Id " + value.getAsString());
				}
			} else if (key.equals(NGSIConstants.JSON_LD_TYPE)) {
				result.setType(value.getAsString());
			} else if (key.equals(NGSIConstants.NGSI_LD_INTERNAL)) {
				result.setInternal(value.getAsJsonArray().get(0).getAsJsonObject()
						.get(NGSIConstants.JSON_LD_VALUE).getAsBoolean());
			} else if (key.equals(NGSIConstants.NGSI_LD_ENDPOINT)) {
				try {
					result.setEndpoint(new URI(value.getAsJsonArray().get(0).getAsJsonObject()
							.get(NGSIConstants.JSON_LD_VALUE).getAsString()));
				} catch (URISyntaxException e) {
					throw new JsonParseException("Invalid endpoint uri " + value.getAsString());
				}
			} else if (key.equals(NGSIConstants.NGSI_LD_INFORMATION)) {
				List<Information> information = new ArrayList<Information>();
				JsonArray jsonEntities = value.getAsJsonArray();
				Iterator<JsonElement> it = jsonEntities.iterator();
				while (it.hasNext()) {
					Information info = new Information();
					List<EntityInfo> entities = info.getEntities();
					Set<String> properties = info.getProperties();
					Set<String> relationships = info.getRelationships();
					information.add(info);
					JsonObject obj = it.next().getAsJsonObject();
					if (obj.has(NGSIConstants.NGSI_LD_ENTITIES)) {
						Iterator<JsonElement> entityIterator = obj.get(NGSIConstants.NGSI_LD_ENTITIES).getAsJsonArray()
								.iterator();
						while (entityIterator.hasNext()) {
							EntityInfo entityInfo = new EntityInfo();
							JsonObject entityObject = entityIterator.next().getAsJsonObject();
							if (entityObject.has(NGSIConstants.JSON_LD_ID)) {
								try {
									entityInfo.setId(new URI(entityObject.get(NGSIConstants.JSON_LD_ID).getAsString()));
								} catch (URISyntaxException e) {
									// TODO Check whether URI in EntityInfo for ID is correct.
								}
							}
							if (entityObject.has(NGSIConstants.JSON_LD_TYPE)) {
								entityInfo.setType(entityObject.get(NGSIConstants.JSON_LD_TYPE).getAsString());
							}
							if (entityObject.has(NGSIConstants.NGSI_LD_ID_PATTERN)) {
								entityInfo.setIdPattern(
										entityObject.get(NGSIConstants.NGSI_LD_ID_PATTERN).getAsJsonArray().get(0)
												.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
							}
							entities.add(entityInfo);
						}
					}
					if (obj.has(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
						Iterator<JsonElement> attribs = obj.get(NGSIConstants.NGSI_LD_RELATIONSHIPS).getAsJsonArray()
								.iterator();
						while (attribs.hasNext()) {
							relationships
									.add(attribs.next().getAsJsonObject().get(NGSIConstants.JSON_LD_ID).getAsString());
						}
					}
					if (obj.has(NGSIConstants.NGSI_LD_PROPERTIES)) {
						Iterator<JsonElement> attribs = obj.get(NGSIConstants.NGSI_LD_PROPERTIES).getAsJsonArray()
								.iterator();
						while (attribs.hasNext()) {
							properties
									.add(attribs.next().getAsJsonObject().get(NGSIConstants.JSON_LD_ID).getAsString());
						}
					}
				}
				result.setInformation(information);
			} else if (key.equals(NGSIConstants.NGSI_LD_LOCATION)) {	
				String geoValue = value.getAsJsonArray().get(0).getAsJsonObject()
						.get(NGSIConstants.JSON_LD_VALUE).getAsString();
				result.setLocation( DataSerializer.getGeojsonGeometry(geoValue) );
			} else if (key.equals(NGSIConstants.NGSI_LD_TIME_STAMP)) {
				result.setTimestamp(new TimeInterval());
				JsonObject timestampObject = value.getAsJsonArray().get(0).getAsJsonObject();
				if (timestampObject.has(NGSIConstants.NGSI_LD_TIMESTAMP_START)) {
					String dateTime = timestampObject.get(NGSIConstants.NGSI_LD_TIMESTAMP_START).getAsJsonArray().get(0)
							.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString();
					try {
						result.getTimestamp().setStart(SerializationTools.date2Long(dateTime));
					} catch (Exception e) {
						throw new JsonParseException(e.getMessage());
					}
				}
				if (timestampObject.has(NGSIConstants.NGSI_LD_TIMESTAMP_END)) {
					String dateTime = timestampObject.get(NGSIConstants.NGSI_LD_TIMESTAMP_END).getAsJsonArray().get(0)
							.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString();
					try {
						result.getTimestamp().setStop(SerializationTools.date2Long(dateTime));
					} catch (Exception e) {
						throw new JsonParseException(e.getMessage());
					}
				}
			}else if(key.equals(NGSIConstants.NGSI_LD_EXPIRES)) {
				String expires=value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString();
				try {
					result.setExpires(SerializationTools.date2Long(expires));
				} catch (Exception e) {
					throw new JsonParseException(e.getMessage());
				}
			}
		}
		return result;
	}

	// private JsonArray parseGeoLocation(JsonNode locationGeoJson) {
	// String type = locationGeoJson.get(NGSIConstants.CSOURCE_TYPE).asText();
	// JsonArray jsonArray = new JsonArray();
	// JsonObject jsonObject = new JsonObject();
	// JsonArray typeArray = new JsonArray();
	// typeArray.add(type);
	// jsonObject.add(NGSIConstants.JSON_LD_TYPE, typeArray);
	//
	// JsonArray coordinatesArray = new JsonArray();
	// JsonNode coordinatesJson =
	// locationGeoJson.get(NGSIConstants.CSOURCE_COORDINATES);
	// if (type.equals(NGSIConstants.GEO_TYPE_POINT)) {
	// if (coordinatesJson.isArray()) {
	// for (final JsonNode objNode : coordinatesJson) {
	// JsonObject tempJsonObject = new JsonObject();
	// tempJsonObject.addProperty(NGSIConstants.JSON_LD_VALUE, objNode.asDouble());
	// coordinatesArray.add(tempJsonObject);
	// }
	// }
	// jsonObject.add(NGSIConstants.NGSI_LD_COORDINATES, coordinatesArray);
	// } else if (type.equals(NGSIConstants.GEO_TYPE_POLYGON)) {
	// coordinatesJson = coordinatesJson.get(0);
	// if (coordinatesJson.isArray()) {
	// Iterator it = coordinatesJson.iterator();
	// while (it.hasNext()) {
	// JsonNode node = (JsonNode) it.next();
	// for (final JsonNode objNode : node) {
	// JsonObject tempJsonObject = new JsonObject();
	// tempJsonObject.addProperty(NGSIConstants.JSON_LD_VALUE, objNode.asDouble());
	// coordinatesArray.add(tempJsonObject);
	// }
	// }
	// }
	// jsonObject.add(NGSIConstants.NGSI_LD_COORDINATES, coordinatesArray);
	// } else if (type.equals(NGSIConstants.GEO_TYPE_LINESTRING)) {
	// coordinatesJson = coordinatesJson.get(0);
	// if (coordinatesJson.isArray()) {
	// Iterator it = coordinatesJson.iterator();
	// while (it.hasNext()) {
	// JsonNode node = (JsonNode) it.next();
	// for (final JsonNode objNode : node) {
	// JsonObject tempJsonObject = new JsonObject();
	// tempJsonObject.addProperty(NGSIConstants.JSON_LD_VALUE, objNode.asDouble());
	// coordinatesArray.add(tempJsonObject);
	// }
	// }
	// }
	// jsonObject.add(NGSIConstants.NGSI_LD_COORDINATES, coordinatesArray);
	// }
	// jsonArray.add(jsonObject);
	// return jsonArray;
	// }
}
