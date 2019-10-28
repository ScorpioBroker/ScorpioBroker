package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EndPoint;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoRelation;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.enums.Format;
import eu.neclab.ngsildbroker.commons.enums.Geometry;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class SubscriptionGsonAdapter implements JsonDeserializer<Subscription>, JsonSerializer<Subscription> {

	@Override
	public Subscription deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		JsonObject top;
		if (json.isJsonArray()) {
			top = json.getAsJsonArray().get(0).getAsJsonObject();
		} else {
			top = json.getAsJsonObject();
		}
		Subscription result = new Subscription();
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
			} else if (key.equals(NGSIConstants.NGSI_LD_ENTITIES)) {
				JsonArray entities = value.getAsJsonArray();
				Iterator<JsonElement> it = entities.iterator();
				while (it.hasNext()) {
					JsonObject obj = it.next().getAsJsonObject();
					EntityInfo entity = new EntityInfo();
					if (obj.has(NGSIConstants.NGSI_LD_ID_PATTERN)) {
						entity.setIdPattern(obj.get(NGSIConstants.NGSI_LD_ID_PATTERN).getAsJsonArray().get(0)
								.getAsJsonObject().get(NGSIConstants.VALUE).getAsString());
					}
					if (obj.has(NGSIConstants.JSON_LD_ID)) {
						try {
							entity.setId(new URI(obj.get(NGSIConstants.JSON_LD_ID).getAsString()));
						} catch (URISyntaxException e) {
							throw new JsonParseException(
									"Invalid Id to subscribe to " + obj.get(NGSIConstants.JSON_LD_ID).getAsString());
						}
					}
					if (obj.has(NGSIConstants.JSON_LD_TYPE)) {
						entity.setType(obj.get(NGSIConstants.JSON_LD_TYPE).getAsJsonArray().get(0).getAsString());
					}
					result.addEntityInfo(entity);
				}

			} else if (key.equals(NGSIConstants.NGSI_LD_GEO_QUERY)) {
				JsonObject query = value.getAsJsonArray().get(0).getAsJsonObject();
				LDGeoQuery geoQuery = new LDGeoQuery();
				Iterator<JsonElement> jsonCoordinates = query.getAsJsonArray(NGSIConstants.NGSI_LD_COORDINATES)
						.iterator();
				ArrayList<Double> coordinates = new ArrayList<Double>();
				while (jsonCoordinates.hasNext()) {
					coordinates.add(
							jsonCoordinates.next().getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsDouble());
				}
				geoQuery.setCoordinates(coordinates);
				String geometry = query.getAsJsonArray(NGSIConstants.NGSI_LD_GEOMETRY).get(0).getAsJsonObject()
						.get(NGSIConstants.JSON_LD_VALUE).getAsString();
				if (geometry.equalsIgnoreCase("point")) {
					geoQuery.setGeometry(Geometry.Point);
				} else if (geometry.equalsIgnoreCase("polygon")) {
					geoQuery.setGeometry(Geometry.Polygon);
				}
				String geoRelString = query.getAsJsonArray(NGSIConstants.NGSI_LD_GEO_REL).get(0).getAsJsonObject()
						.get(NGSIConstants.JSON_LD_VALUE).getAsString();
				String[] relSplit = geoRelString.split(";");
				GeoRelation geoRel = new GeoRelation();
				geoRel.setRelation(relSplit[0]);
				for (int i = 1; i < relSplit.length; i++) {
					String[] temp = relSplit[i].split("==");
					if (temp[0].equalsIgnoreCase("maxDistance")) {
						geoRel.setMaxDistance(Double.parseDouble(temp[1]));
					} else if (temp[0].equalsIgnoreCase("minDistance")) {
						geoRel.setMinDistance(Double.parseDouble(temp[1]));
					}
				}
				geoQuery.setGeoRelation(geoRel);
				result.setLdGeoQuery(geoQuery);

			} else if (key.equals(NGSIConstants.NGSI_LD_NOTIFICATION)) {
				Iterator<JsonElement> attribs = value.getAsJsonArray().get(0).getAsJsonObject()
						.getAsJsonArray(NGSIConstants.NGSI_LD_ATTRIBUTES).iterator();
				ArrayList<String> watchedAttribs = new ArrayList<String>();
				NotificationParam notifyParam = new NotificationParam();
				while (attribs.hasNext()) {
					watchedAttribs.add(attribs.next().getAsJsonObject().get(NGSIConstants.JSON_LD_ID).getAsString());

				}
				notifyParam.setAttributeNames(watchedAttribs);
				EndPoint endPoint = new EndPoint();
				JsonObject jsonEndPoint = value.getAsJsonArray().get(0).getAsJsonObject()
						.getAsJsonArray(NGSIConstants.NGSI_LD_ENDPOINT).get(0).getAsJsonObject();
				endPoint.setAccept(jsonEndPoint.getAsJsonArray(NGSIConstants.NGSI_LD_ACCEPT).get(0).getAsJsonObject()
						.get(NGSIConstants.JSON_LD_VALUE).getAsString());
				try {
					endPoint.setUri(new URI(jsonEndPoint.getAsJsonArray(NGSIConstants.NGSI_LD_URI).get(0)
							.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString()));
				} catch (URISyntaxException e) {
					throw new JsonParseException(e);
				}
				notifyParam.setEndPoint(endPoint);
				String formatString = value.getAsJsonArray().get(0).getAsJsonObject()
						.getAsJsonArray(NGSIConstants.NGSI_LD_FORMAT).get(0).getAsJsonObject()
						.get(NGSIConstants.JSON_LD_VALUE).getAsString();
				if (formatString.equalsIgnoreCase("keyvalues")) {
					notifyParam.setFormat(Format.keyValues);
				} else if (formatString.equalsIgnoreCase("normalized")) {
					notifyParam.setFormat(Format.normalized);
				}

				result.setNotification(notifyParam);

			} else if (key.equals(NGSIConstants.NGSI_LD_QUERY)) {
				result.setLdQuery(
						value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
			} else if (key.equals(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES)) {
				Iterator<JsonElement> it = value.getAsJsonArray().iterator();
				ArrayList<String> watched = new ArrayList<String>();
				while (it.hasNext()) {
					watched.add(it.next().getAsJsonObject().get(NGSIConstants.JSON_LD_ID).getAsString());
				}
				result.setAttributeNames(watched);
			} else if (key.equals(NGSIConstants.NGSI_LD_THROTTLING)) {
				result.setThrottling(
						value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsInt());
			} else if (key.equals(NGSIConstants.NGSI_LD_TIME_INTERVAL)) {
				result.setTimeInterval(
						value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsInt());
			} else if (key.equals(NGSIConstants.NGSI_LD_EXPIRES)) {
				result.setExpires(SerializationTools.date2Long(value.getAsJsonArray().get(0).getAsJsonObject()
						.get(NGSIConstants.JSON_LD_VALUE).getAsString()));
			} else if (key.equals(NGSIConstants.NGSI_LD_STATUS)) {
				result.setStatus(
						value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
			} else if (key.equals(NGSIConstants.NGSI_LD_DESCRIPTION)) {
				result.setDescription(
						value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
			}

		}

//		if (result.getId() == null) {
//			throw new JsonParseException("Id is missing");
//		}
		return result;
	}

	@Override
	public JsonElement serialize(Subscription src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject top = new JsonObject();
		top.add(NGSIConstants.JSON_LD_ID, context.serialize(src.getId().toString()));
		top.add(NGSIConstants.JSON_LD_TYPE, context.serialize(src.getType()));
		JsonArray temp = new JsonArray();
		if (src.getEntities() != null) {
			for (EntityInfo info : src.getEntities()) {
				JsonObject entityObj = new JsonObject();
				if (info.getId() != null) {
					JsonArray temp2 = new JsonArray();
					temp2.add(info.getId().toString());
					entityObj.add(NGSIConstants.JSON_LD_ID, temp2);
				}
				if (info.getType() != null) {
					JsonArray temp2 = new JsonArray();
					temp2.add(info.getType());
					entityObj.add(NGSIConstants.JSON_LD_TYPE, temp2);
				}
				if (info.getIdPattern() != null) {
					JsonArray temp2 = new JsonArray();
					JsonObject tempObj = new JsonObject();
					tempObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(info.getIdPattern()));

					temp2.add(tempObj);
					entityObj.add(NGSIConstants.NGSI_LD_ID_PATTERN, temp2);
				}
				temp.add(entityObj);
			}
			if (temp.size() > 0) {
				top.add(NGSIConstants.NGSI_LD_ENTITIES, temp);
			}
		}
		if (src.getLdGeoQuery() != null) {
			temp = new JsonArray();
			JsonObject geoObj = new JsonObject();
			JsonArray coordArray = new JsonArray();
			for (Double coordinate : src.getLdGeoQuery().getCoordinates()) {
				JsonObject tempObj = new JsonObject();
				tempObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(coordinate));
				coordArray.add(tempObj);
			}
			geoObj.add(NGSIConstants.NGSI_LD_COORDINATES, coordArray);
			JsonArray temp2 = new JsonArray();
			JsonObject tempObj = new JsonObject();
			tempObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(src.getLdGeoQuery().getGeometry().toString()));
			temp2.add(tempObj);
			geoObj.add(NGSIConstants.NGSI_LD_GEOMETRY, temp2);
			if (src.getLdGeoQuery().getGeoRelation() != null) {
				temp2 = new JsonArray();
				tempObj = new JsonObject();
				tempObj.add(NGSIConstants.JSON_LD_VALUE,
						context.serialize(src.getLdGeoQuery().getGeoRelation().getABNFString()));
				temp2.add(tempObj);
				geoObj.add(NGSIConstants.NGSI_LD_GEO_REL, temp2);
			}
			temp.add(geoObj);
			top.add(NGSIConstants.NGSI_LD_GEO_QUERY, temp);
		}
		temp = new JsonArray();
		JsonObject notificationObj = new JsonObject();
		JsonArray attribs = new JsonArray();
		for (String attrib : src.getNotification().getAttributeNames()) {
			JsonObject tempObj = new JsonObject();
			tempObj.add(NGSIConstants.JSON_LD_ID, context.serialize(attrib));
			attribs.add(tempObj);
		}
		notificationObj.add(NGSIConstants.NGSI_LD_ATTRIBUTES, attribs);
		JsonObject endPoint = new JsonObject();
		JsonArray endPointArray = new JsonArray();
		if (src.getNotification().getEndPoint().getAccept() != null) {
			JsonArray tempArray = new JsonArray();
			JsonObject tempObj = new JsonObject();
			tempObj.add(NGSIConstants.JSON_LD_VALUE,
					context.serialize(src.getNotification().getEndPoint().getAccept()));
			tempArray.add(tempObj);
			endPoint.add(NGSIConstants.NGSI_LD_ACCEPT, tempArray);
		}
		JsonArray tempArray = new JsonArray();
		JsonObject tempObj = new JsonObject();
		tempObj.add(NGSIConstants.JSON_LD_VALUE,
				context.serialize(src.getNotification().getEndPoint().getUri().toString()));
		tempArray.add(tempObj);
		endPoint.add(NGSIConstants.NGSI_LD_URI, tempArray);
		endPointArray.add(endPoint);
		notificationObj.add(NGSIConstants.NGSI_LD_ENDPOINT, endPointArray);
		tempArray = new JsonArray();
		tempObj = new JsonObject();
		tempObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(src.getNotification().getFormat().toString()));
		tempArray.add(tempObj);
		notificationObj.add(NGSIConstants.NGSI_LD_FORMAT, tempArray);
		temp.add(notificationObj);
		top.add(NGSIConstants.NGSI_LD_NOTIFICATION, temp);
		if (src.getLdQuery() != null) {
			tempArray = new JsonArray();
			tempObj = new JsonObject();
			tempObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(src.getLdQuery()));
			tempArray.add(tempObj);
			top.add(NGSIConstants.NGSI_LD_QUERY, tempArray);
		}

		attribs = new JsonArray();
		for (String attrib : src.getAttributeNames()) {
			tempObj = new JsonObject();
			tempObj.add(NGSIConstants.JSON_LD_ID, context.serialize(attrib));
			attribs.add(tempObj);
		}
		if (attribs.size() > 0) {
			top.add(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES, attribs);
		}
		if (src.getThrottling() != null) {
			top.add(NGSIConstants.NGSI_LD_THROTTLING, SerializationTools.getValueArray(src.getThrottling()));
		}
		if (src.getTimeInterval() != null) {
			top.add(NGSIConstants.NGSI_LD_TIME_INTERVAL, SerializationTools.getValueArray(src.getTimeInterval()));
		}
		if (src.getExpires() != null) {
			top.add(NGSIConstants.NGSI_LD_EXPIRES, SerializationTools.getValueArray(src.getExpires()));
		}
		if (src.getStatus() != null) {
			top.add(NGSIConstants.NGSI_LD_STATUS, SerializationTools.getValueArray(src.getStatus()));
		}
		if (src.getDescription() != null) {
			top.add(NGSIConstants.NGSI_LD_DESCRIPTION, SerializationTools.getValueArray(src.getDescription()));
		}

		return top;
	}

}
