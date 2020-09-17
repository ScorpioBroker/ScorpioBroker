package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
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
								.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
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
					} else {
						throw new JsonParseException("type is a mandatory field in all entries of entities");
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
					Object distance;
					try {
						distance = Integer.parseInt(temp[1]);
					} catch (NumberFormatException e) {
						distance = Double.parseDouble(temp[1]);
					}
					if (temp[0].equalsIgnoreCase("maxDistance")) {

						geoRel.setMaxDistance(distance);
					} else if (temp[0].equalsIgnoreCase("minDistance")) {
						geoRel.setMinDistance(distance);
					}
				}
				geoQuery.setGeoRelation(geoRel);
				result.setLdGeoQuery(geoQuery);

			} else if (key.equals(NGSIConstants.NGSI_LD_NOTIFICATION)) {
				ArrayList<String> watchedAttribs = new ArrayList<String>();
				NotificationParam notifyParam = new NotificationParam();
				JsonObject ldObj = value.getAsJsonArray().get(0).getAsJsonObject();
				if (ldObj.has(NGSIConstants.NGSI_LD_ATTRIBUTES)
						&& ldObj.get(NGSIConstants.NGSI_LD_ATTRIBUTES).isJsonArray()) {
					Iterator<JsonElement> attribs = ldObj.getAsJsonArray(NGSIConstants.NGSI_LD_ATTRIBUTES).iterator();
					while (attribs.hasNext()) {
						watchedAttribs
								.add(attribs.next().getAsJsonObject().get(NGSIConstants.JSON_LD_ID).getAsString());

					}
				}
				notifyParam.setAttributeNames(watchedAttribs);
				EndPoint endPoint = new EndPoint();
				JsonObject jsonEndPoint = ldObj.getAsJsonArray(NGSIConstants.NGSI_LD_ENDPOINT).get(0).getAsJsonObject();
				if (jsonEndPoint.has(NGSIConstants.NGSI_LD_ACCEPT)) {
					endPoint.setAccept(jsonEndPoint.getAsJsonArray(NGSIConstants.NGSI_LD_ACCEPT).get(0)
							.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
				} else {
					endPoint.setAccept(AppConstants.NGB_APPLICATION_JSON);
				}
				try {
					endPoint.setUri(new URI(jsonEndPoint.getAsJsonArray(NGSIConstants.NGSI_LD_URI).get(0)
							.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString()));
					Map<String,String> infoSettingNotifier = new HashMap<String,String>();
					// add endpoint notification notifierInfo for deserialization
					if (jsonEndPoint.has(NGSIConstants.NGSI_LD_NOTIFIERINFO)
							&& jsonEndPoint.get(NGSIConstants.NGSI_LD_NOTIFIERINFO).isJsonArray()) {
						JsonObject info = jsonEndPoint.getAsJsonArray(NGSIConstants.NGSI_LD_NOTIFIERINFO).get(0).getAsJsonObject();
						String mqttQos = info.getAsJsonArray(NGSIConstants.NGSI_LD_MQTT_QOS).get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString();
						String mqttVersion = info.getAsJsonArray(NGSIConstants.NGSI_LD_MQTT_VERSION).get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString();
						infoSettingNotifier.put(NGSIConstants.MQTT_QOS, mqttQos);
						infoSettingNotifier.put(NGSIConstants.MQTT_VERSION, mqttVersion);
						endPoint.setNotifierInfo(infoSettingNotifier);
					} 
				} catch (URISyntaxException e) {
					throw new JsonParseException(e);
				}
				notifyParam.setEndPoint(endPoint);
				if (ldObj.has(NGSIConstants.NGSI_LD_FORMAT) && ldObj.getAsJsonArray(NGSIConstants.NGSI_LD_FORMAT).get(0)
						.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE) != null) {
					String formatString = ldObj.getAsJsonArray(NGSIConstants.NGSI_LD_FORMAT).get(0).getAsJsonObject()
							.get(NGSIConstants.JSON_LD_VALUE).getAsString();
					if (formatString.equalsIgnoreCase("keyvalues")) {
						notifyParam.setFormat(Format.keyValues);
					} else if (formatString.equalsIgnoreCase("normalized")) {
						notifyParam.setFormat(Format.normalized);
					}
				} else {
					// Default
					notifyParam.setFormat(Format.normalized);
				}
				if (ldObj.has(NGSIConstants.NGSI_LD_LAST_FAILURE)) {
					TemporalAccessor temp = SerializationTools.formatter
							.parse(ldObj.getAsJsonArray(NGSIConstants.NGSI_LD_LAST_FAILURE).get(0).getAsJsonObject()
									.get(NGSIConstants.JSON_LD_VALUE).getAsString());
					notifyParam.setLastFailedNotification(new Date(Instant.from(temp).toEpochMilli()));
				}
				if (ldObj.has(NGSIConstants.NGSI_LD_LAST_SUCCESS)) {
					TemporalAccessor temp = SerializationTools.formatter
							.parse(ldObj.getAsJsonArray(NGSIConstants.NGSI_LD_LAST_SUCCESS).get(0).getAsJsonObject()
									.get(NGSIConstants.JSON_LD_VALUE).getAsString());
					notifyParam.setLastNotification(new Date(Instant.from(temp).toEpochMilli()));

				}
				if (ldObj.has(NGSIConstants.NGSI_LD_TIMES_SEND)) {
					notifyParam.setTimesSent(ldObj.getAsJsonArray(NGSIConstants.NGSI_LD_TIMES_SEND).get(0)
							.getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsInt());
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
				try {
					result.setExpires(SerializationTools.date2Long(value.getAsJsonArray().get(0).getAsJsonObject()
							.get(NGSIConstants.JSON_LD_VALUE).getAsString()));
				} catch (Exception e) {
					throw new JsonParseException(e);
				}
			} else if (key.equals(NGSIConstants.NGSI_LD_STATUS)) {
				result.setStatus(
						value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
			} else if (key.equals(NGSIConstants.NGSI_LD_DESCRIPTION)) {
				result.setDescription(
						value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
			}

		}
		if (result.getNotification() == null) {
			throw new JsonParseException("no notification parameter provided");
		}
		// if (result.getId() == null) {
		//
		// }
		return result;
	}

	@Override
	public JsonElement serialize(Subscription src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject top = new JsonObject();
		// remote subs have no id yet
		if (src.getId() != null) {
			top.add(NGSIConstants.JSON_LD_ID, context.serialize(src.getId().toString()));
		}
		top.add(NGSIConstants.JSON_LD_TYPE, context.serialize(src.getType()));
		JsonArray temp = new JsonArray();
		if (src.getEntities() != null) {
			for (EntityInfo info : src.getEntities()) {
				JsonObject entityObj = new JsonObject();
				if (info.getId() != null) {
					JsonArray temp2 = new JsonArray();
					temp2.add(info.getId().toString());
					entityObj.add(NGSIConstants.JSON_LD_ID, new JsonPrimitive(info.getId().toString()));// temp2);
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
		JsonObject tempObj;
		JsonArray tempArray;
		if (src.getNotification() != null) {
			NotificationParam notification = src.getNotification();
			if (notification.getAttributeNames() != null) {
				for (String attrib : notification.getAttributeNames()) {
					tempObj = new JsonObject();
					tempObj.add(NGSIConstants.JSON_LD_ID, context.serialize(attrib));
					attribs.add(tempObj);
				}
				notificationObj.add(NGSIConstants.NGSI_LD_ATTRIBUTES, attribs);
			}

			JsonObject endPoint = new JsonObject();
			JsonArray endPointArray = new JsonArray();

			if (notification.getEndPoint() != null) {

				if (notification.getEndPoint().getAccept() != null) {
					tempArray = new JsonArray();
					tempObj = new JsonObject();
					tempObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(notification.getEndPoint().getAccept()));
					tempArray.add(tempObj);
					endPoint.add(NGSIConstants.NGSI_LD_ACCEPT, tempArray);
				}
				if (notification.getEndPoint().getUri() != null) {
					tempArray = new JsonArray();
					tempObj = new JsonObject();
					tempObj.add(NGSIConstants.JSON_LD_VALUE,
							context.serialize(notification.getEndPoint().getUri().toString()));
					tempArray.add(tempObj);
					endPoint.add(NGSIConstants.NGSI_LD_URI, tempArray);
				}
				// add endpoint notification notifierInfo for serialization
				if (notification.getEndPoint().getNotifierInfo() != null) {
					JsonObject notifierEndPoint = new JsonObject();
					JsonArray notifierEndPointArray = new JsonArray();
					if (notification.getEndPoint().getNotifierInfo().get(NGSIConstants.MQTT_QOS) != null) {
						tempArray = new JsonArray();
						tempObj = new JsonObject();
						tempObj.add(NGSIConstants.JSON_LD_VALUE, context
								.serialize(notification.getEndPoint().getNotifierInfo().get(NGSIConstants.MQTT_QOS)));
						tempArray.add(tempObj);
						notifierEndPoint.add(NGSIConstants.NGSI_LD_MQTT_QOS, tempArray);
					}
					if (notification.getEndPoint().getNotifierInfo().get(NGSIConstants.MQTT_VERSION) != null) {
						tempArray = new JsonArray();
						tempObj = new JsonObject();
						tempObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(
								notification.getEndPoint().getNotifierInfo().get(NGSIConstants.MQTT_VERSION)));
						tempArray.add(tempObj);
						notifierEndPoint.add(NGSIConstants.NGSI_LD_MQTT_VERSION, tempArray);
					}

					notifierEndPointArray.add(notifierEndPoint);
					endPoint.add(NGSIConstants.NGSI_LD_NOTIFIERINFO, notifierEndPointArray);
					endPointArray.add(endPoint);
					notificationObj.add(NGSIConstants.NGSI_LD_ENDPOINT, endPointArray);
				} else {
					endPointArray.add(endPoint);
					notificationObj.add(NGSIConstants.NGSI_LD_ENDPOINT, endPointArray);
				}
			}
			if (notification.getFormat() != null) {
				tempArray = new JsonArray();
				tempObj = new JsonObject();
				tempObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(notification.getFormat().toString()));
				tempArray.add(tempObj);
				notificationObj.add(NGSIConstants.NGSI_LD_FORMAT, tempArray);
			}
			if (notification.getLastFailedNotification() != null) {
				tempArray = new JsonArray();
				tempObj = new JsonObject();
				tempObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(
						SerializationTools.formatter.format(notification.getLastFailedNotification().toInstant())));
				tempObj.add(NGSIConstants.JSON_LD_TYPE, context.serialize(NGSIConstants.NGSI_LD_DATE_TIME));
				tempArray.add(tempObj);
				notificationObj.add(NGSIConstants.NGSI_LD_LAST_FAILURE, tempArray);
			}
			if (notification.getLastNotification() != null) {
				tempArray = new JsonArray();
				tempObj = new JsonObject();
				tempObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(
						SerializationTools.formatter.format(notification.getLastNotification().toInstant())));
				tempObj.add(NGSIConstants.JSON_LD_TYPE, context.serialize(NGSIConstants.NGSI_LD_DATE_TIME));
				tempArray.add(tempObj);
				notificationObj.add(NGSIConstants.NGSI_LD_LAST_NOTIFICATION, tempArray);
			}
			if (notification.getLastSuccessfulNotification() != null) {
				tempArray = new JsonArray();
				tempObj = new JsonObject();
				tempObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(
						SerializationTools.formatter.format(notification.getLastSuccessfulNotification().toInstant())));
				tempObj.add(NGSIConstants.JSON_LD_TYPE, context.serialize(NGSIConstants.NGSI_LD_DATE_TIME));
				tempArray.add(tempObj);
				notificationObj.add(NGSIConstants.NGSI_LD_LAST_SUCCESS, tempArray);
			}
			if (notification.getTimesSent() > 0) {
				notificationObj.add(NGSIConstants.NGSI_LD_TIMES_SEND,
						SerializationTools.getValueArray(notification.getTimesSent()));
			}
			// {
			// "https://uri.etsi.org/ngsi-ld/lastSuccess": [
			// {
			// "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
			// "@value": "2020-04-04T12:03:04Z"
			// }
			// ]
			// }
			//
			// {
			// "https://uri.etsi.org/ngsi-ld/timesSent": [
			// {
			// "@value": "2020-04-04T12:03:04Z"
			// }
			// ]
			// }
			temp.add(notificationObj);
			top.add(NGSIConstants.NGSI_LD_NOTIFICATION, temp);
		}
		if (src.getLdQuery() != null) {
			tempArray = new JsonArray();
			tempObj = new JsonObject();
			tempObj.add(NGSIConstants.JSON_LD_VALUE, context.serialize(src.getLdQuery()));
			tempArray.add(tempObj);
			top.add(NGSIConstants.NGSI_LD_QUERY, tempArray);
		}

		attribs = new JsonArray();
		if (src.getAttributeNames() != null) {
			for (String attrib : src.getAttributeNames()) {
				tempObj = new JsonObject();
				tempObj.add(NGSIConstants.JSON_LD_ID, context.serialize(attrib));
				attribs.add(tempObj);
			}
		}
		if (attribs.size() > 0) {
			top.add(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES, attribs);
		}
		if (src.getThrottling() != null && src.getTimeInterval() != 0) {
			top.add(NGSIConstants.NGSI_LD_THROTTLING, SerializationTools.getValueArray(src.getThrottling()));
		}
		if (src.getTimeInterval() != null && src.getTimeInterval() != 0) {
			top.add(NGSIConstants.NGSI_LD_TIME_INTERVAL, SerializationTools.getValueArray(src.getTimeInterval()));
		}
		if (src.getExpires() != null) {
			top.add(NGSIConstants.NGSI_LD_EXPIRES, SerializationTools
					.getValueArray(SerializationTools.formatter.format(Instant.ofEpochMilli(src.getExpires()))));
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