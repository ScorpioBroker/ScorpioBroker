package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
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
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class NotificationGsonAdapter implements JsonSerializer<Notification>, JsonDeserializer<Notification>{
	
	@Override
	public JsonElement serialize(Notification src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject top = new JsonObject();
		JsonArray temp = new JsonArray();
		top.add(NGSIConstants.JSON_LD_ID, new JsonPrimitive(src.getId().toString()));
		temp.add(src.getType());
		top.add(NGSIConstants.JSON_LD_TYPE, temp);
		temp = new JsonArray();
		JsonObject tempObj = new JsonObject();
		tempObj.add(NGSIConstants.JSON_LD_ID, new JsonPrimitive(src.getSubscriptionId().toString()));
		temp.add(tempObj);
		top.add(NGSIConstants.NGSI_LD_SUBSCRIPTION_ID, temp);
		
		top.add(NGSIConstants.NGSI_LD_NOTIFIED_AT, SerializationTools.getJson(src.getNotifiedAt(), context));
		
		top.add(NGSIConstants.NGSI_LD_DATA, context.serialize(src.getData(),SerializationTypes.entitiesType));
		return top;
	}

	@Override
	public Notification deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		JsonObject top = json.getAsJsonObject();
		URI id = null;
		Long notifiedAt = null;
		URI subscriptionId = null;
		List<Entity> data = null;
		for(Entry<String, JsonElement> entry: top.entrySet()) {
			String key = entry.getKey();
			JsonElement value = entry.getValue();
			if(NGSIConstants.JSON_LD_ID.equals(key)) {
				try {
					id = new URI(value.getAsString());
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else if(NGSIConstants.NGSI_LD_SUBSCRIPTION_ID.equals(key)) {
				try {
					subscriptionId = new URI(value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_ID).getAsString());
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else if(NGSIConstants.NGSI_LD_NOTIFIED_AT.equals(key)) {
				try {
					notifiedAt = SerializationTools.date2Long(value.getAsJsonArray().get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else if(NGSIConstants.NGSI_LD_DATA.equals(key)) {
				data = context.deserialize(value, SerializationTypes.entitiesType);
			} 
		}
		if(id == null || data == null || notifiedAt == null || subscriptionId == null) {
			throw new JsonParseException("Missing field in notification");
		}
		return new Notification(id, notifiedAt, subscriptionId, data);
	}

}
