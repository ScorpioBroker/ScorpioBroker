package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.util.Map.Entry;

import com.google.common.collect.ArrayListMultimap;
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
import eu.neclab.ngsildbroker.commons.datatypes.EntityRequest;

public class EntityRequestGsonAdapter implements JsonDeserializer<EntityRequest>, JsonSerializer<EntityRequest> {

	@Override
	public JsonElement serialize(EntityRequest src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject root = new JsonObject();
		root.add(AppConstants.REQUEST_KV, new JsonPrimitive(src.getKeyValue()));
		root.add(AppConstants.REQUEST_WA, new JsonPrimitive(src.getWithSysAttrs()));
		root.add(AppConstants.REQUEST_WOA, new JsonPrimitive(src.getEntityWithoutSysAttrs()));
		root.add(AppConstants.REQUEST_T, new JsonPrimitive(src.getOperationType()));
		root.add(AppConstants.REQUEST_ID, new JsonPrimitive(src.getId()));
		if (src.getOperationValue() != null) {
			root.add(AppConstants.REQUEST_OV, new JsonPrimitive(src.getOperationValue()));
		}
		root.add(AppConstants.REQUEST_HD, serializeHeaders(src.getHeaders(), context));
		return root;
	}

	@Override
	public EntityRequest deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {

		EntityRequest result = new EntityRequest();
		JsonObject top = json.getAsJsonObject();
		for (Entry<String, JsonElement> entry : top.entrySet()) {
			if (entry.getKey().equals(AppConstants.REQUEST_KV)) {
				result.setKeyValue(entry.getValue().getAsString());
			} else if (entry.getKey().equals(AppConstants.REQUEST_WA)) {
				result.setWithSysAttrs(entry.getValue().getAsString());
			} else if (entry.getKey().equals(AppConstants.REQUEST_WOA)) {
				result.setEntityWithoutSysAttrs(entry.getValue().getAsString());
			} else if (entry.getKey().equals(AppConstants.REQUEST_T)) {
				result.setOperationType(entry.getValue().getAsInt());
			} else if (entry.getKey().equals(AppConstants.REQUEST_ID)) {
				result.setId(entry.getValue().getAsString());
			} else if (entry.getKey().equals(AppConstants.REQUEST_OV)) {
				result.setOperationValue(entry.getValue().getAsString());
			} else if (entry.getKey().equals(AppConstants.REQUEST_HD)) {
				result.setHeaders(deserializeHeaders(entry.getValue()));
			}
		}
		return result;
	}

	private JsonElement serializeHeaders(ArrayListMultimap<String, String> headers, JsonSerializationContext context) {
		JsonObject result = new JsonObject();
		for (String key : headers.keySet()) {
			result.add(key, context.serialize(headers.get(key)));
		}
		return result;
	}

	private ArrayListMultimap<String, String> deserializeHeaders(JsonElement json) {
		JsonObject root = json.getAsJsonObject();
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		for (Entry<String, JsonElement> entry : root.entrySet()) {
			JsonArray array = entry.getValue().getAsJsonArray();
			for (JsonElement item : array) {
				result.put(entry.getKey(), item.getAsString());
			}
		}
		return result;
	}

}
