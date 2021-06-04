package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;

public class SubscriptionRequestGsonAdapter
		implements JsonDeserializer<SubscriptionRequest>, JsonSerializer<SubscriptionRequest> {

	@Override
	public JsonElement serialize(SubscriptionRequest src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject result = new JsonObject();
		result.add("subscription", context.serialize(src.getSubscription()));
		result.add("context", context.serialize(src.getContext()));
		result.add("headers", serializeHeaders(src.getHeaders(), context));
		return result;
	}

	@Override
	public SubscriptionRequest deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		JsonObject top = json.getAsJsonObject();
		Subscription subscription = null;
		List<Object> subContext = null;
		ArrayListMultimap<String, String> headers = null;
		for (Entry<String, JsonElement> entry : top.entrySet()) {
			String key = entry.getKey();
			switch (key) {
			case "subscription":
				subscription = context.deserialize(entry.getValue(), Subscription.class);
				break;
			case "context":
				subContext = context.deserialize(entry.getValue(), List.class);
				break;
			case "headers":
				headers = deserializeHeaders(entry.getValue());
				break;

			default:
				break;
			}
		}
		SubscriptionRequest result = new SubscriptionRequest(subscription, subContext, headers);
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
