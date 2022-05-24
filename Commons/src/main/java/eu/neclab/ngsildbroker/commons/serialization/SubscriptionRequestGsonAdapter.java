package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

public class SubscriptionRequestGsonAdapter
		implements JsonDeserializer<SubscriptionRequest>, JsonSerializer<SubscriptionRequest> {

	private static final String SUBSCRIPTION = "subscription";
	private static final String CONTEXT = "context";
	private static final String ACTIVE = "active";
	private static final String TYPE = "type";
	private static final String HEADERS = "headers";
	private static final String ID = "id";
	private static final String REQUEST_TYPE = "requestType";
	Type SUB_TYPE = new TypeToken<Subscription>() {
	}.getType();
	Type CONTEXT_TYPE = new TypeToken<List<Object>>() {
	}.getType();

	@Override
	public JsonElement serialize(SubscriptionRequest src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject top = new JsonObject();
		top.add(SUBSCRIPTION, context.serialize(src.getSubscription()));
		top.add(CONTEXT, context.serialize(src.getContext()));
		top.add(ACTIVE, context.serialize(src.isActive()));
		top.add(TYPE, context.serialize(src.getType()));
		top.add(HEADERS, getHeaders(src.getHeaders()));
		top.add(ID, context.serialize(src.getId()));
		top.add(REQUEST_TYPE, context.serialize(src.getRequestType()));
		return top;
	}

	private JsonElement getHeaders(ArrayListMultimap<String, String> headers) {
		JsonObject top = new JsonObject();
		for (Entry<String, Collection<String>> entry : headers.asMap().entrySet()) {
			String key = entry.getKey();
			JsonArray value = new JsonArray();
			for (String element : entry.getValue()) {
				value.add(element);
			}
			top.add(key, value);
		}
		return top;
	}

	@Override
	public SubscriptionRequest deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		SubscriptionRequest result = new SubscriptionRequest();
		JsonObject top = json.getAsJsonObject();
		for (Entry<String, JsonElement> entry : top.entrySet()) {
			String key = entry.getKey();
			JsonElement value = entry.getValue();
			switch (key) {
				case SUBSCRIPTION:
					result.setSubscription(context.deserialize(value, SUB_TYPE));
					break;
				case CONTEXT:
					result.setContext(context.deserialize(value, CONTEXT_TYPE));
					break;
				case ACTIVE:
					result.setActive(value.getAsBoolean());
					break;
				case TYPE:
					result.setType(value.getAsInt());
					break;
				case HEADERS:
					result.setHeaders(getMultiListHeaders(value));
					break;
				case ID:
					result.setId(value.getAsString());
					break;
				case REQUEST_TYPE:
					result.setRequestType(value.getAsInt());
					break;
				default:
					break;
			}
		}
		return result;
	}

	private ArrayListMultimap<String, String> getMultiListHeaders(JsonElement value) {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		JsonObject top = value.getAsJsonObject();
		for (Entry<String, JsonElement> entry : top.entrySet()) {
			Iterator<JsonElement> it = entry.getValue().getAsJsonArray().iterator();
			while (it.hasNext()) {
				result.put(entry.getKey(), it.next().getAsString());
			}
		}
		return result;
	}

}
