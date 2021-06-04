package eu.neclab.ngsildbroker.commons.serialization;

import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceRequest;
import java.util.Map.Entry;
import java.lang.reflect.Type;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.reflect.TypeToken;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;


public class CSourceRequestGsonAdapter implements JsonDeserializer<CSourceRequest>, JsonSerializer<CSourceRequest> {

		private static final Type arrayListType = new TypeToken<ArrayListMultimap<String, String>>() {
		}.getType();

		@Override
		public JsonElement serialize(CSourceRequest src, Type typeOfSrc, JsonSerializationContext context) {
			JsonObject root = new JsonObject();
			
			root.add(AppConstants.REQUEST_ID, new JsonPrimitive(src.getId()));
			root.add(AppConstants.REQUEST_HD, serializeHeaders(src.getHeaders(), context));
			root.add(AppConstants.REQUEST_CSOURCE, context.serialize(src.getCsourceRegistration()));
			return root;
		}

		@Override
		public CSourceRequest deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
				throws JsonParseException {

			CSourceRequest result = new CSourceRequest();
			JsonObject top = json.getAsJsonObject();
			for (Entry<String, JsonElement> entry : top.entrySet()) {
				
				if (entry.getKey().equals(AppConstants.REQUEST_ID)) {
					result.setId(entry.getValue().getAsString());
				} else if (entry.getKey().equals(AppConstants.REQUEST_HD)) {
					result.setHeaders(context.deserialize(json, arrayListType));
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


