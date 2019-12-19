package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import eu.neclab.ngsildbroker.commons.datatypes.Entity;

public class EntitiesGsonAdapter implements JsonSerializer<List<Entity>>, JsonDeserializer<List<Entity>>{

	@Override
	public List<Entity> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		JsonArray top = json.getAsJsonArray();
		ArrayList<Entity> result = new ArrayList<Entity>(top.size());
		for(JsonElement element: top) {
			result.add(context.deserialize(element, SerializationTypes.entityType));
		}
		return result;
	}

	@Override
	public JsonElement serialize(List<Entity> src, Type typeOfSrc, JsonSerializationContext context) {
		JsonArray top = new JsonArray();
		for(Entity entity: src) {
			top.add(context.serialize(entity));
		}
		return top;
	}

}
