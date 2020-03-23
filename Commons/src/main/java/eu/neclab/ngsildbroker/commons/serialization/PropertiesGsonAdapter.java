package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class PropertiesGsonAdapter implements JsonDeserializer<List<Property>>, JsonSerializer<List<Property>>{

	
	@Override
	public JsonElement serialize(List<Property> src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject top = new JsonObject();
		for(Property property: src) {
			top.add(property.getId().toString(), SerializationTools.getJson(property, context));
		}
		return top;
	}

	@Override
	public List<Property> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		ArrayList<Property> result = new ArrayList<Property>();
		JsonObject top = json.getAsJsonObject();
		Set<Entry<String, JsonElement>> jsonProperties = top.entrySet();
		for(Entry<String, JsonElement> entry: jsonProperties) {
			result.add(SerializationTools.parseProperty(entry.getValue().getAsJsonArray(), entry.getKey()));
		}
		return result;
	}

}
