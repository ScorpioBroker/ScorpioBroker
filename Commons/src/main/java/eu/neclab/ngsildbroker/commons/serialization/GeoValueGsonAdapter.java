package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoValue;

public class GeoValueGsonAdapter implements JsonDeserializer<GeoValue>, JsonSerializer<GeoValue> {

	@Override
	public JsonElement serialize(GeoValue src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject top = new JsonObject();
		top.add(NGSIConstants.GEO_JSON_TYPE, new JsonPrimitive(src.getType()));	
		JsonElement coordinates = new Gson().toJsonTree(src.getCoordinates(), new TypeToken<List<Double>>() {}.getType());
		top.add(NGSIConstants.GEO_JSON_COORDINATES, coordinates);
		// must return the strigified version
		JsonPrimitive stringified = new JsonPrimitive(top.toString());
		return stringified;
	}
	
	@Override
	public GeoValue deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {		
		JsonObject top = json.getAsJsonObject();
		GeoValue value = new GeoValue();
		value.setType(top.get(NGSIConstants.GEO_JSON_TYPE).getAsString());
		JsonArray jsonCoordinates;
		switch (value.getType()) {
		case NGSIConstants.GEO_TYPE_POINT:
			jsonCoordinates = top.get(NGSIConstants.GEO_JSON_COORDINATES).getAsJsonArray();
			break;
		case NGSIConstants.GEO_TYPE_LINESTRING:
			jsonCoordinates = top.get(NGSIConstants.GEO_JSON_COORDINATES).getAsJsonArray().get(0).getAsJsonArray();
			break;
		case NGSIConstants.GEO_TYPE_POLYGON:
			jsonCoordinates = top.get(NGSIConstants.GEO_JSON_COORDINATES).getAsJsonArray().get(0).getAsJsonArray()
					.get(0).getAsJsonArray();
			break;
		default:
			throw new JsonParseException("Unexpected GeoJson type");
		}

		ArrayList<Double> coordinates = new ArrayList<Double>();
		Iterator<JsonElement> it = jsonCoordinates.iterator();
		while (it.hasNext()) {
			JsonElement element = it.next();
			if(element.isJsonArray()) {
				Iterator<JsonElement> it2 = element.getAsJsonArray().iterator();
				while(it2.hasNext()) {
					coordinates.add(it2.next().getAsDouble());
				}
			}else {
				coordinates.add(element.getAsDouble());
			}
		}
		value.setCoordinates(coordinates);
		return value;
	}

}
