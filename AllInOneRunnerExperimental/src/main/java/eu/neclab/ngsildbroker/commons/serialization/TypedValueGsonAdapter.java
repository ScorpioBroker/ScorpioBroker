package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.TypedValue;

public class TypedValueGsonAdapter implements JsonSerializer<TypedValue> {

	
	@Override
	public JsonElement serialize(TypedValue src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject top = new JsonObject();
		top.add(NGSIConstants.JSON_LD_TYPE, new JsonPrimitive(src.getType()));
		top.add(NGSIConstants.JSON_LD_VALUE, context.serialize(src.getValue()));
		return top;
	}

}
