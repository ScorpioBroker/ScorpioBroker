package eu.neclab.ngsildbroker.commons.serialization;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import eu.neclab.ngsildbroker.commons.datatypes.BatchResult;

public class BatchResultGsonAdapter implements JsonSerializer<BatchResult> {

	@Override
	public JsonElement serialize(BatchResult src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject top = new JsonObject();
		top.add("success", context.serialize(src.getSuccess()));
		top.add("errors", context.serialize(src.getFails()));
		return top;
	}

}
