package eu.neclab.ngsildbroker.commons.serialization;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ArrayListMultimap;

public class MultiMapDeserializer extends JsonDeserializer<ArrayListMultimap<String, String>> {

	@Override
	public ArrayListMultimap<String, String> deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException, JacksonException {
		
		JsonNode root = p.readValueAsTree();//p.getCodec().readTree(p);
		Iterator<Entry<String, JsonNode>> it = root.fields();
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		while (it.hasNext()) {
			Entry<String, JsonNode> next = it.next();
			Iterator<JsonNode> it2 = ((ArrayNode) next.getValue()).elements();
			while (it2.hasNext()) {
				result.put(next.getKey(), it2.next().asText());
			}
		}
		return result;
	}

}
