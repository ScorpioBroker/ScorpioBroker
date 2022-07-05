package eu.neclab.ngsildbroker.commons.serialization;

import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ArrayListMultimap;

public class MultiMapSerializer extends JsonSerializer<ArrayListMultimap<String, String>> {

	private static final String[] DUMMY_STRING_ARRAY = new String[0];

	@Override
	public void serialize(ArrayListMultimap<String, String> value, JsonGenerator gen, SerializerProvider serializers)
			throws IOException {
		gen.writeStartObject();
		for (Entry<String, Collection<String>> entry : value.asMap().entrySet()) {
			gen.writeFieldName(entry.getKey());
			gen.writeArray(entry.getValue().toArray(DUMMY_STRING_ARRAY), 0, entry.getValue().size());
		}
		gen.writeEndObject();

	}

}
