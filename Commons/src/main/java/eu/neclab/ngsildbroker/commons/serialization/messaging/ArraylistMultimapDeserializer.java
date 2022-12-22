package eu.neclab.ngsildbroker.commons.serialization.messaging;

import com.google.common.collect.ArrayListMultimap;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class ArraylistMultimapDeserializer extends ObjectMapperDeserializer<ArrayListMultimap<String, String>> {

	private static ArrayListMultimap<String, String> tmp = ArrayListMultimap.create();
	private static Class<ArrayListMultimap<String, String>> mmClass = (Class<ArrayListMultimap<String, String>>) tmp
			.getClass();

	public ArraylistMultimapDeserializer() {
		super(mmClass);
	}

}
