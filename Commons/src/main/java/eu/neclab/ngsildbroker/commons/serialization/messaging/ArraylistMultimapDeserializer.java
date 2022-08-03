package eu.neclab.ngsildbroker.commons.serialization.messaging;

import java.util.List;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class ArraylistMultimapDeserializer extends ObjectMapperDeserializer<ArrayListMultimap<String, String>> {

	static final Logger logger = LoggerFactory.getLogger(ArraylistMultimapDeserializer.class);

	public ArraylistMultimapDeserializer(Class<ArrayListMultimap<String, String>> type) {
		super(type);
		// TODO Auto-generated constructor stub
	}

	@Override
	public ArrayListMultimap<String, String> deserialize(String topic, byte[] data) {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		try {
			Map<String, List<String>> mapData = (Map<String, List<String>>) JsonUtils.fromString(new String(data));
			mapData.forEach((t, u) -> {
				result.putAll(t, u);
			});
		} catch (Exception e) {
			logger.error("failed to deserialize ArrayListMultimap", e);
		}
		return result;
	}

}
