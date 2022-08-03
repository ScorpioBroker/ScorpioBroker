package eu.neclab.ngsildbroker.commons.serialization.messaging;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;

public class ArraylistMultimapSerializer extends ObjectMapperSerializer<ArrayListMultimap> {
	static final Logger logger = LoggerFactory.getLogger(ArraylistMultimapSerializer.class);

	@Override
	public byte[] serialize(String topic, ArrayListMultimap data) {
		try {
			return JsonUtils.toString(data.asMap()).getBytes();
		} catch (Exception e) {
			logger.error("failed to serialize arraylist multimap", e);
			return new byte[0];
		}
	}

}
