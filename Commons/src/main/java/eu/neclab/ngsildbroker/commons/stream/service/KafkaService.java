package eu.neclab.ngsildbroker.commons.stream.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.stream.interfaces.KafkaConsumerChannels;
import eu.neclab.ngsildbroker.commons.stream.interfaces.KafkaProducerChannels;


@Service
public class KafkaService {
	@Autowired
	ObjectMapper objectMapper;
	@Autowired
	KafkaOps operations;

	private final KafkaProducerChannels producerChannels;
	@SuppressWarnings("unused")
	//TODO check to remove ... never used
	private final KafkaConsumerChannels consumerChannels;
	
	Duration brokerPollDurationMillis = Duration.ofMillis(200);
	Duration brokerHeartbeatPollDurationinMillis=Duration.ofMillis(0);

	public KafkaService(KafkaProducerChannels producerChannels, KafkaConsumerChannels consumerChannels) {
		this.producerChannels = producerChannels;
		this.consumerChannels = consumerChannels;
	}

	public String createMessage(String payload) {
		MessageChannel messageChannel = producerChannels.entityCreate();
		String key = UUID.randomUUID().toString();
		if(isDuplicate(key))
			return "DUPLICATE";
		boolean result = operations.pushToKafka(messageChannel, key.getBytes(), payload.getBytes());
		System.out.println(result);
		if(!result) {
			return "ERROR";
		}
		return "CREATED";
	}
	
	public String deleteTopicMessage(String key) {
		MessageChannel messageChannel = producerChannels.entityCreate();
		System.out.println("key :::: " + key);
		boolean result = messageChannel
						.send(MessageBuilder.withPayload("null")
						.setHeader(KafkaHeaders.MESSAGE_KEY, key.getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build());
		System.out.println(result);
		if(result) {
			return "DELETED";
		}
		return "ERROR";
	}

	@StreamListener(KafkaConsumerChannels.readChannel)
	public void handleEntity(@Payload String payload) {
		System.out.println("Received : {} :::: " + payload);
	}

	@StreamListener(KafkaConsumerChannels.readChannel)
	public void handleEntityKey(Message<?> message) throws Exception {
		String payload = new String((byte[]) message.getPayload());
		String key = new String((byte[]) message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY));
		System.out.println("key received ::::: " + key);
		System.out.println("Received message: {} :::: " + payload);
	}

	@SuppressWarnings("deprecation")
	//TODO replace poll method... needs subscription listener when replaced
	public boolean isDuplicate(String key) {
		boolean flag = false;
		Properties properties = new Properties();
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//seek to begining
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

		try(KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props)){
			consumer.subscribe(new ArrayList<String>(Collections.singletonList("ENTITY_CREATE")));
			consumer.poll(brokerHeartbeatPollDurationinMillis);
			// seek to begining
			consumer.seekToBeginning(consumer.assignment());
			// poll and time-out if no replies
			ConsumerRecords<byte[], byte[]> records = consumer.poll(brokerPollDurationMillis);
			for (ConsumerRecord<byte[], byte[]> record : records) {
				String key1 = new String(record.key());
				if(key1.equals(key)) {
					flag=true;
					break;
				}
			}
		}
		return flag;
	}
}
