package eu.neclab.ngsildbroker.commons.stream.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.RetryOperations;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.neclab.ngsildbroker.commons.constants.KafkaConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityDetails;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

@Component
public class KafkaOps {

	@Value("${bootstrap.servers}")
	String BOOTSTRAP_SERVERS;

	AdminClient adminClient = null;

	@Autowired
	ObjectMapper objectMapper;

	// Duration brokerPollDurationMillis = Duration.ofMillis(200);
	// Duration brokerHeartbeatPollDurationinMillis=Duration.ofMillis(0);
	long brokerPollDurationMillis = 200;
	long brokerHeartbeatPollDurationinMillis = 0;

	// private final static Logger logger = LoggerFactory.getLogger(KafkaOps.class);

	public boolean pushToKafka(MessageChannel messageChannel, byte[] key, byte[] payload) throws ResponseException {
		try {
			boolean result = messageChannel
					.send(MessageBuilder.withPayload(payload).setHeader(KafkaHeaders.MESSAGE_KEY, key)
							.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build());
			return result;
		} catch (Exception e) {
			throw new ResponseException(ErrorType.KafkaWriteError, e.getMessage());
		}
	}

	public void createTopic(String topicName) {

		KafkaProperties kafkaProperties = new KafkaProperties();
		kafkaProperties.setBootstrapServers(Collections.singletonList("localhost:9092"));
		KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties = new KafkaBinderConfigurationProperties(
				kafkaProperties);
		KafkaTopicProvisioner kafkaTopicProvisioner = new KafkaTopicProvisioner(kafkaBinderConfigurationProperties,
				kafkaProperties);
		RetryOperations metadataRetryOperations = new RetryTemplate();
		kafkaTopicProvisioner.setMetadataRetryOperations(metadataRetryOperations);
		KafkaProducerProperties kafkaProducerProperties = new KafkaProducerProperties();
		ExtendedProducerProperties<KafkaProducerProperties> extendedProducerProperties = new ExtendedProducerProperties<KafkaProducerProperties>(
				kafkaProducerProperties);
		kafkaTopicProvisioner.provisionProducerDestination(topicName, extendedProducerProperties);
	}

	public Set<String> getTopics() throws Exception {
		KafkaProperties kafkaProperties = new KafkaProperties();
		kafkaProperties.setBootstrapServers(Collections.singletonList("localhost:9092"));
		Map<String, Object> adminClientProperties = kafkaProperties.buildAdminProperties();
		try (AdminClient adminClient = AdminClient.create(adminClientProperties)) {
			ListTopicsResult listTopicsResult = adminClient.listTopics();
			KafkaFuture<Set<String>> namesFutures = listTopicsResult.names();
			Set<String> names = namesFutures.get(30, TimeUnit.SECONDS);
			return names;
		}
	}

	public void deleteTopic(Collection<String> topicName) throws Exception {
		KafkaProperties kafkaProperties = new KafkaProperties();
		kafkaProperties.setBootstrapServers(Collections.singletonList("localhost:9092"));
		Map<String, Object> adminClientProperties = kafkaProperties.buildAdminProperties();
		try (AdminClient adminClient = AdminClient.create(adminClientProperties)) {
			// TODO what's up with this result??
			DeleteTopicsResult deleteTopicResult = adminClient.deleteTopics(topicName);
		}
	}

	public boolean isMessageExists(String key, String topicname) {
		Map<String, byte[]> entityMap = pullFromKafka(topicname);
		return entityMap.containsKey(key);
	}

	public String generateUUIDKey() {
		return UUID.randomUUID().toString();
	}

	@SuppressWarnings("deprecation")
	// TODO replace poll method... needs subscription listener when replaced
	public Map<String, byte[]> pullFromKafka(String topicname) {
		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(this.getProperties());
		try {
			Map<String, byte[]> entityMap = new HashMap<String, byte[]>(2000);
			boolean stop = false;
			consumer.subscribe(new ArrayList<String>(Collections.singletonList(topicname)));
			consumer.poll(brokerHeartbeatPollDurationinMillis);
			// Reading topic offset from beginning
			consumer.seekToBeginning(consumer.assignment());
			while (!stop) {
				// Request unread messages from the topic.
				ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(brokerPollDurationMillis);
				Iterator<ConsumerRecord<byte[], byte[]>> iterator = consumerRecords.iterator();
				if (iterator.hasNext()) {
					while (iterator.hasNext()) {
						ConsumerRecord<byte[], byte[]> record = iterator.next();
						entityMap.put(new String(record.key()), record.value());
					}
				} else {
					stop = true;
				}
			}
			return entityMap;
		} finally {
			consumer.unsubscribe();
			consumer.close();
		}
	}

	@SuppressWarnings("deprecation")
	// TODO replace poll method... needs subscription listener when replaced
	public Map<String, EntityDetails> getAllEntitiesDetails() throws IOException {
		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(this.getProperties());
		try {
			Map<String, EntityDetails> entityMap = new HashMap<String, EntityDetails>(2000);
			boolean stop = false;
			consumer.subscribe(new ArrayList<String>(Collections.singletonList(KafkaConstants.ENTITY_TOPIC)));
			consumer.poll(brokerHeartbeatPollDurationinMillis);
			// Reading topic offset from beginning
			consumer.seekToBeginning(consumer.assignment());
			while (!stop) {
				// Request unread messages from the topic.
				ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(brokerPollDurationMillis);
				Iterator<ConsumerRecord<byte[], byte[]>> iterator = consumerRecords.iterator();
				if (iterator.hasNext()) {
					while (iterator.hasNext()) {
						ConsumerRecord<byte[], byte[]> record = iterator.next();
						JsonNode entityJsonBody = objectMapper.readTree(record.value());
						boolean isDeletedMsg = entityJsonBody.isNull();
						if (isDeletedMsg) {
							entityMap.remove(new String(record.key()));
						} else {
							entityMap.put(new String(record.key()),
									new EntityDetails(new String(record.key()), record.partition(), record.offset()));
						}
					}
				} else {
					stop = true;
				}
			}
			return entityMap;
		} finally {
			consumer.unsubscribe();
			consumer.close();
		}
	}

	/**
	 * Method used for read message from topic
	 * 
	 * @param key
	 * @param topicname
	 * @return byte[]
	 */
	public byte[] getMessage(String key, String topicname) {
		Map<String, byte[]> entityMap = pullFromKafka(topicname, key);
		return entityMap.get(key);
	}

	@SuppressWarnings("deprecation")
	// TODO replace poll method... needs subscription listener when replaced
	public Map<String, byte[]> pullFromKafka(String topicname, String key) {
		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(this.getProperties());
		try {
			Map<String, byte[]> entityMap = new HashMap<String, byte[]>(2000);
			boolean stop = false;
			consumer.subscribe(new ArrayList<String>(Collections.singletonList(topicname)));
			consumer.poll(brokerHeartbeatPollDurationinMillis);
			// Reading topic offset from beginning
			consumer.seekToBeginning(consumer.assignment());

			while (!stop) {
				// Request unread messages from the topic.
				ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(brokerPollDurationMillis);
				Iterator<ConsumerRecord<byte[], byte[]>> iterator = consumerRecords.iterator();
				if (iterator.hasNext()) {
					while (iterator.hasNext()) {
						ConsumerRecord<byte[], byte[]> record = iterator.next();
						if (key.equals(new String(record.key()))) {
							entityMap.put(new String(record.key()), record.value());
						}
					}
				} else {
					stop = true;
				}
			}
			return entityMap;
		} finally {
			consumer.unsubscribe();
			consumer.close();
		}
	}

	/**
	 * Method used for set properties
	 * 
	 * @return
	 */
	public Map<String, Object> getProperties() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// seek to begining
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.MAX_VALUE);
		props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, Integer.MAX_VALUE);
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		return props;
	}

	@SuppressWarnings("deprecation")
	// TODO replace poll method... needs subscription listener when replaced
	public byte[] getMessage(String topicname, String key, int partition, long offset) {
		Map<String, byte[]> entityMap = new HashMap<String, byte[]>();
		Map<String, Object> props = getProperties();
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.remove(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
		try {
			consumer.subscribe(new ArrayList<String>(Collections.singletonList(topicname)));
			consumer.poll(brokerHeartbeatPollDurationinMillis);
			consumer.seek(new TopicPartition(topicname, partition), offset);
			boolean flag = true;
			int retry = 0;
			while (flag && retry < 3) {
				ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(brokerPollDurationMillis);
				consumerRecords.forEach(record -> {
					entityMap.put(new String(record.key()), record.value());
				});
				if (entityMap.containsKey(key)) {
					break;
				} else {
					System.out.println(
							"not found in try ::" + retry + " for partition ::" + partition + " & offset ::" + offset);
					retry++;
				}
			}
			return entityMap.get(key);
		} finally {
			consumer.unsubscribe();
			consumer.close();
		}
	}

	@SuppressWarnings("deprecation")
	// TODO replace poll method... needs subscription listener when replaced
	public byte[] getMessageDetails(String topicName, String key) {
		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(this.getProperties());
		try {
			boolean stop = false;
			Map<String, byte[]> entityMap = new HashMap<String, byte[]>(2000);
			consumer.subscribe(new ArrayList<String>(Collections.singletonList(topicName)));
			consumer.poll(brokerHeartbeatPollDurationinMillis);
			// Reading topic offset from beginning
			consumer.seekToBeginning(consumer.assignment());
			while (!stop) {
				// Request unread messages from the topic.
				ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(brokerPollDurationMillis);
				Iterator<ConsumerRecord<byte[], byte[]>> iterator = consumerRecords.iterator();
				if (iterator.hasNext()) {
					while (iterator.hasNext()) {
						ConsumerRecord<byte[], byte[]> record = iterator.next();
						if (key.equals(new String(record.key()))) {
							entityMap.put(key, record.value());
						}
					}
				} else {
					stop = true;
				}
			}
			return entityMap.get(key);
		} finally {
			consumer.unsubscribe();
			consumer.close();
		}
	}

	public static String getMessageKey(Message<?> message) {
		Object key = message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY);
		return (String) key;
	}
}
