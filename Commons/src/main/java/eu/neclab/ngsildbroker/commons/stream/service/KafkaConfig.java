package eu.neclab.ngsildbroker.commons.stream.service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

@Configuration
public class KafkaConfig extends CommonKafkaConfig{

	
	@Value("${query.result.topic}")
	String queryResultTopic;
	

	
	@Bean
	public KafkaTemplate<String, byte[]> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public ReplyingKafkaTemplate<String, byte[], byte[]> replyKafkaTemplate(ProducerFactory<String, byte[]> pf,
			KafkaMessageListenerContainer<String, byte[]> container) {
		return new ReplyingKafkaTemplate<>(pf, container);

	}

	@Bean
	public KafkaMessageListenerContainer<String, byte[]> replyContainer(ConsumerFactory<String, byte[]> cf) {
		ContainerProperties containerProperties = new ContainerProperties(queryResultTopic);
		return new KafkaMessageListenerContainer<>(cf, containerProperties);
	}

	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, byte[]>> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setReplyTemplate(kafkaTemplate());
		return factory;
	}

	// region Consumer config/factory and kafkalistener factory to use manual offset
	// commit (acknowledge method)

	/*
	 * https://stackoverflow.com/questions/47427948/how-to-acknowledge-current-
	 * offset-in-spring-kafka-for-manual-commit
	 * https://github.com/contactsunny/spring-kafka-test
	 */

	public Map<String, Object> consumerConfigsManualOffsetCommit() {
		Map<String, Object> props = this.consumerConfigs();
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		return props;
	}

	public ConsumerFactory<String, byte[]> consumerFactoryManualOffsetCommit() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigsManualOffsetCommit(), new StringDeserializer(),
				new ByteArrayDeserializer());
	}

	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, byte[]>> kafkaListenerContainerFactoryManualOffsetCommit() {
		ConcurrentKafkaListenerContainerFactory<String, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactoryManualOffsetCommit());
		factory.setReplyTemplate(kafkaTemplate());
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		factory.getContainerProperties().setSyncCommits(true);

		return factory;
	}

	// endregion

}
