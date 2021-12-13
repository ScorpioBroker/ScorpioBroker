package eu.neclab.ngsildbroker.subscriptionmanager;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;

import eu.neclab.ngsildbroker.commons.securityConfig.SecurityConfiguration;

@SpringBootApplication
@Import(SecurityConfiguration.class)
public class SubscriptionHandler {

	@Value("${atcontext.url}")
	String atContextServerUrl;

	public static void main(String[] args) {
		SpringApplication.run(SubscriptionHandler.class, args);

	}

	@Value("${query.result.topic}")
	String queryResultTopic;

	@Bean // register and configure replying kafka template
	public ReplyingKafkaTemplate<String, String, String> replyingTemplate(ProducerFactory<String, String> pf,
			ConcurrentKafkaListenerContainerFactory<String, String> containerFactory) {
		return new ReplyingKafkaTemplate<>(pf, containerFactory.createContainer(queryResultTopic));
	}
}
