package eu.neclab.ngsildbroker.queryhandler;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class QueryHandler {

	public static void main(String[] args) {
		SpringApplication.run(QueryHandler.class, args);
	}

	@Bean
	RestTemplate restTemp() {
		return new RestTemplate();
	}

	@Value("${query.result.topic}")
	String queryResultTopic;

	@Bean // register and configure replying kafka template
	public ReplyingKafkaTemplate<String, String, String> replyingTemplate(ProducerFactory<String, String> pf,
			ConcurrentKafkaListenerContainerFactory<String, String> containerFactory) {
		return new ReplyingKafkaTemplate<>(pf, containerFactory.createContainer(queryResultTopic));
	}

}
