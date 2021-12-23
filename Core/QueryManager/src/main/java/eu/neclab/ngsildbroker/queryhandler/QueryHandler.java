package eu.neclab.ngsildbroker.queryhandler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.web.client.RestTemplate;

import eu.neclab.ngsildbroker.commons.securityConfig.WebSecurityConfiguration;

@SpringBootApplication
@Import(WebSecurityConfiguration.class)
public class QueryHandler {

	public static void main(String[] args) {
		SpringApplication.run(QueryHandler.class, args);
	}

	@Bean
	RestTemplate restTemp() {
		return new RestTemplate();
	}

	
	

}
