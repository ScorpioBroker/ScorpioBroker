package eu.neclab.ngsildbroker.runner;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.nativex.hint.NativeHint;
import org.springframework.nativex.hint.ResourceHint;
import org.springframework.web.client.RestTemplate;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.securityConfig.SecurityConfiguration;
import eu.neclab.ngsildbroker.commons.storage.StorageWriterDAO;

@SpringBootApplication
@NativeHint(options = { "--enable-all-security-services" }, resources = {
		@ResourceHint(patterns = "org/flywaydb/core/internal/version.txt") })
@Import(SecurityConfiguration.class)
public class Runner {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Runner.class, args);
	}

	@Bean("emdao")
	StorageWriterDAO storageWriterDAO() {
		return new StorageWriterDAO();
	}


	@Bean("hhdao")
	StorageWriterDAO storagewriterDAO() {
		return new StorageWriterDAO();
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

	@Bean
	CSourceRegistration getCsourceRegistration() {
		return new CSourceRegistration();
	}
}
