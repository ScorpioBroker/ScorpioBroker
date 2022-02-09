package eu.neclab.ngsildbroker.runner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.nativex.hint.ResourceHint;
import org.springframework.nativex.hint.ResourcesHints;

import eu.neclab.ngsildbroker.commons.messagebus.InternalKafkaReplacement;

@SpringBootApplication
@ComponentScan(basePackages = { "eu.neclab.ngsildbroker.*" }, excludeFilters = {
		@Filter(type = FilterType.ANNOTATION, value = SpringBootApplication.class) })

@ResourcesHints({ @ResourceHint(patterns = "org/flywaydb/core/internal/version.txt") })
public class Runner {

	public static void main(String[] args) throws Exception {
		System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
		SpringApplication.run(Runner.class, args);
	}

	@Bean
	@ConditionalOnProperty(prefix = "scorpio.kafka", matchIfMissing = false, name = "enabled", havingValue = "false")
	InternalKafkaReplacement internalKafkaReplacement() {
		return new InternalKafkaReplacement();
	}
}
