package eu.neclab.ngsildbroker.runner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.messagebus.InternalKafkaReplacement;
import eu.neclab.ngsildbroker.commons.securityConfig.WebSecurityConfiguration;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;

@SpringBootApplication
@ComponentScan(basePackages = { "eu.neclab.ngsildbroker.*" }, excludeFilters = {
		@Filter(type = FilterType.REGEX, pattern = { "eu.neclab.ngsildbroker.commons.*" }),
		@Filter(type = FilterType.ANNOTATION, value = SpringBootApplication.class) })
@Import({ MicroServiceUtils.class, WebSecurityConfiguration.class })

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
