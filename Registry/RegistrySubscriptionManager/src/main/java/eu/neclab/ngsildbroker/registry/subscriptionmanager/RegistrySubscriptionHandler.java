package eu.neclab.ngsildbroker.registry.subscriptionmanager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.reactive.function.client.WebClient;

import eu.neclab.ngsildbroker.commons.securityConfig.WebSecurityConfiguration;

@SpringBootApplication
@Import(WebSecurityConfiguration.class)
public class RegistrySubscriptionHandler {

	public static void main(String[] args) {
		SpringApplication.run(RegistrySubscriptionHandler.class, args);

	}

	@Bean("subwebclient")
	WebClient webClient() {
		return WebClient.builder().build();
	}

}
