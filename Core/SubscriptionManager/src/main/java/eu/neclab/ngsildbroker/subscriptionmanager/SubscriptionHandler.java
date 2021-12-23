package eu.neclab.ngsildbroker.subscriptionmanager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.reactive.function.client.WebClient;

import eu.neclab.ngsildbroker.commons.securityConfig.WebSecurityConfiguration;

@SpringBootApplication
@Import(WebSecurityConfiguration.class)
public class SubscriptionHandler {


	public static void main(String[] args) {
		SpringApplication.run(SubscriptionHandler.class, args);

	}
	
	@Bean("subwebclient")
	WebClient webClient() {
		return WebClient.builder().build();
	}

}
