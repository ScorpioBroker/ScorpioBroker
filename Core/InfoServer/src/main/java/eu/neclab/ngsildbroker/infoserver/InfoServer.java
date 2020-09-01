package eu.neclab.ngsildbroker.infoserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.securityConfig.ResourceConfigDetails;
import eu.neclab.ngsildbroker.commons.securityConfig.SecurityConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaConfig;
import eu.neclab.ngsildbroker.commons.swaggerConfig.SwaggerConfigDetails;


@SpringBootApplication
@Import({KafkaConfig.class, SwaggerConfigDetails.class})
public class InfoServer {// implements QueryHandlerInterface{

	public static void main(String[] args) {
		SpringApplication.run(InfoServer.class, args);
	}

	
	@Bean
	SecurityConfig securityConfig() {
		return new SecurityConfig();
	}
		
	@Bean
	ResourceConfigDetails resourceConfigDetails() {
		return new ResourceConfigDetails();
	}
}
