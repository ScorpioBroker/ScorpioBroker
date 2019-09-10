package eu.neclab.ngsildbroker.testManager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import eu.neclab.ngsildbroker.commons.securityConfig.ResourceConfigDetails;
import eu.neclab.ngsildbroker.commons.securityConfig.SecurityConfig;




@SpringBootApplication
public class TestMgrHandler {// implements QueryHandlerInterface{

	public static void main(String[] args) {
		SpringApplication.run(TestMgrHandler.class, args);
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
