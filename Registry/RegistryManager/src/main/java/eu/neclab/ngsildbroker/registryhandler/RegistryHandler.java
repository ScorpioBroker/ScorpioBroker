package eu.neclab.ngsildbroker.registryhandler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.securityConfig.WebSecurityConfiguration;

@SpringBootApplication
@Import(WebSecurityConfiguration.class)
public class RegistryHandler {

	public static void main(String[] args) {
		SpringApplication.run(RegistryHandler.class, args);
	}

}
