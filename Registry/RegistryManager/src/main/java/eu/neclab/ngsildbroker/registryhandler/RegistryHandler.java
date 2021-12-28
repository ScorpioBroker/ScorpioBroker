package eu.neclab.ngsildbroker.registryhandler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.securityConfig.WebSecurityConfiguration;
import eu.neclab.ngsildbroker.registryhandler.config.RegistryJdbcConfig;

@SpringBootApplication
@Import(WebSecurityConfiguration.class)
public class RegistryHandler {

	public static void main(String[] args) {
		SpringApplication.run(RegistryHandler.class, args);
	}

	@Autowired
	RegistryJdbcConfig jdbcConfig;
	



}
