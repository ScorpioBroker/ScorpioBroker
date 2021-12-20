package eu.neclab.ngsildbroker.entityhandler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.securityConfig.WebSecurityConfiguration;
import eu.neclab.ngsildbroker.commons.storage.StorageWriterDAO;
import eu.neclab.ngsildbroker.entityhandler.config.EntityJdbcConfig;

@SpringBootApplication
@Import(WebSecurityConfiguration.class)
public class EntityHandler {
	public static void main(String[] args) {
		SpringApplication.run(EntityHandler.class, args);
	}

	@Autowired
	EntityJdbcConfig jdbcConfig;

	@Bean("emdao")
	StorageWriterDAO storageWriterDAO() {
		return new StorageWriterDAO();
	}

}
