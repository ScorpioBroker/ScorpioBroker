package eu.neclab.ngsildbroker.historymanager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.securityConfig.WebSecurityConfiguration;
import eu.neclab.ngsildbroker.commons.storage.StorageWriterDAO;
import eu.neclab.ngsildbroker.historymanager.config.HistoryJdbcConfig;

@SpringBootApplication
@Import(WebSecurityConfiguration.class)
public class HistoryHandler {
	public static void main(String[] args) {
		SpringApplication.run(HistoryHandler.class, args);
	}

	@Autowired
	HistoryJdbcConfig jdbcConfig;

	@Bean("hhdao")
	StorageWriterDAO storagewriterDAO() {
		return new StorageWriterDAO();
	}


}
