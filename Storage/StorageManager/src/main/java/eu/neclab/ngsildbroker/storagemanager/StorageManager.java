package eu.neclab.ngsildbroker.storagemanager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.storage.StorageWriterDAO;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaConfig;
import eu.neclab.ngsildbroker.storagemanager.config.JdbcConfig;
import eu.neclab.ngsildbroker.storagemanager.services.StorageReaderService;
import eu.neclab.ngsildbroker.storagemanager.services.StorageWriterService;

@SpringBootApplication
@Import(KafkaConfig.class)
public class StorageManager {

	@Autowired
	JdbcConfig jdbcConfig;
	
	@Bean("storagewriterdao")	
	StorageWriterDAO storagewriterDAO() {
		return new StorageWriterDAO();
	}

	@Autowired(required=false)
	StorageWriterService storageWriterService;

	@Autowired(required=false)
	StorageReaderService storageReaderService;	
	
	public static void main(String[] args) {
		SpringApplication.run(StorageManager.class,args);
	}
	
}
