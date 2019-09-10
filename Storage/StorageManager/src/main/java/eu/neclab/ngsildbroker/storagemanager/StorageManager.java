package eu.neclab.ngsildbroker.storagemanager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.stream.service.KafkaConfig;
import eu.neclab.ngsildbroker.storagemanager.config.JdbcConfig;
import eu.neclab.ngsildbroker.storagemanager.services.StorageWriterService;
import eu.neclab.ngsildbroker.storagemanager.services.StorageReaderService;

@SpringBootApplication
@Import(KafkaConfig.class)
public class StorageManager {

	@Autowired
	JdbcConfig jdbcConfig;

	@Autowired(required=false)
	StorageWriterService storageWriterService;

	@Autowired(required=false)
	StorageReaderService storageReaderService;	
	
	public static void main(String[] args) {
		SpringApplication.run(StorageManager.class,args);
	}
	
}
