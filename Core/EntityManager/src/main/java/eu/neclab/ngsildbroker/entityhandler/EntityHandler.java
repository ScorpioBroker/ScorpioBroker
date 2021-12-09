package eu.neclab.ngsildbroker.entityhandler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import eu.neclab.ngsildbroker.commons.ldcontext.AtContextProducerChannel;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;

import eu.neclab.ngsildbroker.commons.storage.StorageWriterDAO;
import eu.neclab.ngsildbroker.commons.stream.service.CommonKafkaConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.entityhandler.config.EntityJdbcConfig;
import eu.neclab.ngsildbroker.entityhandler.config.EntityProducerChannel;
import eu.neclab.ngsildbroker.entityhandler.config.EntityTopicMap;




//@Component(immediate=true)
@SpringBootApplication
@EnableBinding({ EntityProducerChannel.class, AtContextProducerChannel.class }) // enable channel binding with topics
@Import({CommonKafkaConfig.class})
public class EntityHandler {
	public static void main(String[] args) {
		SpringApplication.run(EntityHandler.class, args);
	}
	
	@Autowired
	EntityJdbcConfig jdbcConfig;

	@Bean("emops")
	@Primary
	KafkaOps ops() {
		return new KafkaOps();
	}
		
	@Bean("emparamsres")
	@Primary
	ParamsResolver paramsResolver() {
		return new ParamsResolver();
	}
	

	
	@Bean("emtopicmap")
	@Primary
	EntityTopicMap entityTopicMap() {
		return new EntityTopicMap();
	}
	
	@Bean("emstorage")
	@Primary
	StorageWriterDAO storageWriterDAO() {
		return new StorageWriterDAO();
	}

//	@Bean(name = "tenantAwareDataSource")
//	@Primary
//	public DataSource tenantAwareDataSource() {
//		return new TenantAwareDataSource();
//	}
	
}
