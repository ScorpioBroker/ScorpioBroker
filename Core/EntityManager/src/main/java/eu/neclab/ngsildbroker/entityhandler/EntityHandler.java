package eu.neclab.ngsildbroker.entityhandler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;

import eu.neclab.ngsildbroker.commons.ldcontext.AtContextProducerChannel;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.securityConfig.ResourceConfigDetails;
import eu.neclab.ngsildbroker.commons.securityConfig.SecurityConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.entityhandler.config.EntityConsumerChannel;
import eu.neclab.ngsildbroker.entityhandler.config.EntityProducerChannel;
import eu.neclab.ngsildbroker.entityhandler.config.EntityTopicMap; 

//@Component(immediate=true)
@SpringBootApplication
@EnableBinding({ EntityConsumerChannel.class, EntityProducerChannel.class, AtContextProducerChannel.class }) // enable channel binding with topics
public class EntityHandler {
	public static void main(String[] args) {
		SpringApplication.run(EntityHandler.class, args);
	}
	
	

	@Bean
	KafkaOps ops() {
		return new KafkaOps();
	}
	@Bean
	ContextResolverBasic conRes() {
		return new ContextResolverBasic();
	}
	
	
	
	@Bean
	SecurityConfig securityConfig() {
		return new SecurityConfig();
	}
		
	@Bean
	ResourceConfigDetails resourceConfigDetails() {
		return new ResourceConfigDetails();
	}
	
	@Bean
	ParamsResolver paramsResolver() {
		return new ParamsResolver();
	}
	
	@Bean
	QueryParser queryParser() {
		return new QueryParser();
	}
	
	@Bean
	EntityTopicMap entityTopicMap() {
		return new EntityTopicMap();
	}

	
	
}
