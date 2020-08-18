package eu.neclab.ngsildbroker.entityhandler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import eu.neclab.ngsildbroker.commons.ldcontext.AtContextProducerChannel;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.securityConfig.ResourceConfigDetails;
import eu.neclab.ngsildbroker.commons.securityConfig.SecurityConfig;
import eu.neclab.ngsildbroker.commons.stream.service.CommonKafkaConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.swaggerConfig.SwaggerConfigDetails;
import eu.neclab.ngsildbroker.entityhandler.config.EntityProducerChannel;
import eu.neclab.ngsildbroker.entityhandler.config.EntityTopicMap;


//@Component(immediate=true)
@SpringBootApplication
@EnableBinding({ EntityProducerChannel.class, AtContextProducerChannel.class }) // enable channel binding with topics
@Import({CommonKafkaConfig.class, SwaggerConfigDetails.class})
public class EntityHandler {
	public static void main(String[] args) {
		SpringApplication.run(EntityHandler.class, args);
	}
	
	

	@Bean("emops")
	@Primary
	KafkaOps ops() {
		return new KafkaOps();
	}
	@Bean("emconRes")
	@Primary
	ContextResolverBasic conRes() {
		return new ContextResolverBasic();
	}
	
	
	
	@Bean("emsec")
	SecurityConfig securityConfig() {
		return new SecurityConfig();
	}
		
	@Bean("emresconfdet")
	ResourceConfigDetails resourceConfigDetails() {
		return new ResourceConfigDetails();
	}
	
	@Bean("emparamsres")
	@Primary
	ParamsResolver paramsResolver() {
		return new ParamsResolver();
	}
	
	@Bean("emqueryparser")
	@Primary
	QueryParser queryParser() {
		return new QueryParser();
	}
	
	@Bean("emtopicmap")
	@Primary
	EntityTopicMap entityTopicMap() {
		return new EntityTopicMap();
	}

	
	
}
