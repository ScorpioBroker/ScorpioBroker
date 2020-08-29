package eu.neclab.ngsildbroker.historymanager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

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
import eu.neclab.ngsildbroker.historymanager.config.ProducerChannel;


@SpringBootApplication
@EnableBinding({ AtContextProducerChannel.class,ProducerChannel.class })
@Import({CommonKafkaConfig.class, SwaggerConfigDetails.class})
public class HistoryHandler {
	public static void main(String[] args) {
		SpringApplication.run(HistoryHandler.class, args);
	}
	
	@Bean
	ContextResolverBasic conRes() {
		return new ContextResolverBasic();
	}
	
	@Bean
	KafkaOps ops() {
		return new KafkaOps();
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
	QueryParser queryParser() {
		return new QueryParser();
	}
	@Bean
	ParamsResolver paramsResolver() {
		return new ParamsResolver();
	}
	
}
