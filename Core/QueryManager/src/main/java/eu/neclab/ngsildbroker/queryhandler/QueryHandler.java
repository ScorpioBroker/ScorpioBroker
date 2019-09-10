package eu.neclab.ngsildbroker.queryhandler;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestTemplate;

import eu.neclab.ngsildbroker.commons.ldcontext.AtContextProducerChannel;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.securityConfig.ResourceConfigDetails;
import eu.neclab.ngsildbroker.commons.securityConfig.SecurityConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.queryhandler.config.QueryConsumerChannel;
import eu.neclab.ngsildbroker.queryhandler.config.QueryProducerChannel;


@SpringBootApplication
@Import(KafkaConfig.class)
@EnableBinding({ QueryConsumerChannel.class, AtContextProducerChannel.class, QueryProducerChannel.class})
public class QueryHandler {// implements QueryHandlerInterface{

	@Value("${atcontext.url}")
	String atContextServerUrl;
	
	public static void main(String[] args) {
		SpringApplication.run(QueryHandler.class, args);
	}

	@Bean
	KafkaOps ops() {
		return new KafkaOps();
	}
	@Bean
	ContextResolverBasic conRes() {
		return new ContextResolverBasic(atContextServerUrl);
	}
	
	
	@Bean
	RestTemplate restTemp() {
		return new RestTemplate();
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
