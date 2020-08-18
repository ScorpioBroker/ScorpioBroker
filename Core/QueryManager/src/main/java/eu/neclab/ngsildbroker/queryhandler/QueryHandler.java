package eu.neclab.ngsildbroker.queryhandler;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.web.client.RestTemplate;

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
import eu.neclab.ngsildbroker.queryhandler.config.QueryProducerChannel;



@SpringBootApplication
@Import({KafkaConfig.class, SwaggerConfigDetails.class})
@EnableBinding({ AtContextProducerChannel.class, QueryProducerChannel.class})

public class QueryHandler {// implements QueryHandlerInterface{



	@Value("${atcontext.url}")
	String atContextServerUrl;
	
	public static void main(String[] args) {
		SpringApplication.run(QueryHandler.class, args);
	}
	
	@Bean("qmops")
	KafkaOps ops() {
		return new KafkaOps();
	}
	@Bean("qmconRes")
	ContextResolverBasic conRes() {
		return new ContextResolverBasic(atContextServerUrl);
	}
	
	
	@Bean("qmrestTemp")
	RestTemplate restTemp() {
		return new RestTemplate();
	}
	
	@Bean("qmsecurityConfig")
	SecurityConfig securityConfig() {
		return new SecurityConfig();
	}
		
	@Bean("qmresourceConfigDetails")
	ResourceConfigDetails resourceConfigDetails() {
		return new ResourceConfigDetails();
	}
	
	@Bean("qmqueryParser")
	QueryParser queryParser() {
		return new QueryParser();
	}
	@Bean("qmparamsResolver")
	ParamsResolver paramsResolver() {
		return new ParamsResolver();
	}
	
}
