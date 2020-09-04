package eu.neclab.ngsildbroker.atcontextserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestTemplate;

import eu.neclab.ngsildbroker.commons.ldcontext.AtContext;
import eu.neclab.ngsildbroker.commons.ldcontext.AtContextProducerChannel;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.securityConfig.ResourceConfigDetails;
import eu.neclab.ngsildbroker.commons.securityConfig.SecurityConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.swaggerConfig.SwaggerConfigDetails;


@SpringBootApplication
@Import({KafkaConfig.class, SwaggerConfigDetails.class})
@EnableBinding({AtContextProducerChannel.class})
public class AtContextServer {// implements QueryHandlerInterface{

	public static void main(String[] args) {
		SpringApplication.run(AtContextServer.class, args);
	}

	@Bean
	KafkaOps ops() {
		return new KafkaOps();
	}
	
	@Bean
	AtContext atCon() {
		return new AtContext();
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
}
