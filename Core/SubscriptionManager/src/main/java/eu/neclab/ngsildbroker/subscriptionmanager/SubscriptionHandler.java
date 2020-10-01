package eu.neclab.ngsildbroker.subscriptionmanager;

import org.springframework.beans.factory.annotation.Value;
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
import eu.neclab.ngsildbroker.commons.stream.service.KafkaConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.swaggerConfig.SwaggerConfigDetails;

@SpringBootApplication
@EnableBinding({ AtContextProducerChannel.class })
@Import({KafkaConfig.class, SwaggerConfigDetails.class})
public class SubscriptionHandler {

	@Value("${atcontext.url}")
	String atContextServerUrl;

	public static void main(String[] args) {
		SpringApplication.run(SubscriptionHandler.class, args);

	}
	
	@Bean("smops")
	KafkaOps ops() {
		return new KafkaOps();
	}
	
	@Bean("smconRes")
	ContextResolverBasic conRes() {
		return new ContextResolverBasic(atContextServerUrl);
	}
	
	@Bean("smsecurityConfig")
	SecurityConfig securityConfig() {
		return new SecurityConfig();
	}
//	@Bean("smrestTemp")
//	RestTemplate restTemp() {
//		return new RestTemplate();
//	}
//		
	@Bean("smresourceConfigDetails")
	ResourceConfigDetails resourceConfigDetails() {
		return new ResourceConfigDetails();
	}
	
	@Bean("smparamsResolver")
	ParamsResolver paramsResolver() {
		return new ParamsResolver();
	}	
	@Bean("smqueryParser")
	QueryParser queryParser() {
		return new QueryParser();
	}
}
