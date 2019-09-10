package eu.neclab.ngsildbroker.registryhandler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.ldcontext.AtContextProducerChannel;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.securityConfig.ResourceConfigDetails;
import eu.neclab.ngsildbroker.commons.securityConfig.SecurityConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.registryhandler.config.CSourceConsumerChannel;
import eu.neclab.ngsildbroker.registryhandler.config.CSourceProducerChannel;

//@Component(immediate=true)
@SpringBootApplication
@EnableBinding({ CSourceConsumerChannel.class, CSourceProducerChannel.class, AtContextProducerChannel.class })
public class RegistryHandler {// implements EntityHandlerInterface{

	public static void main(String[] args) {
		SpringApplication.run(RegistryHandler.class, args);
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
	RestTemplate restTemplate() {
		return new RestTemplate(clientHttpRequestFactory());
	}

	//rest template timeout configs
	private ClientHttpRequestFactory clientHttpRequestFactory() {
		HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
		factory.setReadTimeout(10000);
		factory.setConnectTimeout(10000);
		return factory;
	}

	@Bean
	CSourceRegistration getCsourceRegistration() {
		return new CSourceRegistration();
	}

	@Bean
	SecurityConfig securityConfig() {
		return new SecurityConfig();
	}
		
	@Bean
	ResourceConfigDetails resourceConfigDetails() {
		return new ResourceConfigDetails();
	}
	@Bean QueryParser queryParser() {
		return new QueryParser();
	}
	@Bean
	ParamsResolver paramsResolver() {
		return new ParamsResolver();
	}
	
}
