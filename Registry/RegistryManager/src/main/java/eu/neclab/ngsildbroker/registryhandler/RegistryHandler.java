package eu.neclab.ngsildbroker.registryhandler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
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
import eu.neclab.ngsildbroker.commons.stream.service.CommonKafkaConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.swaggerConfig.SwaggerConfigDetails;
import eu.neclab.ngsildbroker.registryhandler.config.CSourceProducerChannel;

//@Component(immediate=true)
@SpringBootApplication
@EnableBinding({ CSourceProducerChannel.class, AtContextProducerChannel.class })
@Import({CommonKafkaConfig.class, SwaggerConfigDetails.class})
public class RegistryHandler {
	
	public static void main(String[] args) {
		SpringApplication.run(RegistryHandler.class);
	}

	@Bean("rmops")
	KafkaOps ops() {
		return new KafkaOps();
	}

	@Bean("rmconRes")
	ContextResolverBasic conRes() {
		return new ContextResolverBasic();
	}

	@Bean("rmrestTemplate")
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

	@Bean("rmgetCsourceRegistration")
	CSourceRegistration getCsourceRegistration() {
		return new CSourceRegistration();
	}

	@Bean("rmsecurityConfig")
	SecurityConfig securityConfig() {
		return new SecurityConfig();
	}
		
	@Bean("rmresourceConfigDetails")
	ResourceConfigDetails resourceConfigDetails() {
		return new ResourceConfigDetails();
	}
	@Bean("rmqueryParser")
	QueryParser queryParser() {
		return new QueryParser();
	}
	@Bean("rmparamsResolver")
	ParamsResolver paramsResolver() {
		return new ParamsResolver();
	}
	
}
