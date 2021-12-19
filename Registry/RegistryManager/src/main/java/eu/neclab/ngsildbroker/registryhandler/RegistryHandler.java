package eu.neclab.ngsildbroker.registryhandler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.securityConfig.SecurityConfiguration;
import eu.neclab.ngsildbroker.commons.storage.StorageWriterDAO;
import eu.neclab.ngsildbroker.registryhandler.config.RegistryJdbcConfig;

@SpringBootApplication
@Import(SecurityConfiguration.class)
public class RegistryHandler {

	public static void main(String[] args) {
		SpringApplication.run(RegistryHandler.class, args);
	}

	@Autowired
	RegistryJdbcConfig jdbcConfig;
	

	@Bean
	RestTemplate restTemplate() {
		return new RestTemplate(clientHttpRequestFactory());
	}

	// rest template timeout configs
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
	
	@Bean("csdao")
	StorageWriterDAO storageWriterDAO() {
		return new StorageWriterDAO();
	}

}
