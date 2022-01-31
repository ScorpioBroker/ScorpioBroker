package eu.neclab.ngsildbroker.atcontextserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestTemplate;

import eu.neclab.ngsildbroker.commons.securityConfig.WebSecurityConfiguration;


@SpringBootApplication
@Import(WebSecurityConfiguration.class)
public class AtContextServer {

	public static void main(String[] args) {
		SpringApplication.run(AtContextServer.class, args);
	}

}
