package eu.neclab.ngsildbroker.atcontextserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;


@SpringBootApplication
public class AtContextServer {

	public static void main(String[] args) {
		SpringApplication.run(AtContextServer.class, args);
	}


	@Bean
	RestTemplate restTemp() {
		return new RestTemplate();
	}

}
