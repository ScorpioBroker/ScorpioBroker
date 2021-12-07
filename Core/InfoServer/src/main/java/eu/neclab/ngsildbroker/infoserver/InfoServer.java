package eu.neclab.ngsildbroker.infoserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.stream.service.KafkaConfig;

@SpringBootApplication
@Import({ KafkaConfig.class })
public class InfoServer {// implements QueryHandlerInterface{

	public static void main(String[] args) {
		SpringApplication.run(InfoServer.class, args);
	}

}
