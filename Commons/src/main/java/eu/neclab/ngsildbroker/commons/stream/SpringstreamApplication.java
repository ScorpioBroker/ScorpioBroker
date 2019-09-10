package eu.neclab.ngsildbroker.commons.stream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;

import eu.neclab.ngsildbroker.commons.stream.interfaces.KafkaConsumerChannels;
import eu.neclab.ngsildbroker.commons.stream.interfaces.KafkaProducerChannels;

@SpringBootApplication
@EnableBinding({KafkaConsumerChannels.class,KafkaProducerChannels.class})
public class SpringstreamApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringstreamApplication.class, args);
	}
}
