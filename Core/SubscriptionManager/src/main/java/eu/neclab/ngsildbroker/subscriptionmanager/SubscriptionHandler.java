package eu.neclab.ngsildbroker.subscriptionmanager;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.ldcontext.AtContextProducerChannel;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.subscriptionmanager.config.SubscriptionManagerProducerChannel;

@SpringBootApplication
@EnableBinding({ SubscriptionManagerProducerChannel.class, AtContextProducerChannel.class })
@Import({ KafkaConfig.class })
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

	@Bean("smparamsResolver")
	ParamsResolver paramsResolver() {
		return new ParamsResolver();
	}
}
