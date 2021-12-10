package eu.neclab.ngsildbroker.runner;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.web.client.RestTemplate;

import eu.neclab.ngsildbroker.commons.ldcontext.AtContext;
import eu.neclab.ngsildbroker.commons.ldcontext.AtContextProducerChannel;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.storage.StorageWriterDAO;
import eu.neclab.ngsildbroker.commons.stream.service.CommonKafkaConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.entityhandler.config.EntityJdbcConfig;
import eu.neclab.ngsildbroker.entityhandler.config.EntityProducerChannel;
import eu.neclab.ngsildbroker.entityhandler.config.EntityTopicMap;
import eu.neclab.ngsildbroker.historymanager.config.ProducerChannel;
import eu.neclab.ngsildbroker.queryhandler.config.QueryProducerChannel;
import eu.neclab.ngsildbroker.registryhandler.config.CSourceProducerChannel;
import eu.neclab.ngsildbroker.subscriptionmanager.config.SubscriptionManagerProducerChannel;

import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@SpringBootApplication
@ComponentScan(basePackages = { "eu.neclab.ngsildbroker.*" }, excludeFilters = {
		@Filter(type = FilterType.REGEX, pattern = { "eu.neclab.ngsildbroker.commons.*" }),
		@Filter(type = FilterType.ANNOTATION, value = SpringBootApplication.class) })
@EnableBinding({ EntityProducerChannel.class, AtContextProducerChannel.class, ProducerChannel.class,
		QueryProducerChannel.class, CSourceProducerChannel.class, SubscriptionManagerProducerChannel.class })
@Import(KafkaConfig.class)
public class Runner {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Runner.class, args);
	}

	@Bean
	@Primary
	KafkaOps ops() {
		return new KafkaOps();
	}

	@Bean
	@Primary
	AtContext atCon() {
		return new AtContext();
	}

	@Bean
	@Primary
	RestTemplate restTemp() {
		return new RestTemplate();
	}

	@Autowired
	EntityJdbcConfig jdbcConfig;

	@Bean
	@Primary
	ParamsResolver paramsResolver() {
		return new ParamsResolver();
	}

	@Bean
	@Primary
	EntityTopicMap entityTopicMap() {
		return new EntityTopicMap();
	}

	@Bean
	@Primary
	StorageWriterDAO storageWriterDAO() {
		return new StorageWriterDAO();
	}

}
