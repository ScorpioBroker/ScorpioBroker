package eu.neclab.ngsildbroker.runner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;

import eu.neclab.ngsildbroker.commons.stream.service.KafkaConfig;

import org.springframework.context.annotation.ComponentScans;
//import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@ComponentScan(basePackages = {
		"eu.neclab.ngsildbroker.*" }, excludeFilters = @Filter(type = FilterType.REGEX, pattern = {
				"eu.neclab.ngsildbroker.commons.*" }))

// @EntityScan(basePackages = { "eu.neclab.ngsildbroker.entityhandler.*",
// "eu.neclab.ngsildbroker.atcontextserver.*",
// "eu.neclab.ngsildbroker.historymanager.*",
// "eu.neclab.ngsildbroker.queryhandler.*",
// "eu.neclab.ngsildbroker.registryhandler.*",
// "eu.neclab.ngsildbroker.storagemanager.*",
// "eu.neclab.ngsildbroker.subscriptionmanager.*" })
// @EnableJpaRepositories(basePackages = {
// "eu.neclab.ngsildbroker.entityhandler.*",
// "eu.neclab.ngsildbroker.atcontextserver.*",
// "eu.neclab.ngsildbroker.historymanager.*",
// "eu.neclab.ngsildbroker.queryhandler.*",
// "eu.neclab.ngsildbroker.registryhandler.*",
// "eu.neclab.ngsildbroker.storagemanager.*",
// "eu.neclab.ngsildbroker.subscriptionmanager.*" })
//@Import(KafkaConfig.class)
public class Runner {

	public static void main(String[] args) {
		SpringApplication.run(Runner.class, args);
	}
}
