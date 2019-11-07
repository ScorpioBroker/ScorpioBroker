package eu.neclab.ngsildbroker.runner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;

import org.springframework.context.annotation.FilterType;

@SpringBootApplication
@ComponentScan(basePackages = {
		"eu.neclab.ngsildbroker.*" }, excludeFilters = @Filter(type = FilterType.REGEX, pattern = {
				"eu.neclab.ngsildbroker.commons.*" }))
//@Import(KafkaConfig.class)
public class Runner {

	public static void main(String[] args) throws Exception{
		SpringApplication.run(Runner.class, args);
	}
}
