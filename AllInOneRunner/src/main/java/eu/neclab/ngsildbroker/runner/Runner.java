package eu.neclab.ngsildbroker.runner;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;

import eu.neclab.ngsildbroker.commons.securityConfig.SecurityConfiguration;

@SpringBootApplication
@ComponentScan(basePackages = {
		"eu.neclab.ngsildbroker.*" }, excludeFilters = @Filter(type = FilterType.REGEX, pattern = {
				"eu.neclab.ngsildbroker.commons.*" }))
public class Runner {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Runner.class, args);
	}
	
	



}
