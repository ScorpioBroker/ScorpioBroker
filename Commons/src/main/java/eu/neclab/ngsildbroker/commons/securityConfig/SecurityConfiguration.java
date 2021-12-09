package eu.neclab.ngsildbroker.commons.securityConfig;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

@EnableWebFluxSecurity
public class SecurityConfiguration {

	@Value("${security.active:false}")
	boolean securityActive;

	

	@Bean
	public SecurityWebFilterChain configure(ServerHttpSecurity http) throws Exception {
		System.err.println("%%%%%%%%%%%%%%%^^^^^^^^^^^^^&&&&&&&&&&&&&&&***************");
		http.cors();
		if (securityActive) {
			http.oauth2ResourceServer().jwt();
			
		} else {
			http.authorizeExchange().anyExchange().permitAll();
		}
		return http.build();
	}
}
