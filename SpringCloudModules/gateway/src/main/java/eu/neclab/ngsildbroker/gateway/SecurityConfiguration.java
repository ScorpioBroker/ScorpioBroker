
package eu.neclab.ngsildbroker.gateway;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

@EnableWebFluxSecurity
public class SecurityConfiguration {

	@Value("${spring.security.active:false}")
	boolean securityActive;

	@Bean
	public SecurityWebFilterChain configure(ServerHttpSecurity http, Environment env) {
		http.cors();
		if (securityActive) { // http.oauth2ResourceServer().jwt();
			http.authorizeExchange().anyExchange().authenticated().and().httpBasic().and().oauth2Client().and()
					.oauth2Login().and().formLogin();
		} else {
			http.authorizeExchange().anyExchange().permitAll().and().csrf().disable();
		}
		return http.build();
	}
}
