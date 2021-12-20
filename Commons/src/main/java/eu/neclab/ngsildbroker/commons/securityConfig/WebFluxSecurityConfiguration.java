
package eu.neclab.ngsildbroker.commons.securityConfig;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

@EnableWebFluxSecurity
public class WebFluxSecurityConfiguration {

	@Value("${spring.security.active:false}")
	boolean securityActive;

	@Bean
	public SecurityWebFilterChain configure(ServerHttpSecurity http, Environment env) {
		// This checks if gateway is present in front of the microservices if yes it
		// will deactivate the security because it is handled by the gateway
		if (Arrays.stream(env.getActiveProfiles())
				.anyMatch(profile -> (profile.equalsIgnoreCase("docker") || profile.equalsIgnoreCase("eureka")))) {
			securityActive = false;
		}

		http.cors();
		if (securityActive) { // http.oauth2ResourceServer().jwt();
			http.authorizeExchange().anyExchange().authenticated().and().httpBasic().and().oauth2Client().and()
					.oauth2Login().and().formLogin();
		} else {
			http.csrf().disable().authorizeExchange().anyExchange().permitAll();
		}
		return http.build();
	}
}
