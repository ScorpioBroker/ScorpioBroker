
package eu.neclab.ngsildbroker.gateway;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsConfigurationSource;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

@EnableWebFluxSecurity
public class SecurityConfiguration {

	@Value("${spring.security.mode:deactivated}")
	String securityMode;

	@Value("${scorpio.cors.enable:false}")
	boolean enableCors;
	@Value("${scorpio.cors.allowall:false}")
	boolean allowAllCors;

	@Value("${scorpio.cors.allowedorigin:null}")
	String allowedOrigin;

	@Value("${scorpio.cors.allowedheader:null}")
	String allowedHeader;

	@Value("${scorpio.cors.allowallmethods:false}")
	boolean allowAllCorsMethods;

	@Value("${scorpio.cors.allowedmethods:null}")
	String allowedMethods;

	@Bean
	public SecurityWebFilterChain configure(ServerHttpSecurity http, Environment env) {
		// CORS has to be first otherwise the access-control-allow-origin header gets
		// stripped

		switch (securityMode) {
			case "header":
				http.cors().configurationSource(corsConfiguration()).and().authorizeExchange().anyExchange()
						.authenticated().and().csrf().disable().oauth2ResourceServer().jwt();
				break;
			case "webauth":
				http.cors().configurationSource(corsConfiguration()).and().authorizeExchange().anyExchange()
						.authenticated().and().httpBasic().and().oauth2Client().and().oauth2Login().and().formLogin();
				break;
			case "deactivated":
			default:
				http.cors().configurationSource(corsConfiguration()).and().authorizeExchange().anyExchange().permitAll()
						.and().csrf().disable();
				break;
		}
		return http.build();
	}

	public CorsConfigurationSource corsConfiguration() {
		final UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
		if (!enableCors) {
			return source;
		}
		final CorsConfiguration config = new CorsConfiguration();
		if (allowAllCors) {
			config.applyPermitDefaultValues();
			config.setAllowedMethods(Arrays.asList("*"));
		} else {
			if (allowedOrigin != null) {
				for (String origin : allowedOrigin.split(",")) {
					config.addAllowedOrigin(origin);
				}
			}
			if (allowedHeader != null) {
				config.setAllowedHeaders(Arrays.asList(allowedHeader.split(",")));
			}
			if (allowAllCorsMethods) {
				config.setAllowedMethods(Arrays.asList("*"));
			} else {
				if (allowedMethods != null) {
					config.setAllowedMethods(Arrays.asList(allowedMethods.split(",")));
				}
			}
		}
		source.registerCorsConfiguration("/**", config);
		return source;
	}

}
