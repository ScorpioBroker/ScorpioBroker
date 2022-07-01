package eu.neclab.ngsildbroker.commons.securityConfig;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.firewall.DefaultHttpFirewall;
import org.springframework.security.web.firewall.HttpFirewall;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

@Configuration
@EnableWebSecurity
public class WebSecurityConfiguration extends WebSecurityConfigurerAdapter {
	@Value("${spring.security.mode:deactivated}")
	String securityMode;
	@Autowired
	Environment env;

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

	public WebSecurityConfiguration() {
		System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
	}

	private HttpFirewall allowUrlEncodedSlashHttpFirewall() {
		DefaultHttpFirewall firewall = new DefaultHttpFirewall();
		firewall.setAllowUrlEncodedSlash(true);
		return firewall;
	}

	@Override
	protected void configure(HttpSecurity http) throws Exception {
		// This checks if gateway is present in front of the microservices if yes it
		// will deactivate the security because it is handled by the gateway
		if (Arrays.stream(env.getActiveProfiles())
				.anyMatch(profile -> (profile.equalsIgnoreCase("docker") || profile.equalsIgnoreCase("eureka")))) {
			securityMode = "deactivated";
		}
		// CORS has to be first otherwise the access-control-allow-origin header gets
		// stripped
		switch (securityMode) {
			case "header":
				http.cors().configurationSource(corsConfiguration()).and().authorizeRequests().anyRequest()
						.authenticated().and().sessionManagement()
						.sessionCreationPolicy(SessionCreationPolicy.STATELESS).and().csrf().disable()
						.oauth2ResourceServer().jwt();
				break;
			case "webauth":
				http.cors().configurationSource(corsConfiguration()).and().antMatcher("/**").authorizeRequests()
						.anyRequest().authenticated().and().httpBasic().and().oauth2Client().and().oauth2Login().and()
						.formLogin();
				break;
			case "deactivated":
			default:
				http.cors().configurationSource(corsConfiguration()).and().antMatcher("/**").authorizeRequests()
						.antMatchers("/", "/webjars/**").permitAll().and().csrf().disable();
				break;
		}
	}

	@Override
	public void configure(WebSecurity web) throws Exception {
		super.configure(web);
		web.httpFirewall(allowUrlEncodedSlashHttpFirewall());
	}
}
