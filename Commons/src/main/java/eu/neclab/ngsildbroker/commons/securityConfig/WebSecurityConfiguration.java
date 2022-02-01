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
import org.springframework.security.web.firewall.DefaultHttpFirewall;
import org.springframework.security.web.firewall.HttpFirewall;

@Configuration
@EnableWebSecurity
public class WebSecurityConfiguration extends WebSecurityConfigurerAdapter {
	@Value("${spring.security.active:false}")
	boolean securityActive;
	@Autowired
	Environment env;

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
			securityActive = false;
		}
		http.cors();
		if (securityActive) {
			http.antMatcher("/**").authorizeRequests().anyRequest().authenticated().and().httpBasic().and()
					.oauth2Client().and().oauth2Login().and().formLogin();
		} else {
			http.antMatcher("/**").authorizeRequests().antMatchers("/", "/webjars/**").permitAll().and().csrf()
					.disable();
		}
	}

	@Override
	public void configure(WebSecurity web) throws Exception {
		// TODO Auto-generated method stub
		super.configure(web);
		web.httpFirewall(allowUrlEncodedSlashHttpFirewall());
	}
}
