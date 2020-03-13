package eu.neclab.ngsildbroker.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.security.oauth2.client.EnableOAuth2Sso;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

@Configuration
@EnableOAuth2Sso
@EnableWebSecurity
public class SiteSecurityConfigurer extends WebSecurityConfigurerAdapter {
	
	@Value("${security.active}")
    private String securityEnabled;
	
	@Override
	protected void configure(HttpSecurity http) throws Exception {
		 if (securityEnabled.equalsIgnoreCase("true")) {
		http.antMatcher("/**")
			.authorizeRequests()
			.antMatchers("/", "/webjars/**")
			.permitAll()
			.anyRequest()
			.authenticated()
			.and()
			.logout()
			.logoutSuccessUrl("/")
			.permitAll()
			.and()
			.csrf()
			.disable();
	     } else {
	    	 http.antMatcher("/**")
	    	 .authorizeRequests().antMatchers("/", "/webjars/**")
	    	 .permitAll()
	    	 .and()
	    	 .csrf()
	    	 .disable(); 
	     }
	}
}
