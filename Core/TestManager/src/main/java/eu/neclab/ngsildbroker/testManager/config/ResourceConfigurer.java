package eu.neclab.ngsildbroker.testManager.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import eu.neclab.ngsildbroker.commons.securityConfig.ResourceConfigDetails;

/**
 * REST API Resource Server.
 */
@Configuration
@EnableWebSecurity
@EnableResourceServer
@EnableGlobalMethodSecurity(prePostEnabled = true) // Allow method annotations like @PreAuthorize
public class ResourceConfigurer extends ResourceServerConfigurerAdapter {
	@Autowired
	private ResourceConfigDetails resourceConfigDetails;

	@Override
	public void configure(HttpSecurity http) throws Exception {
		resourceConfigDetails.ngbSecurityConfig(http);
	}
}
