
package eu.neclab.ngsildbroker.commons.securityConfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
@Configuration
public class ResourceConfigDetails {
	@Autowired
	SecurityConfig securityConfig;

	public void ngbSecurityConfig(HttpSecurity http) throws Exception {
//		if (securityConfig.getSecEnabled().equalsIgnoreCase("true")) {
//			http.httpBasic().disable();
//			for (ConfigDetails details : securityConfig.getAuth()) {
//					List<String> arrRole = details.getRole();
//					http.authorizeRequests().antMatchers(HttpMethod.resolve(details.getMethod()),details.getApi())
//					.hasAnyRole(arrRole.toString().replace("[","").replace("]",""));
//					http.authorizeRequests().antMatchers(HttpMethod.resolve(details.getMethod()),details.getApi())
//					.permitAll();
//				}
//		} else {
			http.authorizeRequests().anyRequest().permitAll();
//		}

	}
}
