package eu.neclab.ngsildbroker.commons.securityConfig;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties
@ConfigurationProperties
public class SecurityConfig {

	private String secEnabled;
	private List<ConfigDetails> auth;
	public String getSecEnabled() {
		return secEnabled;
	}
	public void setSecEnabled(String secEnabled) {
		this.secEnabled = secEnabled;
	}
	public List<ConfigDetails> getAuth() {
		return auth;
	}
	public void setAuth(List<ConfigDetails> auth) {
		this.auth = auth;
	}
}
