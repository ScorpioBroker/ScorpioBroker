package eu.neclab.ngsildbroker.commons.securityConfig;

import java.util.List;

public class ConfigDetails {
	private String api;
	private List<String> role;
	private String method;
	public String getApi() {
		return api;
	}
	public void setApi(String api) {
		this.api = api;
	}
	public List<String> getRole() {
		return role;
	}
	public void setRole(List<String> role) {
		this.role = role;
	}
	public String getMethod() {
		return method;
	}
	public void setMethod(String method) {
		this.method = method;
	}
}
