package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.Map;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class EndPoint {

	private String accept;
	private URI uri;
	private Map<String,String>notifierInfo;

	public EndPoint(){

	}

	public void finalize() throws Throwable {

	}

	public String getAccept() {
		return accept;
	}

	public void setAccept(String accept) {
		this.accept = accept;
	}

	public URI getUri() {
		return uri;
	}

	public void setUri(URI uri) {
		this.uri = uri;
	}
	
	public Map<String, String> getNotifierInfo() {
		return notifierInfo;
	}

	public void setNotifierInfo(Map<String, String> notifierInfo) {
		this.notifierInfo = notifierInfo;
	}
	
}