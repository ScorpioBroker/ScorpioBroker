package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.http.impl.headers.HeadersMultiMap;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
@RegisterForReflection
public class EndPoint {

	private String accept;
	private URI uri;
	private Map<String, String> notifierInfo;
	private HeadersMultiMap receiverInfo;

	public EndPoint() {

	}

	public EndPoint(EndPoint endPoint) {
		this.accept = endPoint.accept;
		try {
			this.uri = new URI(endPoint.uri.toString());
		} catch (URISyntaxException e) {
			// will never happen
		}
		if (endPoint.notifierInfo != null) {
			this.notifierInfo = new HashMap<String, String>(endPoint.notifierInfo);
		}
	}

	public void update(EndPoint endPoint) {
		if (endPoint.accept != null) {
			this.accept = endPoint.accept;
		}
		if (endPoint.uri != null) {
			try {
				this.uri = new URI(endPoint.uri.toString());
			} catch (URISyntaxException e) {
				// will never happen
			}
		}
		if (endPoint.notifierInfo != null) {
			this.notifierInfo = new HashMap<String, String>(endPoint.notifierInfo);
		}

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

	public void setReceiverInfo(HeadersMultiMap receiverInfo) {
		this.receiverInfo = receiverInfo;
	}

	public HeadersMultiMap getReceiverInfo() {
		return receiverInfo;
	}

}