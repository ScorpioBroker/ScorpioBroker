package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */

public class EndPoint implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -7535049847687307441L;
	private String accept;
	private URI uri;
	private Map<String, String> notifierInfo;
	private ArrayListMultimap<String, String> receiverInfo;

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
		if (accept == null) {
			return "application/json";
		}
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

	public void setReceiverInfo(ArrayListMultimap<String, String> receiverInfo) {
		this.receiverInfo = receiverInfo;
	}

	public ArrayListMultimap<String, String> getReceiverInfo() {
		return receiverInfo;
	}

}