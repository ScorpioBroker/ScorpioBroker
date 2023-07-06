package com.github.jsonldjava.core;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.ext.web.client.WebClient;

/**
 * Resolves URLs to {@link RemoteDocument}s. Subclass this class to change the
 * behaviour of loadDocument to suit your purposes.
 */
public class DocumentLoader {

	private final Map<String, Object> m_injectedDocs = new HashMap<String, Object>();

	/**
	 * Identifies a system property that can be set to "true" in order to disallow
	 * remote context loading.
	 */
	public static final String DISALLOW_REMOTE_CONTEXT_LOADING = "com.github.jsonldjava.disallowRemoteContextLoading";

	/**
	 * Avoid resolving a document by instead using the given serialised
	 * representation.
	 *
	 * @param url The URL this document represents.
	 * @param doc The serialised document as a String
	 * @return This object for fluent addition of other injected documents.
	 * @throws JsonLdError If loading of the document failed for any reason.
	 */
	public Uni<DocumentLoader> addInjectedDoc(String url, String doc) {
		return JsonUtils.fromString(doc).onItem().transformToUni(json -> {
			try {
				m_injectedDocs.put(url, json);
				return Uni.createFrom().item(this);
			} catch (final Exception e) {
				return Uni.createFrom()
						.failure(new JsonLdError(JsonLdError.Error.LOADING_INJECTED_CONTEXT_FAILED, url, e));
			}
		});
	}

	/**
	 * Loads the URL if possible, returning it as a RemoteDocument.
	 *
	 * @param url The URL to load
	 * @return The resolved URL as a RemoteDocument
	 * @throws JsonLdError If there are errors loading or remote context loading has
	 *                     been disallowed.
	 */
	public Uni<RemoteDocument> loadDocument(String url, WebClient webClient) {
		if (m_injectedDocs.containsKey(url)) {
			try {
				return Uni.createFrom().item(new RemoteDocument(url, m_injectedDocs.get(url)));
			} catch (final Exception e) {
				return Uni.createFrom()
						.failure(new JsonLdError(JsonLdError.Error.LOADING_INJECTED_CONTEXT_FAILED, url, e));
			}
		} else {
			final String disallowRemote = System.getProperty(DocumentLoader.DISALLOW_REMOTE_CONTEXT_LOADING);
			if ("true".equalsIgnoreCase(disallowRemote)) {
				return Uni.createFrom().failure(new JsonLdError(JsonLdError.Error.LOADING_REMOTE_CONTEXT_FAILED,
						"Remote context loading has been disallowed (url was " + url + ")"));
			}

			try {
				return JsonUtils.fromURL(new URL(url), webClient).onItem()
						.transform(body -> new RemoteDocument(url, body));
			} catch (final Exception e) {
				return Uni.createFrom()
						.failure(new JsonLdError(JsonLdError.Error.LOADING_REMOTE_CONTEXT_FAILED, url, e));
			}
		}
	}

}
