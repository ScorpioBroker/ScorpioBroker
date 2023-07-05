package com.github.jsonldjava.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringBufferInputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.DocumentLoader;
import com.github.jsonldjava.core.JsonLdApi;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.net.HttpHeaders;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;

import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.RequestAcceptEncoding;
import org.apache.http.client.protocol.ResponseContentEncoding;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.cache.BasicHttpCacheStorage;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;

/**
 * Functions used to make loading, parsing, and serializing JSON easy using
 * Jackson.
 *
 * @author tristan
 *
 */
public class JsonUtils {
	/**
	 * An HTTP Accept header that prefers JSONLD.
	 */
	public static final String ACCEPT_HEADER = "application/ld+json, application/json;q=0.9, application/javascript;q=0.5, text/javascript;q=0.5, text/plain;q=0.2, */*;q=0.1";

	/**
	 * The user agent used by the default {@link CloseableHttpClient}.
	 *
	 * This will not be used if
	 * {@link DocumentLoader#setHttpClient(CloseableHttpClient)} is called with a
	 * custom client.
	 */
	public static final String JSONLD_JAVA_USER_AGENT = "JSONLD-Java";

	private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
	private static final JsonFactory JSON_FACTORY = new JsonFactory(JSON_MAPPER);

	private static volatile CloseableHttpClient DEFAULT_HTTP_CLIENT;
	// Avoid possible endless loop when following alternate locations
	private static final int MAX_LINKS_FOLLOW = 20;

	static {
		// Disable default Jackson behaviour to close
		// InputStreams/Readers/OutputStreams/Writers
		JSON_FACTORY.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
		// Disable string retention features that may work for most JSON where
		// the field names are in limited supply, but does not work for JSON-LD
		// where a wide range of URIs are used for subjects and predicates
		JSON_FACTORY.disable(JsonFactory.Feature.INTERN_FIELD_NAMES);
		JSON_FACTORY.disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES);
	}

	/**
	 * Parses a JSON-LD document from the given {@link InputStream} to an object
	 * that can be used as input for the {@link JsonLdApi} and
	 * {@link JsonLdProcessor} methods.<br>
	 * Uses UTF-8 as the character encoding when decoding the InputStream.
	 *
	 * @param input The JSON-LD document in an InputStream.
	 * @return A JSON Object.
	 * @throws JsonParseException If there was a JSON related error during parsing.
	 * @throws IOException        If there was an IO error during parsing.
	 */
	public static Uni<Object> fromInputStream(InputStream input) {
		// filter BOMs from InputStream
		try (final BOMInputStream bOMInputStream = new BOMInputStream(input, false, ByteOrderMark.UTF_8,
				ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_32BE, ByteOrderMark.UTF_32LE);) {
			Charset charset = StandardCharsets.UTF_8;
			// Attempt to use the BOM if it exists
			if (bOMInputStream.hasBOM()) {
				try {
					charset = Charset.forName(bOMInputStream.getBOMCharsetName());
				} catch (final IllegalArgumentException e) {
					// If there are any issues with the BOM charset, attempt to
					// parse with UTF_8
					charset = StandardCharsets.UTF_8;
				}
			}
			return fromInputStream(bOMInputStream, charset);
		} catch (IOException e) {
			return Uni.createFrom().failure(e);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					return Uni.createFrom().failure(e);
				}
			}
		}
	}

	/**
	 * Parses a JSON-LD document from the given {@link InputStream} to an object
	 * that can be used as input for the {@link JsonLdApi} and
	 * {@link JsonLdProcessor} methods.
	 *
	 * @param input The JSON-LD document in an InputStream.
	 * @param enc   The character encoding to use when interpreting the characters
	 *              in the InputStream.
	 * @return A JSON Object.
	 * @throws JsonParseException If there was a JSON related error during parsing.
	 * @throws IOException        If there was an IO error during parsing.
	 */
	public static Object fromInputStream(InputStream input, String enc) throws IOException {
		return fromInputStream(input, Charset.forName(enc));
	}

	/**
	 * Parses a JSON-LD document from the given {@link InputStream} to an object
	 * that can be used as input for the {@link JsonLdApi} and
	 * {@link JsonLdProcessor} methods.
	 *
	 * @param input The JSON-LD document in an InputStream.
	 * @param enc   The character encoding to use when interpreting the characters
	 *              in the InputStream.
	 * @return A JSON Object.
	 * @throws JsonParseException If there was a JSON related error during parsing.
	 * @throws IOException        If there was an IO error during parsing.
	 */
	public static Uni<Object> fromInputStream(InputStream input, Charset enc) {
		try (InputStreamReader in = new InputStreamReader(input, enc);
				BufferedReader reader = new BufferedReader(in);) {
			return fromReader(reader);
		} catch (IOException e) {
			return Uni.createFrom().failure(e);
		}
	}

	/**
	 * Parses a JSON-LD document from the given {@link Reader} to an object that can
	 * be used as input for the {@link JsonLdApi} and {@link JsonLdProcessor}
	 * methods.
	 *
	 * @param reader The JSON-LD document in a Reader.
	 * @return A JSON Object.
	 * @throws JsonParseException If there was a JSON related error during parsing.
	 * @throws IOException        If there was an IO error during parsing.
	 */
	public static Uni<Object> fromReader(Reader reader) {
		JsonParser jp;
		try {
			jp = JSON_FACTORY.createParser(reader);
		} catch (IOException e) {
			return Uni.createFrom().failure(e);
		}
		return fromJsonParser(jp);
	}

	/**
	 * Parses a JSON-LD document from the given {@link JsonParser} to an object that
	 * can be used as input for the {@link JsonLdApi} and {@link JsonLdProcessor}
	 * methods.
	 *
	 * @param jp The JSON-LD document in a {@link JsonParser}.
	 * @return A JSON Object.
	 * @throws JsonParseException If there was a JSON related error during parsing.
	 * @throws IOException        If there was an IO error during parsing.
	 */
	public static Uni<Object> fromJsonParser(JsonParser jp) {
		Object rval;
		JsonToken initialToken;
		try {
			initialToken = jp.nextToken();

			if (initialToken == JsonToken.START_ARRAY) {
				rval = jp.readValueAs(List.class);
			} else if (initialToken == JsonToken.START_OBJECT) {
				rval = jp.readValueAs(Map.class);
			} else if (initialToken == JsonToken.VALUE_STRING) {
				rval = jp.readValueAs(String.class);
			} else if (initialToken == JsonToken.VALUE_FALSE || initialToken == JsonToken.VALUE_TRUE) {
				rval = jp.readValueAs(Boolean.class);
			} else if (initialToken == JsonToken.VALUE_NUMBER_FLOAT || initialToken == JsonToken.VALUE_NUMBER_INT) {
				rval = jp.readValueAs(Number.class);
			} else if (initialToken == JsonToken.VALUE_NULL) {
				rval = null;
			} else {
				return Uni.createFrom().failure(new JsonParseException(jp,
						"document doesn't start with a valid json element : " + initialToken, jp.getCurrentLocation()));
			}
		} catch (IOException e) {
			return Uni.createFrom().failure(e);
		}

		JsonToken t;
		try {
			t = jp.nextToken();
		} catch (final JsonParseException ex) {
			return Uni.createFrom()
					.failure(new JsonParseException(jp,
							"Document contains more content after json-ld element - (possible mismatched {}?)",
							jp.getCurrentLocation()));
		} catch (IOException e) {
			return Uni.createFrom().failure(e);
		}
		if (t != null) {
			return Uni.createFrom().failure(new JsonParseException(jp,
					"Document contains possible json content after the json-ld element - (possible mismatched {}?)",
					jp.getCurrentLocation()));
		}
		return Uni.createFrom().item(rval);
	}

	/**
	 * Parses a JSON-LD document from a string to an object that can be used as
	 * input for the {@link JsonLdApi} and {@link JsonLdProcessor} methods.
	 *
	 * @param jsonString The JSON-LD document as a string.
	 * @return A JSON Object.
	 * @throws JsonParseException If there was a JSON related error during parsing.
	 * @throws IOException        If there was an IO error during parsing.
	 */
	public static Object fromString(String jsonString) throws JsonParseException, IOException {
		return fromReader(new StringReader(jsonString));
	}

	/**
	 * Writes the given JSON-LD Object out to a String, using indentation and new
	 * lines to improve readability.
	 *
	 * @param jsonObject The JSON-LD Object to serialize.
	 * @return A JSON document serialised to a String.
	 * @throws JsonGenerationException If there is a JSON error during
	 *                                 serialization.
	 * @throws IOException             If there is an IO error during serialization.
	 */
	public static String toPrettyString(Object jsonObject) throws JsonGenerationException, IOException {
		final StringWriter sw = new StringWriter();
		writePrettyPrint(sw, jsonObject);
		return sw.toString();
	}

	/**
	 * Writes the given JSON-LD Object out to a String.
	 *
	 * @param jsonObject The JSON-LD Object to serialize.
	 * @return A JSON document serialised to a String.
	 * @throws JsonGenerationException If there is a JSON error during
	 *                                 serialization.
	 * @throws IOException             If there is an IO error during serialization.
	 */
	public static String toString(Object jsonObject) throws JsonGenerationException, IOException {
		final StringWriter sw = new StringWriter();
		write(sw, jsonObject);
		return sw.toString();
	}

	/**
	 * Writes the given JSON-LD Object out to the given Writer.
	 *
	 * @param writer     The writer that is to receive the serialized JSON-LD
	 *                   object.
	 * @param jsonObject The JSON-LD Object to serialize.
	 * @throws JsonGenerationException If there is a JSON error during
	 *                                 serialization.
	 * @throws IOException             If there is an IO error during serialization.
	 */
	public static void write(Writer writer, Object jsonObject) throws JsonGenerationException, IOException {
		final JsonGenerator jw = JSON_FACTORY.createGenerator(writer);
		jw.writeObject(jsonObject);
	}

	/**
	 * Writes the given JSON-LD Object out to the given Writer, using indentation
	 * and new lines to improve readability.
	 *
	 * @param writer     The writer that is to receive the serialized JSON-LD
	 *                   object.
	 * @param jsonObject The JSON-LD Object to serialize.
	 * @throws JsonGenerationException If there is a JSON error during
	 *                                 serialization.
	 * @throws IOException             If there is an IO error during serialization.
	 */
	public static void writePrettyPrint(Writer writer, Object jsonObject) throws JsonGenerationException, IOException {
		final JsonGenerator jw = JSON_FACTORY.createGenerator(writer);
		jw.useDefaultPrettyPrinter();
		jw.writeObject(jsonObject);
	}

	/**
	 * Parses a JSON-LD document, from the contents of the JSON resource resolved
	 * from the JsonLdUrl, to an object that can be used as input for the
	 * {@link JsonLdApi} and {@link JsonLdProcessor} methods.
	 *
	 * @param url        The JsonLdUrl to resolve
	 * @param httpClient The {@link CloseableHttpClient} to use to resolve the URL.
	 * @return A JSON Object.
	 * @throws JsonParseException If there was a JSON related error during parsing.
	 * @throws IOException        If there was an IO error during parsing.
	 */
	public static Uni<Object> fromURL(java.net.URL url, WebClient webClient) {
		final String protocol = url.getProtocol();
		// We can only use the Apache HTTPClient for HTTP/HTTPS, so use the
		// native java client for the others
		if (!protocol.equalsIgnoreCase("http") && !protocol.equalsIgnoreCase("https")) {
			// Can't use the HTTP client for those!
			// Fallback to Java's built-in JsonLdUrl handler. No need for
			// Accept headers as it's likely to be file: or jar:
			try {
				return fromInputStream(url.openStream());
			} catch (IOException e) {
				return Uni.createFrom().failure(e);
			}
		} else {
			return fromJsonLdViaHttpUri(url, webClient, 0);
		}
	}

	private static Uni<Object> fromJsonLdViaHttpUri(final URL url, WebClient webClient, int linksFollowed) {
		final HttpUriRequest request = new HttpGet(url.toExternalForm());
		// We prefer application/ld+json, but fallback to application/json
		// or whatever is available
		request.addHeader("Accept", ACCEPT_HEADER);
		return webClient.get(url.toExternalForm()).send().onItem().transformToUni(result -> {
			final int status = result.statusCode();
			if (status != 200 && status != 203) {
				return Uni.createFrom().failure(new IOException("Can't retrieve " + url + ", status code: " + status));
			}
			URL alternateLink;
			try {
				alternateLink = alternateLink(url, result);
			} catch (MalformedURLException e) {
				return Uni.createFrom().failure(e);
			}
			if (alternateLink != null) {

				if (linksFollowed + 1 > MAX_LINKS_FOLLOW) {
					return Uni.createFrom().failure(
							new IOException("Too many alternate links followed. This may indicate a cycle. Aborting."));
				}
				return fromJsonLdViaHttpUri(alternateLink, webClient, linksFollowed + 1);
			}
			return fromInputStream(new ByteArrayInputStream(result.bodyAsString().getBytes(StandardCharsets.UTF_8)));

		});

	}

	private static URL alternateLink(URL url, HttpResponse<Buffer> result) throws MalformedURLException {

		if (result.headers().contains(HttpHeaders.CONTENT_TYPE)
				&& !result.headers().get(HttpHeaders.CONTENT_TYPE).equals("application/ld+json")) {
			for (Entry<String, String> header : result.headers().entries()) {
				if (header.getKey().equalsIgnoreCase("link")) {
					String alternateLink = "";
					boolean relAlternate = false;
					boolean jsonld = false;
					for (String value : header.getValue().split(";")) {
						value = value.trim();
						if (value.startsWith("<") && value.endsWith(">")) {
							alternateLink = value.substring(1, value.length() - 1);
						}
						if (value.startsWith("type=\"application/ld+json\"")) {
							jsonld = true;
						}
						if (value.startsWith("rel=\"alternate\"")) {
							relAlternate = true;
						}
					}
					if (jsonld && relAlternate && !alternateLink.isEmpty()) {
						return new URL(url.getProtocol() + "://" + url.getAuthority() + alternateLink);
					}
				}
			}
		}
		return null;
	}

	/**
	 * Fallback method directly using the {@link java.net.HttpURLConnection} class
	 * for cases where servers do not interoperate correctly with Apache HTTPClient.
	 *
	 * @param url The URL to access.
	 * @return The result, after conversion from JSON to a Java Object.
	 * @throws JsonParseException If there was a JSON related error during parsing.
	 * @throws IOException        If there was an IO error during parsing.
	 */
	public static Object fromURLJavaNet(URL url) throws JsonParseException, IOException {
		final HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
		urlConn.addRequestProperty("Accept", ACCEPT_HEADER);

		final StringWriter output = new StringWriter();
		try (final InputStream directStream = urlConn.getInputStream();) {
			IOUtils.copy(directStream, output, StandardCharsets.UTF_8);
		} finally {
			output.flush();
		}
		final Object context = JsonUtils.fromReader(new StringReader(output.toString()));
		return context;
	}

	public static CloseableHttpClient getDefaultHttpClient() {
		CloseableHttpClient result = DEFAULT_HTTP_CLIENT;
		if (result == null) {
			synchronized (JsonUtils.class) {
				result = DEFAULT_HTTP_CLIENT;
				if (result == null) {
					result = DEFAULT_HTTP_CLIENT = JsonUtils.createDefaultHttpClient();
				}
			}
		}
		return result;
	}

	public static CloseableHttpClient createDefaultHttpClient() {
		final CacheConfig cacheConfig = createDefaultCacheConfig();

		final CloseableHttpClient result = createDefaultHttpClient(cacheConfig);

		return result;
	}

	public static CacheConfig createDefaultCacheConfig() {
		return CacheConfig.custom().build();
//		return CacheConfig.custom().setMaxCacheEntries(500).setMaxObjectSize(1024 * 256).setSharedCache(false)
//				.setHeuristicCachingEnabled(true).setHeuristicDefaultLifetime(86400).build();
	}

	public static CloseableHttpClient createDefaultHttpClient(final CacheConfig cacheConfig) {
		return createDefaultHttpClientBuilder(cacheConfig).build();
	}

	public static HttpClientBuilder createDefaultHttpClientBuilder(final CacheConfig cacheConfig) {
		// Common CacheConfig for both the JarCacheStorage and the underlying
		// BasicHttpCacheStorage
		return CachingHttpClientBuilder.create()
				// allow caching
				.setCacheConfig(cacheConfig)
				// Wrap the local JarCacheStorage around a BasicHttpCacheStorage
				.setHttpCacheStorage(new JarCacheStorage(null, cacheConfig, new BasicHttpCacheStorage(cacheConfig)))
				// Support compressed data
				// https://wayback.archive.org/web/20130901115452/http://hc.apache.org:80/httpcomponents-client-ga/tutorial/html/httpagent.html#d5e1238
				.addInterceptorFirst(new RequestAcceptEncoding()).addInterceptorFirst(new ResponseContentEncoding())
				.setRedirectStrategy(DefaultRedirectStrategy.INSTANCE)
				// User agent customisation
				.setUserAgent(JSONLD_JAVA_USER_AGENT)
				// use system defaults for proxy etc.
				.useSystemProperties();
	}

	private JsonUtils() {
		// Static class, no access to constructor
	}
}
