package eu.neclab.ngsildbroker.commons.tools;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.net.ssl.SSLContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.params.ConnRouteParams;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.params.HttpParams;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.ResponseEntity.BodyBuilder;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.HttpErrorResponseException;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.CompactedJson;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;

/**
 * A utility class to handle HTTP Requests and Responses.
 * 
 * @author the scorpio team
 * 
 */
@SuppressWarnings("deprecation")
public final class HttpUtils {

	/** Timeout for all requests to respond. */
	private static final Integer REQ_TIMEOUT_MS = 10000;

	private static HttpUtils SINGLETON;

	private static final int BUFFER_SIZE = 1024;

	private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

	private static final int DEFAULT_PROXY_PORT = 8080;

	private HttpHost httpProxy = null;

	private ContextResolverBasic contextResolver;
	private Pattern headerPattern = Pattern.compile(
			"((\\*\\/\\*)|(application\\/\\*)|(application\\/json)|(application\\/ld\\+json)|(application\\/n-quads))(\\s*\\;\\s*q=(\\d(\\.\\d)*))?\\s*\\,?\\s*");

	private HttpUtils(ContextResolverBasic contextResolver) {
		this.contextResolver = contextResolver;
		// Nothing to do, but make sure not more than one instance is created.
	}
	
	//Dummy instance with out context resolving. only used for gets and posts etc.
	private static HttpUtils NULL_INSTANCE = new HttpUtils(null);
	/**
	 * Returns the singleton instance of this class.
	 * 
	 * @return an HttpUtils instance
	 */
	public static HttpUtils getInstance(ContextResolverBasic contextResolver) {
		if(contextResolver == null) {
			return NULL_INSTANCE;
		}
		if (SINGLETON == null) {
			SINGLETON = new HttpUtils(contextResolver);
		}
		return SINGLETON;
	}

	public static void doPreflightCheck(HttpServletRequest req, String payload) throws ResponseException {
		String contentType = req.getHeader(HttpHeaders.CONTENT_TYPE);
		if (contentType == null) {
			throw new ResponseException(ErrorType.UnsupportedMediaType, "No content type header provided");
		}
		if (!contentType.toLowerCase().contains("application/json")
				&& !contentType.toLowerCase().contains("application/ld+json")) {
			throw new ResponseException(ErrorType.UnsupportedMediaType,
					"Unsupported content type. Allowed are application/json and application/ld+json. You provided "
							+ contentType);
		}
		if (payload == null || payload.trim().isEmpty() || payload.trim().equals("{}") || payload.trim().equals("[]")) {
			throw new ResponseException(ErrorType.BadRequestData, "empty payloads are not allowed in this operation");
		}

		if (contentType.toLowerCase().contains("application/json") && payload.contains("@context")) {
			throw new ResponseException(ErrorType.BadRequestData,
					"data of the content type application/json cannot provide an @context entry in the body");
		}

	}

	public String expandPayload(HttpServletRequest request, String payload, int endPoint)
			throws ResponseException, MalformedURLException, UnsupportedEncodingException {

		String ldResolved = null;

		final String contentType = request.getContentType();
		final List<Object> linkHeaders = HttpUtils.parseLinkHeader(request, NGSIConstants.HEADER_REL_LDCONTEXT);

		// PayloadValidationRule rule = new PayloadValidationRule();
		// rule.validateEntity(payload, request);

		if (contentType.equalsIgnoreCase(AppConstants.NGB_APPLICATION_JSON)) {

			ldResolved = contextResolver.expand(payload, linkHeaders, true, endPoint);

		} else if (contentType.equalsIgnoreCase(AppConstants.NGB_APPLICATION_JSONLD)) {
			if (!payload.contains("@context")) {
				throw new ResponseException(ErrorType.BadRequestData,
						"You have to provide an @context entry in the body with Content-Type: "
								+ AppConstants.NGB_APPLICATION_JSONLD);
			}

			ldResolved = contextResolver.expand(payload, null, true, endPoint);

		} else {
			throw new ResponseException(ErrorType.BadRequestData,
					"Missing or unknown Content-Type header. Content-Type header is mandatory. Only application/json or application/ld+json mime type is allowed");
		}

		return ldResolved;
	}

	/**
	 * Set the HTTP Proxy that will be used for all future requests.
	 * 
	 * @param httpProxy a URL with the HTTP proxy
	 */
	public static void setHttpProxy(URL httpProxy) {
		if (httpProxy != null) {
			int port = httpProxy.getPort();
			if (port == -1) {
				port = DEFAULT_PROXY_PORT;
			}
			SINGLETON.httpProxy = new HttpHost(httpProxy.getHost(), port, httpProxy.getProtocol());
		} else {
			SINGLETON.httpProxy = null;
		}
	}

	private String doHTTPRequest(URI uri, HTTPMethod method, Object body, Map<String, String> additionalHeaders,
			AuthScope authScope, UsernamePasswordCredentials credentials) throws IOException {
		ErrorAwareResponseHandler handler = new ErrorAwareResponseHandler();
		return doHTTPRequest2(uri, method, body, additionalHeaders, authScope, credentials, handler);
	}

	private String doHTTPRequest(URI uri, HTTPMethod method, Object body, Map<String, String> additionalHeaders,
			AuthScope authScope, UsernamePasswordCredentials credentials, ResponseHandler<String> handler)
			throws IOException {
		DefaultHttpClient httpClient = new DefaultHttpClient();
		HttpParams params = httpClient.getParams();
		params.setParameter("http.socket.timeout", REQ_TIMEOUT_MS);
		params.setParameter("http.connection.timeout", REQ_TIMEOUT_MS);
		params.setParameter("http.connection-manager.timeout", REQ_TIMEOUT_MS.longValue());
		params.setParameter("http.protocol.head-body-timeout", REQ_TIMEOUT_MS);

		if (httpProxy != null) {
			params.setParameter(ConnRouteParams.DEFAULT_PROXY, httpProxy);
		}
		if (credentials != null) {
			httpClient.getCredentialsProvider().setCredentials(authScope, credentials);
		}
		HttpRequestBase request;

		switch (method) {
		case GET:
			request = new HttpGet(uri);
			break;
		case POST:
			HttpPost postRequest = new HttpPost(uri);
			if (body instanceof String) {
				addBody((String) body, postRequest);
			} else if (body instanceof File) {
				addBody((File) body, postRequest);
			} else if (body instanceof byte[]) {
				addBody((byte[]) body, postRequest);
			}

			request = postRequest;
			break;
		case PUT:
			HttpPut putRequest = new HttpPut(uri);
			if (body instanceof String) {
				addBody((String) body, putRequest);
			} else if (body instanceof File) {
				addBody((File) body, putRequest);
			}

			request = putRequest;
			break;
		case DELETE:
			request = new HttpDelete(uri);
			break;
		case HEAD:
			request = new HttpHead(uri);
			break;
		default:
			httpClient.close();
			throw new AssertionError("Unknown method: " + method);
		}
		if (additionalHeaders != null && !additionalHeaders.isEmpty()) {
			addHeaders(additionalHeaders, request);
		}
		try {
			return httpClient.execute(request, handler);
		} finally {
			httpClient.getConnectionManager().shutdown();
			httpClient.close();

		}
	}

	/**
	 * Perform an HTTP request using a disposable HTTP client.
	 * 
	 * @param httpClient the HTTP client
	 * @return the body of the message
	 * @throws IOException                if an error occurs during the request
	 * @throws HttpErrorResponseException if a non 2xx response code is returned
	 *                                    (this is an unchecked exception!)
	 */

	private String doHTTPRequest2(URI uri, HTTPMethod method, Object body, Map<String, String> additionalHeaders,
			AuthScope authScope, UsernamePasswordCredentials credentials, ResponseHandler<String> handler)
			throws IOException {

		CloseableHttpClient httpClient;
		try {
			httpClient = getClient(authScope, credentials);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		HttpRequestBase request;
		switch (method) {
		case GET:
			request = new HttpGet(uri);
			break;
		case POST:
			HttpPost postRequest = new HttpPost(uri);
			if (body instanceof String) {
				addBody((String) body, postRequest);
			} else if (body instanceof File) {
				addBody((File) body, postRequest);
			} else if (body instanceof byte[]) {
				addBody((byte[]) body, postRequest);
			}

			request = postRequest;
			break;
		case PUT:
			HttpPut putRequest = new HttpPut(uri);
			if (body instanceof String) {
				addBody((String) body, putRequest);
			} else if (body instanceof File) {
				addBody((File) body, putRequest);
			}

			request = putRequest;
			break;
		case DELETE:
			request = new HttpDelete(uri);
			break;
		case HEAD:
			request = new HttpHead(uri);
			break;
		default:
			httpClient.close();
			throw new AssertionError("Unknown method: " + method);
		}
		if (additionalHeaders != null && !additionalHeaders.isEmpty()) {
			addHeaders(additionalHeaders, request);
		}
		try {
			return httpClient.execute(request, handler);
		} finally {
			httpClient.close();

		}
	}

	private CloseableHttpClient getClient(AuthScope authScope, UsernamePasswordCredentials credentials)
			throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
		SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, (x509CertChain, authType) -> true)
				.build();
		HttpClientBuilder temp = HttpClientBuilder.create().setSSLContext(sslContext)
				.setConnectionManager(new PoolingHttpClientConnectionManager(RegistryBuilder
						.<ConnectionSocketFactory>create().register("http", PlainConnectionSocketFactory.INSTANCE)
						.register("https", new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE))
						.build()));
		if (credentials != null) {
			BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(authScope, credentials);
			temp.setDefaultCredentialsProvider(credentialsProvider);
		}
		if (httpProxy != null) {
			temp.setProxy(httpProxy);
		}

		return temp.build();
	}

	private void addBody(File body, HttpEntityEnclosingRequest req) {
		if (body != null) {
			FileEntity fileEntity = new FileEntity(body);
			req.setEntity(fileEntity);
		}

	}

	private void addBody(byte[] body, HttpEntityEnclosingRequest req) {
		if (body != null) {
			ByteArrayEntity bodyEntity = new ByteArrayEntity(body, ContentType.APPLICATION_JSON);
			req.setEntity(bodyEntity);
		}
	}

	private void addBody(String body, HttpEntityEnclosingRequest req) {
		if (body != null) {
			StringEntity bodyEntity = new StringEntity(body, ContentType.APPLICATION_JSON);
			req.setEntity(bodyEntity);
		}
	}

	private void addHeaders(Map<String, String> headers, HttpRequest req) {
		for (Entry<String, String> entry : headers.entrySet()) {
			req.addHeader(entry.getKey(), entry.getValue());
		}

	}

	/**
	 * Perform a GET request on the URI.
	 * 
	 * @param uri the URI to do the request on
	 * @return the body of the response
	 * @throws IOException generally if a communication problem occurs or
	 *                     specifically an {@link HttpErrorResponseException} if
	 *                     something other than HTTP 200 OK was returned
	 */
	public String doGet(URI uri) throws IOException {
		return doGet(uri, null, null, null, null);
	}

	public String doGet(URI uri, ResponseHandler<String> handler) throws IOException {
		return doGet(uri, null, handler);
	}

	public String doGet(URI uri, Map<String, String> headers) throws IOException {
		return doGet(uri, headers, null);
	}

	public String doGet(URI uri, Map<String, String> headers, ResponseHandler<String> handler) throws IOException {
		return doGet(uri, headers, null, null, handler);
	}

	/**
	 * Perform a GET request on the URI.
	 * 
	 * @param uri         the URI to do the request on
	 * @param authScope   the authentication scope
	 * @param credentials the authentication credentials
	 * @return the body of the response
	 * @throws IOException generally if a communication problem occurs or
	 *                     specifically an {@link HttpErrorResponseException} if
	 *                     something other than HTTP 200 OK was returned
	 */
	public String doGet(URI uri, AuthScope authScope, UsernamePasswordCredentials credentials) throws IOException {
		return doGet(uri, null, authScope, credentials, null);
	}

	public String doGet(URI uri, AuthScope authScope, UsernamePasswordCredentials credentials,
			ResponseHandler<String> handler) throws IOException {
		return doGet(uri, null, authScope, credentials, handler);
	}

	public String doGet(URI uri, Map<String, String> headers, AuthScope authScope,
			UsernamePasswordCredentials credentials) throws IOException {
		return doGet(uri, headers, authScope, credentials, null);
	}

	public String doGet(URI uri, Map<String, String> headers, AuthScope authScope,
			UsernamePasswordCredentials credentials, ResponseHandler<String> handler) throws IOException {
		if (handler == null) {
			return doHTTPRequest(uri, HTTPMethod.GET, null, headers, authScope, credentials);
		}
		return doHTTPRequest(uri, HTTPMethod.GET, null, headers, authScope, credentials, handler);
	}

	/**
	 * Perform a PUT request on the URI.
	 * 
	 * @param uri  the URI to do the request on
	 * @param body the request body
	 * @return the body of the response
	 * @throws IOException generally if a communication problem occurs or
	 *                     specifically an {@link HttpErrorResponseException} if
	 *                     something other than HTTP 200 OK was returned
	 */
	public String doPut(URI uri, String body) throws IOException {
		return doPut(uri, body, null, null);
	}

	/**
	 * Perform a PUT request on the URI.
	 * 
	 * @param uri         the URI to do the request on
	 * @param body        the request body
	 * @param authScope   the authentication scope
	 * @param credentials the authentication credentials
	 * @return the body of the response
	 * @throws IOException generally if a communication problem occurs or
	 *                     specifically an {@link HttpErrorResponseException} if
	 *                     something other than HTTP 200 OK was returned
	 */
	public String doPut(URI uri, String body, AuthScope authScope, UsernamePasswordCredentials credentials)
			throws IOException {
		return doHTTPRequest(uri, HTTPMethod.PUT, body, null, authScope, credentials);
	}

	/**
	 * Perform a POST request on the URI.
	 * 
	 * @param uri  the URI to do the request on
	 * @param body the request body
	 * @return the body of the response
	 * @throws IOException generally if a communication problem occurs or
	 *                     specifically an {@link HttpErrorResponseException} if
	 *                     something other than HTTP 200 OK was returned
	 */
	public String doPost(URI uri, Object body, Map<String, String> additionalHeaders) throws IOException {
		return doPost(uri, body, additionalHeaders, null, null);
	}

	/**
	 * Perform a POST request on the URI.
	 * 
	 * @param uri         the URI to do the request on
	 * @param body        the request body
	 * @param authScope   the authentication scope
	 * @param credentials the authentication credentials
	 * @return the body of the response
	 * @throws IOException generally if a communication problem occurs or
	 *                     specifically an {@link HttpErrorResponseException} if
	 *                     something other than HTTP 200 OK was returned
	 */
	public String doPost(URI uri, Object body, Map<String, String> additionalHeaders, AuthScope authScope,
			UsernamePasswordCredentials credentials) throws IOException {
		return doHTTPRequest(uri, HTTPMethod.POST, body, additionalHeaders, authScope, credentials);
	}

	/**
	 * Perform a DELETE request on the URI.
	 * 
	 * @param uri the URI to do the request on
	 * @return the body of the response
	 * @throws IOException generally if a communication problem occurs or
	 *                     specifically an {@link HttpErrorResponseException} if
	 *                     something other than HTTP 200 OK was returned
	 */
	public String doDelete(URI uri) throws IOException {
		return doDelete(uri, null, null);
	}

	/**
	 * Perform a POST request on the URI.
	 * 
	 * @param uri         the URI to do the request on
	 * @param authScope   the authentication scope
	 * @param credentials the authentication credentials
	 * @return the body of the response
	 * @throws IOException generally if a communication problem occurs or
	 *                     specifically an {@link HttpErrorResponseException} if
	 *                     something other than HTTP 200 OK was returned
	 */
	public String doDelete(URI uri, AuthScope authScope, UsernamePasswordCredentials credentials) throws IOException {
		return doHTTPRequest(uri, HTTPMethod.DELETE, null, null, authScope, credentials);
	}

	/**
	 * Perform a HEAD request on the URI.
	 * 
	 * @param uri the URI to do the request on
	 * @return the body of the response
	 * @throws IOException generally if a communication problem occurs or
	 *                     specifically an {@link HttpErrorResponseException} if
	 *                     something other than HTTP 200 OK was returned
	 */
	public String doHead(URI uri) throws IOException {
		return doHead(uri, null, null);
	}

	/**
	 * Perform a HEAD request on the URI.
	 * 
	 * @param uri         the URI to do the request on
	 * @param authScope   the authentication scope
	 * @param credentials the authentication credentials
	 * @return the body of the response
	 * @throws IOException generally if a communication problem occurs or
	 *                     specifically an {@link HttpErrorResponseException} if
	 *                     something other than HTTP 200 OK was returned
	 */
	public String doHead(URI uri, AuthScope authScope, UsernamePasswordCredentials credentials) throws IOException {
		return doHTTPRequest(uri, HTTPMethod.HEAD, null, null, authScope, credentials);
	}

	/**
	 * Read and decode the request's body using the encoding specified in the
	 * request.
	 * 
	 * @param req the request to read
	 * @return a String with the body
	 * @throws IOException if a connection error occurs
	 */
	public static String getBody(HttpServletRequest req) throws IOException {
		BufferedReader reader = null;
		try {
			reader = req.getReader();
			StringBuilder body = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				body.append(line);
			}

			return body.toString();
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

	/**
	 * Read and decode the response's body using the encoding in the response. If no
	 * encoding is found, UTF-8 is used.
	 * 
	 * @param res the response object
	 * @return the body of the response
	 * @throws IOException if a communication problem occurs
	 */
	public static String getBody(HttpResponse res) throws IOException {
		return EntityUtils.toString(res.getEntity(), "UTF-8");
	}

	/**
	 * Perform a HEAD request on the remoteURI and return true if the HTTP
	 * connection succeeds, regardless of the status code (potentially an error)
	 * received.
	 * 
	 * @param remoteURI the URI to check
	 * @return true if the server serving URI responds at all
	 */
	public boolean isReachable(URI remoteURI) {
		try {
			doHead(remoteURI);
		} catch (HttpErrorResponseException e) {
			// That's ok, we got errors, but the server is reachable via the
			// network.
			return true;
		} catch (IOException e) {
			// The server is not reachable for whatever reason
			return false;
		}
		// Got a 2xx message, all is good
		return true;
	}

	private static String getSanitizedHeader(HttpServletRequest req, String header) {
		String dirtyHeader = req.getHeader(header);
		if (dirtyHeader != null) {
			return dirtyHeader.replaceAll("[\r\n]", "");
		} else {
			return null;
		}
	}

	/**
	 * Enable CORS by setting the appropriate access headers required by pre-flight
	 * requests (OPTIONS method) and other non-same-origin requests. Note that the
	 * headers extracted from the request are always (re) URL encoded to prevent
	 * <a href="http://en.wikipedia.org/wiki/HTTP_response_splitting">HTTP Response
	 * Splitting attacks</a>
	 * 
	 * @param req  the request (needed due to the origin and methods it requires)
	 * @param resp the response with the right CORS headers
	 */
	private static void enableCORS(HttpServletRequest req, HttpServletResponse resp) {
		// Sanitize the origin
		String origin = getSanitizedHeader(req, "Origin");
		if (origin != null) {
			resp.addHeader("Access-Control-Allow-Origin", origin);
			if ("options".equalsIgnoreCase(req.getMethod())) {
				resp.setHeader("Allow", "GET, HEAD, POST, PUT, DELETE, TRACE, OPTIONS");
				String headers = getSanitizedHeader(req, "Access-Control-Request-Headers");
				String method = getSanitizedHeader(req, "Access-Control-Request-Method");
				resp.addHeader("Access-Control-Allow-Methods", method);
				resp.addHeader("Access-Control-Allow-Headers", headers);
			}
		}
		// Fix ios6 caching post requests
		if ("post".equalsIgnoreCase(req.getMethod())) {
			resp.addHeader("Cache-Control", "no-cache");
		}
	}

	/**
	 * Send the given response with a 200 OK status.
	 * 
	 * @param req      the request object
	 * @param response the response body
	 * @param resp     the response object
	 */
	public static void sendResponse(HttpServletRequest req, String response, HttpServletResponse resp) {
		enableCORS(req, resp);
		resp.setContentType(AppConstants.NGB_APPLICATION_JSON);
		try {
			resp.getWriter().write(response);
		} catch (IOException e) {
			LOG.warn("IOException when sending a response: " + e.getMessage());
		}

	}

	/**
	 * Send an error with the given error code as status and the given message as
	 * reason. The body is HTML formatted and contains the message.
	 * 
	 * 
	 * @param req       the request object
	 * @param errorCode the status code
	 * @param message   reason for the error
	 * @param resp      the response object
	 * 
	 */
	public static void sendError(HttpServletRequest req, int errorCode, String message, HttpServletResponse resp) {
		enableCORS(req, resp);
		try {
			LOG.warn(message);
			resp.sendError(errorCode, message);
		} catch (IOException ioe) {
			LOG.warn("IOException when sending an error: " + message);
		}
	}

	/**
	 * Get the path out of the URL from this request, trimming out slashes (both at
	 * the beginning and end) and turning nulls into empty paths.
	 * 
	 * @param req the request object
	 * @return the clean path
	 */
	public static String getCleanPath(HttpServletRequest req) {
		String path = req.getPathInfo();
		if (!StringUtils.isSet(path)) {
			return "";
		}

		try {
			path = URLDecoder.decode(path, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			LOG.error("UTF-8 should be supported.");
			throw new AssertionError(e);
		}

		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		if (path.startsWith("/")) {
			path = path.substring(1);
		}
		return path;
	}

	/**
	 * Download the file pointed by the given URI and save it into the target file.
	 * 
	 * @param uri    the URI of the file to download
	 * @param target local file to store the download into
	 * @throws IOException if a communication error occurs. Specifically throw an
	 *                     {@link HttpErrorResponseException} if anything but a 200
	 *                     OK is returned.
	 */
	public void downloadFile(URI uri, File target) throws IOException {
		downloadFile(uri, target, null, null);
	}

	/**
	 * Download the file pointed by the given URI and save it into the target file.
	 * 
	 * @param uri         the URI of the file to download
	 * @param target      local file to store the download into
	 * @param authScope   the authentication scope
	 * @param credentials username and password credentials for Basic authentication
	 * @throws IOException if a communication error occurs. Specifically throw an
	 *                     {@link HttpErrorResponseException} if anything but a 200
	 *                     OK is returned.
	 */

	public void downloadFile(URI uri, File target, AuthScope authScope, UsernamePasswordCredentials credentials)
			throws IOException {
		DefaultHttpClient httpClient = new DefaultHttpClient();
		if (httpProxy != null) {
			httpClient.getParams().setParameter(ConnRouteParams.DEFAULT_PROXY, httpProxy);
		}
		if (credentials != null) {
			httpClient.getCredentialsProvider().setCredentials(authScope, credentials);
		}

		HttpResponse response;
		response = httpClient.execute(new HttpGet(uri));
		StatusLine status = response.getStatusLine();
		httpClient.close();
		if (status.getStatusCode() != HttpServletResponse.SC_OK) {
			throw new HttpErrorResponseException(status.getStatusCode(), status.getReasonPhrase());
		}
		InputStream input = null;
		OutputStream output = null;
		byte[] buffer = new byte[BUFFER_SIZE];

		try {
			input = response.getEntity().getContent();
			output = new FileOutputStream(target);

			while (true) {
				int length = input.read(buffer);
				if (length > 0) {
					output.write(buffer, 0, length);
				} else {
					input.close();
					break;
				}
			}
		} finally {
			if (output != null) {
				output.close();
			}
			if (input != null) {
				input.close();
			}
		}
	}

	public static List<Object> getAtContext(HttpServletRequest req) {
		return parseLinkHeader(req, NGSIConstants.HEADER_REL_LDCONTEXT);
	}

	public static List<Object> parseLinkHeader(HttpServletRequest req, String headerRelLdcontext) {
		return parseLinkHeader(req.getHeaders("Link"), headerRelLdcontext);
	}

	public static List<Object> parseLinkHeader(Enumeration<String> rawLinks, String headerRelLdcontext) {

		ArrayList<Object> result = new ArrayList<Object>();
		if (rawLinks == null) {
			return result;
		}

		while (rawLinks.hasMoreElements()) {
			String[] rawLinkInfos = rawLinks.nextElement().split(";");
			boolean isWantedRel = false;
			for (String rawLinkInfo : rawLinkInfos) {
				if (rawLinkInfo.trim().startsWith("rel=")) {
					String[] relInfo = rawLinkInfo.trim().split("=");
					if (relInfo.length == 2 && (relInfo[1].equalsIgnoreCase(headerRelLdcontext)
							|| relInfo[1].equalsIgnoreCase("\"" + headerRelLdcontext + "\""))) {
						isWantedRel = true;
					}
					break;
				}
			}
			if (isWantedRel) {
				String rawLink = rawLinkInfos[0];
				if (rawLink.trim().startsWith("<")) {
					rawLink = rawLink.substring(rawLink.indexOf("<") + 1, rawLink.indexOf(">"));
				}
				result.add(rawLink);
			}

		}
		return result;
	}

	public ResponseEntity<byte[]> generateReply(HttpServletRequest request, String reply) throws ResponseException {
		return generateReply(request, reply, null);

	}

	public ResponseEntity<byte[]> generateReply(HttpServletRequest request, String reply,
			HashMap<String, List<String>> additionalHeaders) throws ResponseException {
		return generateReply(request, reply, additionalHeaders, null);
	}

	public ResponseEntity<byte[]> generateReply(HttpServletRequest request, String reply,
			HashMap<String, List<String>> additionalHeaders, List<Object> additionalContext) throws ResponseException {
		return generateReply(request, reply, additionalHeaders, additionalContext, false);
	}

	public static void main(String[] args) {
		Pattern p = Pattern.compile("([\\w\\/\\+]+)(\\s*\\;\\s*q=(\\d\\.\\d))?");
		Matcher m = p.matcher("*/*");
		float q = 1;
		String app = null;
		String result = null;
		while (m.find()) {
			String floatString = m.group(3);
			float newQ = 1;
			if (floatString != null) {
				newQ = Float.parseFloat(floatString);
			}
			if (result != null && (newQ <= q)) {
				continue;
			}
			app = m.group(0);
			if (app.equalsIgnoreCase("application/ld+json") || app.equalsIgnoreCase("application/*")
					|| app.equalsIgnoreCase("*/*")) {
				result = "application/ld+json";
			} else if (app.equalsIgnoreCase("application/json")) {
				result = "application/json";
			}

		}
		
	}

	private int parseAcceptHeader(Enumeration<String> acceptHeaders) {
		float q = 1;
		int appGroup = -1;

		while (acceptHeaders.hasMoreElements()) {
			String header = acceptHeaders.nextElement();
			
			Matcher m = headerPattern.matcher(header.toLowerCase());
			while (m.find()) {
				String floatString = m.group(8);
				float newQ = 1;
				int newAppGroup = -2;
				if (floatString != null) {
					newQ = Float.parseFloat(floatString);
				}
				if (appGroup != -1 && (newQ < q)) {
					continue;
				}
				for (int i = 2; i <= 6; i++) {
					if (m.group(i) == null) {
						continue;
					}
					newAppGroup = i;
					break;
				}
				if (newAppGroup > appGroup) {
					appGroup = newAppGroup;
				}
			}
		}
		switch (appGroup) {
		case 2:
		case 3:
		case 5:
			return 2; // application/ld+json
		case 4:
			return 1; // application/json
		case 6:
			return 3;// application/n-quads
		default:
			return -1;// error
		}
	}

	public ResponseEntity<byte[]> generateReply(HttpServletRequest request, String reply,
			HashMap<String, List<String>> additionalHeaders, List<Object> additionalContext, boolean forceArrayResult)
			throws ResponseException {
		List<Object> requestAtContext = getAtContext(request);
		if (additionalContext != null) {
			requestAtContext.addAll(additionalContext);
		}

		String replyBody;

		CompactedJson compacted = contextResolver.compact(reply, requestAtContext);
		ArrayList<String> temp = new ArrayList<String>();
		if (additionalHeaders == null) {
			additionalHeaders = new HashMap<String, List<String>>();
		}
		int sendingContentType = parseAcceptHeader(request.getHeaders(HttpHeaders.ACCEPT));
		switch (sendingContentType) {
		case 1:
			temp.add(AppConstants.NGB_APPLICATION_JSON);
			replyBody = compacted.getCompacted();
			List<String> links = additionalHeaders.get(HttpHeaders.LINK);
			if (links == null) {
				links = new ArrayList<String>();
				additionalHeaders.put(HttpHeaders.LINK, links);
			}
			links.add("<" + compacted.getContextUrl()
					+ ">; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
			break;
		case 2:
			temp.add(AppConstants.NGB_APPLICATION_JSONLD);
			if (compacted.getCompacted() == null || compacted.getCompacted().isEmpty()
					|| compacted.getCompacted().trim().equals("{ }") || compacted.getCompacted().trim().equals("{}")) {
				replyBody = "{ }";
			} else {
				replyBody = compacted.getCompactedWithContext();
			}
			break;
		case 3:
			temp.add(AppConstants.NGB_APPLICATION_NQUADS);
			replyBody = contextResolver.getRDF(reply);
			break;
		case -1:
		default:
			throw new ResponseException(ErrorType.InvalidRequest, "Provided accept types are not supported");
		}

		additionalHeaders.put(HttpHeaders.CONTENT_TYPE, temp);
		if (forceArrayResult && !replyBody.startsWith("[")) {
			if (replyBody.equals("{ }") || replyBody.equals("{}")) {
				replyBody = "[]";
			} else {
				replyBody = "[" + replyBody + "]";
			}
		}
		boolean compress = false;
		String options = request.getParameter(NGSIConstants.QUERY_PARAMETER_OPTIONS);
		if (options != null && options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_COMPRESS)) {
			compress = true;
		}
		return generateReply(replyBody, additionalHeaders, compress);
	}

	public ResponseEntity<byte[]> generateReply(String replyBody, HashMap<String, List<String>> additionalHeaders,
			boolean compress) {
		return generateReply(replyBody, additionalHeaders, HttpStatus.OK, compress);
	}

	public ResponseEntity<byte[]> generateReply(String replyBody, HashMap<String, List<String>> additionalHeaders,
			HttpStatus status, boolean compress) {
		BodyBuilder builder = ResponseEntity.status(status);
		if (additionalHeaders != null) {
			for (Entry<String, List<String>> entry : additionalHeaders.entrySet()) {
				for (String value : entry.getValue()) {
					builder.header(entry.getKey(), value);
				}

			}
		}
		byte[] body;
		if (compress) {
			body = zipResult(replyBody);
			builder.header(HttpHeaders.CONTENT_TYPE, "application/zip");
		} else {
			body = replyBody.getBytes();
		}
		return builder.body(body);
	}

	private byte[] zipResult(String replyBody) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ZipOutputStream zipOutputStream = new ZipOutputStream(baos);
		ZipEntry entry = new ZipEntry("index.json");
		entry.setSize(replyBody.length());
		try {
			zipOutputStream.putNextEntry(entry);
			zipOutputStream.write(replyBody.getBytes());
			zipOutputStream.closeEntry();
			zipOutputStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	// public static ResponseEntity<Object> generateReply(String acceptHeader,
	// List<Object> contextLinks,
	// String expandedJson, ContextResolverBasic contextResolver, String
	// atContextServerUrl)
	// throws ResponseException {
	// CompactedJson simplified = contextResolver.compact(expandedJson,
	// contextLinks);
	// BodyBuilder builder = ResponseEntity.status(HttpStatus.OK);
	// ResponseEntity<Object> result;
	// if ("application/json".equalsIgnoreCase(acceptHeader)) {
	// builder = builder.contentType(MediaType.APPLICATION_JSON);
	//
	// String[] links = new String[contextLinks.size()];
	// Object[] contextLinksArray = contextLinks.toArray();
	// for (int i = 0; i < contextLinksArray.length; i++) {
	// links[i] = (String) contextLinksArray[i];
	// }
	//
	// builder = builder.header("Link", links);
	//
	// result = builder.body(simplified.getCompacted());
	// } else {
	// builder = builder.header("Content-Type", "application/ld+json");
	//
	// result = builder.body(simplified.getCompactedWithContext());
	// }
	// return result;
	// }

}
