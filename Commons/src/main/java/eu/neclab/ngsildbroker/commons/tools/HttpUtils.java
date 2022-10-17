package eu.neclab.ngsildbroker.commons.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.servlet.http.HttpServletRequest;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.ResponseEntity.BodyBuilder;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;
import com.github.jsonldjava.core.RDFDatasetUtils;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

/**
 * A utility class to handle HTTP Requests and Responses.
 * 
 * @author the scorpio team
 * 
 */

public final class HttpUtils {

	private static final Logger logger = LoggerFactory.getLogger(HttpUtils.class);
	/** Timeout for all requests to respond. */

	private static Pattern headerPattern = Pattern.compile(
			"((\\*\\/\\*)|(application\\/\\*)|(application\\/json)|(application\\/ld\\+json)|(application\\/n-quads)|(application\\/geo\\+json))(\\s*\\;\\s*q=(\\d(\\.\\d)*))?\\s*\\,?\\s*");

	private static JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	public static boolean doPreflightCheck(HttpServletRequest req, List<Object> atContextLinks)
			throws ResponseException {
		String contentType = req.getHeader(HttpHeaders.CONTENT_TYPE);
		if (contentType == null) {
			throw new ResponseException(ErrorType.UnsupportedMediaType, "No content type header provided");
		}
		if (contentType.toLowerCase().contains("application/ld+json")) {
			if (!atContextLinks.isEmpty()) {
				throw new ResponseException(ErrorType.BadRequestData,
						"You can not have a Link to a context is content-type application/ld+json");
			}
			return true;
		}
		if (!contentType.toLowerCase().contains("application/json")
				&& !contentType.toLowerCase().contains("application/merge-patch+json")) {
			throw new ResponseException(ErrorType.UnsupportedMediaType,
					"Unsupported content type. Allowed are application/json and application/ld+json. You provided "
							+ contentType);
		}
		return false;

	}

	public static List<Object> getAtContext(HttpServletRequest req) {
		return parseLinkHeader(req, NGSIConstants.HEADER_REL_LDCONTEXT);
	}

	public static List<Object> parseLinkHeader(HttpServletRequest req, String headerRelLdcontext) {
		return parseLinkHeader(Collections.list(req.getHeaders("Link")), headerRelLdcontext);
	}

	public static List<Object> parseLinkHeader(List<String> rawLinks, String headerRelLdcontext) {

		ArrayList<Object> result = new ArrayList<Object>();
		if (rawLinks == null) {
			return result;
		}
		Iterator<String> it = rawLinks.iterator();
		while (it.hasNext()) {
			String[] rawLinkInfos = it.next().split(";");
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

	public static ResponseEntity<String> generateReply(HttpServletRequest request, String reply, int endPoint)
			throws ResponseException {
		return generateReply(request, reply, ArrayListMultimap.create(), endPoint);

	}

	public static ResponseEntity<String> generateReply(HttpServletRequest request, String reply,
			ArrayListMultimap<String, String> additionalHeaders, int endPoint) throws ResponseException {
		return generateReply(request, reply, additionalHeaders, null, endPoint);
	}

	public static ResponseEntity<String> generateReply(HttpServletRequest request, String reply,
			ArrayListMultimap<String, String> additionalHeaders, List<Object> context, int endPoint)
			throws ResponseException {
		return generateReply(request, reply, additionalHeaders, context, false, endPoint);
	}

	public static int parseAcceptHeader(List<String> acceptHeaders) {
		float q = 1;
		int appGroup = -1;
		Iterator<String> it = acceptHeaders.iterator();
		while (it.hasNext()) {
			String header = it.next();

			Matcher m = headerPattern.matcher(header.toLowerCase());
			while (m.find()) {
				String floatString = m.group(9);
				float newQ = 1;
				int newAppGroup = -2;
				if (floatString != null) {
					newQ = Float.parseFloat(floatString);
				}
				if (appGroup != -1 && (newQ < q)) {
					continue;
				}
				for (int i = 2; i <= 7; i++) {
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
			case 5:
				return 2; // application/ld+json
			case 2:
			case 3:
			case 4:
				return 1; // application/json
			case 6:
				return 3;// application/n-quads
			case 7:
				return 4;// application/geo+json
			default:
				return -1;// error
		}
	}

	public static ResponseEntity<String> generateReply(HttpServletRequest request, String reply,
			ArrayListMultimap<String, String> additionalHeaders, List<Object> additionalContext,
			boolean forceArrayResult, int endPoint) throws ResponseException {
		List<Object> requestAtContext = getAtContext(request);
		if (additionalContext == null) {
			additionalContext = new ArrayList<Object>();
		}
		if (requestAtContext != null) {
			additionalContext.addAll(requestAtContext);
		}
		Context context = JsonLdProcessor.getCoreContextClone().parse(additionalContext, true);
		return generateReply(request, reply, additionalHeaders, context, additionalContext, forceArrayResult, endPoint);
	}

	private static ResponseEntity<String> generateReply(HttpServletRequest request, String reply,
			ArrayListMultimap<String, String> additionalHeaders, Context ldContext, List<Object> contextLinks,
			boolean forceArrayResult, int endPoint) throws ResponseException {
		try {
			return generateReply(request, JsonUtils.fromString(reply), additionalHeaders, ldContext, contextLinks,
					forceArrayResult, endPoint);
		} catch (JsonLdError | IOException e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static ResponseEntity<String> generateReply(HttpServletRequest request, Object expanded,
			ArrayListMultimap<String, String> additionalHeaders, Context ldContext, List<Object> contextLinks,
			boolean forceArrayResult, int endPoint) throws ResponseException {
		String replyBody;
		try {

			replyBody = getReplyBody(Collections.list(request.getHeaders(HttpHeaders.ACCEPT)), endPoint,
					additionalHeaders, expanded, forceArrayResult, ldContext, contextLinks, getGeometry(request));
			boolean compress = false;
			String options = getQueryParamMap(request).getFirst(NGSIConstants.QUERY_PARAMETER_OPTIONS);
			if (options != null && options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_COMPRESS)) {
				compress = true;
			}
			return generateReply(replyBody, additionalHeaders, compress);
		} catch (JsonLdError | IOException e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
		}

	}

	private static String getReplyBody(List<String> acceptHeader, int endPoint,
			ArrayListMultimap<String, String> additionalHeaders, Object expanded, boolean forceArrayResult,
			Context ldContext, List<Object> contextLinks, String geometryProperty)
			throws ResponseException, JsonGenerationException, JsonParseException, IOException {
		String replyBody;
		int sendingContentType = parseAcceptHeader(acceptHeader);
		Map<String, Object> compacted = JsonLdProcessor.compact(expanded, contextLinks, ldContext, opts, endPoint);
		Object context = compacted.get(JsonLdConsts.CONTEXT);
		Object result;
		Object graph = compacted.get(JsonLdConsts.GRAPH);
		if (graph != null) {
			result = graph;
		} else {
			result = compacted;
		}
		if (forceArrayResult && !(result instanceof List)) {
			ArrayList<Object> temp = new ArrayList<Object>();
			temp.add(result);
			result = temp;
		}
		if (additionalHeaders == null) {
			additionalHeaders = ArrayListMultimap.create();
		}
		switch (sendingContentType) {
			case 1:
				additionalHeaders.put(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON);
				if (result instanceof Map) {
					((Map) result).remove(JsonLdConsts.CONTEXT);
				}
				if (result instanceof List) {
					List<Map<String, Object>> list = (List<Map<String, Object>>) result;
					for (Map<String, Object> entry : list) {
						entry.remove(JsonLdConsts.CONTEXT);
					}
				}
				replyBody = JsonUtils.toPrettyString(result);
				if (contextLinks != null) {
					for (Object entry : contextLinks) {
						if (entry instanceof String) {
							additionalHeaders.put(HttpHeaders.LINK, "<" + entry
									+ ">; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
						} else {
							additionalHeaders.put(HttpHeaders.LINK, "<" + getAtContextServing(entry)
									+ ">; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
						}

					}
				}
				break;
			case 2:
				additionalHeaders.put(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSONLD);
				if (result instanceof List) {
					List<Map<String, Object>> list = (List<Map<String, Object>>) result;
					for (Map<String, Object> entry : list) {
						entry.put(JsonLdConsts.CONTEXT, context);
					}
				}
				replyBody = JsonUtils.toPrettyString(result);
				break;
			case 3:
				additionalHeaders.put(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_NQUADS);
				replyBody = RDFDatasetUtils.toNQuads((RDFDataset) JsonLdProcessor.toRDF(result));
				break;
			case 4:// geo+json
				switch (endPoint) {
					case AppConstants.QUERY_ENDPOINT:
						additionalHeaders.put(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_GEO_JSON);
						replyBody = JsonUtils.toPrettyString(generateGeoJson(result, geometryProperty, context));
						break;
					default:
						throw new ResponseException(ErrorType.NotAcceptable,
								"Provided accept types " + acceptHeader + " are not supported");
				}
				break;
			case -1:
			default:
				throw new ResponseException(ErrorType.NotAcceptable,
						"Provided accept types " + acceptHeader + " are not supported");
		}
		return replyBody;

	}

	@SuppressWarnings("unchecked")
	private static Object generateGeoJson(Object result, String geometry, Object context)
			throws JsonParseException, IOException {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		if (result instanceof List) {
			resultMap.put(NGSIConstants.CSOURCE_TYPE, NGSIConstants.FEATURE_COLLECTION);
			ArrayList<Object> value = new ArrayList<Object>();
			for (Object entry : (List<Object>) result) {
				Object valueEntry = generateGeoJson(entry, geometry, context);
				((Map<String, Object>) valueEntry).remove(NGSIConstants.JSON_LD_CONTEXT);
				value.add(valueEntry);
			}
			resultMap.put(NGSIConstants.JSON_LD_CONTEXT, context);
			resultMap.put(NGSIConstants.FEATURES, value);
		} else {
			Map<String, Object> entryMap = (Map<String, Object>) result;
			resultMap.put(NGSIConstants.QUERY_PARAMETER_ID, entryMap.remove(NGSIConstants.QUERY_PARAMETER_ID));
			resultMap.put(NGSIConstants.CSOURCE_TYPE, NGSIConstants.FEATURE);
			Object geometryEntry = entryMap.get(geometry);
			if (geometryEntry != null) {
				resultMap.put(NGSIConstants.GEOMETRY, ((Map<String, Object>) geometryEntry).get(NGSIConstants.VALUE));
			}
			resultMap.put(NGSIConstants.PROPERTIES, entryMap);
			resultMap.put(NGSIConstants.JSON_LD_CONTEXT, context);
		}
		return resultMap;
	}

	private static String getGeometry(HttpServletRequest request) {
		String result = request.getParameter(NGSIConstants.QUERY_PARAMETER_GEOMETRY_PROPERTY);
		if (result == null) {
			return NGSIConstants.NGSI_LD_LOCATION_SHORT;
		}
		return result;
	}

	private static String getAtContextServing(Object entry) {
		// TODO Auto-generated method stub
		return "http change this";
	}

	public static ResponseEntity<String> generateReply(String replyBody,
			ArrayListMultimap<String, String> additionalHeaders, boolean compress) {
		return generateReply(replyBody, additionalHeaders, HttpStatus.OK, compress);
	}

	public static ResponseEntity<String> generateReply(HttpServletRequest request, QueryResult qResult,
			boolean forceArray, boolean count, Context context, List<Object> contextLinks, int endPoint)
			throws ResponseException {
		ArrayListMultimap<String, String> additionalHeaders = ArrayListMultimap.create();
		if (count == true) {
			additionalHeaders.put(NGSIConstants.COUNT_HEADER_RESULT, String.valueOf(qResult.getCount()));
		}
		if (qResult == null || qResult.getActualDataString() == null || qResult.getActualDataString().size() == 0) {
			return HttpUtils.generateReply(request, "[]", additionalHeaders, endPoint);
		}
		String nextLink = HttpUtils.generateNextLink(request, qResult);
		String prevLink = HttpUtils.generatePrevLink(request, qResult);
		ArrayList<Object> additionalLinks = new ArrayList<Object>();
		if (nextLink != null) {
			additionalLinks.add(nextLink);
		}
		if (prevLink != null) {
			additionalLinks.add(prevLink);
		}

		if (!additionalLinks.isEmpty()) {
			for (Object entry : additionalLinks) {
				additionalHeaders.put(HttpHeaders.LINK, (String) entry);
			}
		}
		return HttpUtils.generateReply(request, "[" + String.join(",", qResult.getDataString()) + "]",
				additionalHeaders, context, contextLinks, forceArray, endPoint);
	}

	public static ResponseEntity<String> generateReply(String replyBody,
			ArrayListMultimap<String, String> additionalHeaders, HttpStatus status, boolean compress) {
		BodyBuilder builder = ResponseEntity.status(status);
		if (additionalHeaders != null) {
			HttpHeaders headers = new HttpHeaders();
			for (Entry<String, Collection<String>> entry : additionalHeaders.asMap().entrySet()) {
				headers.addAll(entry.getKey(), new ArrayList<String>(entry.getValue()));
			}
			builder.headers(headers);
		}
		String body;
		if (compress) {
			// TODO reenable zip some how
			// body = zipResult(replyBody);
			body = replyBody;
			builder.header(HttpHeaders.CONTENT_TYPE, "application/zip");
		} else {
			body = replyBody;
		}
		return builder.body(body);
	}

	private static byte[] zipResult(String replyBody) {
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

	public static ArrayListMultimap<String, String> getHeaders(HttpServletRequest request) {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		for (String headerName : Collections.list(request.getHeaderNames())) {
			result.putAll(headerName, Collections.list(request.getHeaders(headerName)));
		}
		return result;
	}

	public static String getTenantFromHeaders(ArrayListMultimap<String, String> headers) {
		if (headers.containsKey(NGSIConstants.TENANT_HEADER_FOR_INTERNAL_CHECK)) {
			return headers.get(NGSIConstants.TENANT_HEADER_FOR_INTERNAL_CHECK).get(0);
		}
		return null;
	}

	public static String getInternalTenant(ArrayListMultimap<String, String> headers) {
		String tenantId = getTenantFromHeaders(headers);
		if (tenantId == null) {
			return AppConstants.INTERNAL_NULL_KEY;
		}
		return tenantId;
	}

	static String generateNextLink(HttpServletRequest request, QueryResult qResult) {
		if (qResult.getResultsLeftAfter() == null || qResult.getResultsLeftAfter() <= 0) {
			return null;
		}
		return generateFollowUpLinkHeader(request, qResult.getOffset() + qResult.getLimit(), qResult.getLimit(),
				qResult.getqToken(), "next");
	}

	public static String generateFollowUpLinkHeader(HttpServletRequest request, int offset, int limit, String token,
			String rel) {

		StringBuilder builder = new StringBuilder("<" + request.getRequestURL().toString());
		builder.append("?");

		for (Entry<String, List<String>> entry : getQueryParamMap(request).entrySet()) {
			List<String> values = entry.getValue();
			String key = entry.getKey();
			if (key.equals("offset")) {
				continue;
			}
			if (key.equals("qtoken")) {
				continue;
			}
			if (key.equals("limit")) {
				continue;
			}

			for (String value : values) {
				builder.append(key + "=" + value + "&");
			}

		}
		builder.append("offset=" + offset + "&");
		builder.append("limit=" + limit);
		// Not yet implemented ... we might look into views on sql level
		// builder.append("&qtoken=" + token);
		builder.append(">;rel=\"" + rel + "\"");
		return builder.toString();
	}

	private static String generatePrevLink(HttpServletRequest request, QueryResult qResult) {
		if (qResult.getResultsLeftBefore() == null || qResult.getResultsLeftBefore() <= 0) {
			return null;
		}
		int offset = qResult.getOffset() - qResult.getLimit();
		if (offset < 0) {
			offset = 0;
		}
		int limit = qResult.getLimit();

		return generateFollowUpLinkHeader(request, offset, limit, qResult.getqToken(), "prev");
	}

	public static ResponseEntity<String> handleControllerExceptions(Exception e) {
		if (e instanceof ResponseException) {
			ResponseException responseException = (ResponseException) e;
			logger.debug("Exception :: ", responseException);
			return ResponseEntity.status(responseException.getHttpStatus())
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.body(new RestResponse(responseException).toJson());
		}
		if (e instanceof DateTimeParseException) {
			logger.debug("Exception :: ", e);
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.")
							.toJson());
		}
		if (e instanceof JsonProcessingException || e instanceof JsonLdError) {
			logger.debug("Exception :: ", e);
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.body(new RestResponse(ErrorType.InvalidRequest, "There is an error in the provided json document")
							.toJson());
		}
		logger.error("Exception :: ", e);
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.body(new RestResponse(ErrorType.InternalError, e.getMessage()).toJson());
	}

	public static String validateUri(String mapValue) throws ResponseException {
		try {
			if (!new URI(mapValue).isAbsolute()) {
				throw new ResponseException(ErrorType.BadRequestData, "id is not a URI");
			}
			return mapValue;
		} catch (URISyntaxException e) {
			throw new ResponseException(ErrorType.BadRequestData, "id is not a URI");
		}

	}

	public static URI validateUri(URI mapValue) throws ResponseException {
		try {
			if (!mapValue.isAbsolute()) {
				throw new ResponseException(ErrorType.BadRequestData, "id is not a URI");
			}
			return mapValue;
		} catch (ResponseException e) {
			throw new ResponseException(ErrorType.BadRequestData, "id is not a URI");
		}

	}

	public static MultiValueMap<String, String> getQueryParamMap(HttpServletRequest request) {
		Map<String, String[]> paramMap = request.getParameterMap();
		if (paramMap == null) {
			return new LinkedMultiValueMap<String, String>();
		}
		LinkedMultiValueMap<String, String> result = new LinkedMultiValueMap<String, String>(paramMap.size());
		for (Entry<String, String[]> entry : paramMap.entrySet()) {
			result.addAll(entry.getKey(), Arrays.asList(entry.getValue()));
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public static ResponseEntity<String> generateReply(HttpServletRequest request, UpdateResult update,
			List<Object> context, int endpoint)
			throws JsonLdError, ResponseException, JsonGenerationException, IOException {
		if (update.getNotUpdated().isEmpty()) {
			return ResponseEntity.noContent().build();
		}
		Context ldContext = JsonLdProcessor.getCoreContextClone().parse(context, true);
		Map<String, Object> expanded = update.toJsonMap();
		Map<String, Object> compacted = JsonLdProcessor.compact(expanded, context, ldContext, opts,
				AppConstants.UPDATE_REQUEST);
		Object updated = compacted.get(NGSIConstants.NGSI_LD_UPDATED_SHORT);
		if (updated != null) {
			List<String> result = new ArrayList<String>();
			for (String entry : (List<String>) updated) {
				HashMap<String, Object> tmp = new HashMap<String, Object>();
				tmp.put(NGSIConstants.JSON_LD_VALUE, entry);
				result.add((String) ldContext.compactValue("dummy", tmp));
			}
			compacted.put(NGSIConstants.NGSI_LD_UPDATED_SHORT, result);
		}
		Object resultContext = compacted.get(JsonLdConsts.CONTEXT);
		Object result;
		Object graph = compacted.get(JsonLdConsts.GRAPH);
		if (graph != null) {
			result = graph;
		} else {
			result = compacted;
		}
		int sendingContentType = parseAcceptHeader(Collections.list(request.getHeaders(HttpHeaders.ACCEPT)));
		HttpHeaders resultHeaders = new HttpHeaders();
		String replyBody;
		switch (sendingContentType) {
			case 1:
				resultHeaders.add(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON);
				((Map) result).remove(JsonLdConsts.CONTEXT);
				replyBody = JsonUtils.toPrettyString(result);
				for (Object entry : context) {
					if (entry instanceof String) {
						resultHeaders.add(HttpHeaders.LINK, "<" + entry
								+ ">; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
					} else {
						resultHeaders.add(HttpHeaders.LINK, "<" + getAtContextServing(entry)
								+ ">; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
					}

				}
				break;
			case 2:
				resultHeaders.add(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSONLD);
				replyBody = JsonUtils.toPrettyString(result);
				break;
			default:
				throw new ResponseException(ErrorType.NotAcceptable, "Provided accept types are not supported");
		}
		return ResponseEntity.status(HttpStatus.MULTI_STATUS).headers(resultHeaders).body(replyBody);
	}

	public static ResponseEntity<String> generateNotification(ArrayListMultimap<String, String> origHeaders,
			Object notificationData, List<Object> context, String geometryProperty, String contentType)
			throws ResponseException, JsonGenerationException, JsonParseException, IOException {

		Context ldContext = JsonLdProcessor.getCoreContextClone();
		if (context != null) {
			ldContext = ldContext.parse(context, true);
		}

		ArrayListMultimap<String, String> headers;
		if (origHeaders == null) {
			headers = ArrayListMultimap.create();
		} else {
			headers = ArrayListMultimap.create(origHeaders);
		}

		List<String> acceptHeader;
		if (contentType == null || contentType.isEmpty()) {
			acceptHeader = Lists.newArrayList();
			acceptHeader.add("application/json");
		} else {
			acceptHeader = Lists.newArrayList(contentType);
		}

		String body = getReplyBody(acceptHeader, AppConstants.QUERY_ENDPOINT, headers, notificationData, true,
				ldContext, context, geometryProperty);
		// need to clean context for subscriptions. This is a bit bad practice but reply
		// generation relies on side effects so clean up here
		if (context != null) {
			HashSet<Object> temp = Sets.newHashSet(context);
			context.clear();
			context.addAll(temp);
		}

		return ResponseEntity.ok().headers(getHttpHeaders(headers)).body(body);
	}

	@SuppressWarnings("unchecked")
	public static HttpHeaders getAdditionalHeaders(Map<String, Object> registration, List<Object> context,
			List<String> accept) {
		HttpHeaders result = new HttpHeaders();
		result.addAll(HttpHeaders.ACCEPT, accept);
		Object tenant = registration.get(NGSIConstants.NGSI_LD_TENANT);
		if (tenant != null) {
			Map<String, Object> tmp = Maps.newHashMap();
			tmp.put(NGSIConstants.NGSI_LD_TENANT, tenant);
			try {
				result.add(NGSIConstants.TENANT_HEADER,
						(String) JsonLdProcessor.compact(tmp, null, opts).get(NGSIConstants.NGSI_LD_TENANT_SHORT));
			} catch (JsonLdError | ResponseException e) {
				logger.error("Failed to extract tenant", e);
			}

		}
		Object receiverInfo = registration.get(NGSIConstants.NGSI_LD_CONTEXT_SOURCE_INFO);
		if (receiverInfo != null) {
			try {
				Map<String, Object> headerMap = JsonLdProcessor.compact(receiverInfo, context, opts);
				headerMap.remove(NGSIConstants.JSON_LD_CONTEXT);
				for (Entry<String, Object> entry : headerMap.entrySet()) {
					result.add(entry.getKey(), entry.getValue().toString());
				}
			} catch (Exception e) {
				logger.error("Failed to read additional headers", e);
			}
		}
		return result;
	}

	private static HttpHeaders getHttpHeaders(ArrayListMultimap<String, String> headers) {
		HttpHeaders result = new HttpHeaders();
		for (String key : headers.keySet()) {
			switch (key.toLowerCase()) {
				case "postman-token":
				case "accept-encoding":
				case "user-agent":
				case "host":
				case "connection":
				case "cache-control":
				case "content-length":
					break;
				default:
					result.put(key, Lists.newArrayList(Sets.newHashSet(headers.get(key))));
			}

		}
		return result;
	}

	public static RestTemplate getRestTemplate() {
		final RestTemplate restTemplate = new RestTemplate();
		final HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
		CloseableHttpClient httpClient = HttpClientBuilder.create().setRedirectStrategy(new LaxRedirectStrategy())
				.build();
		factory.setHttpClient(httpClient);
		restTemplate.setRequestFactory(factory);
		return restTemplate;
	}

}
