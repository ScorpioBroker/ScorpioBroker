package eu.neclab.ngsildbroker.commons.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.http.HttpStatus;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.google.common.collect.Sets;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.Attrib;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

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

	private static Set<String> DO_NOT_SCAN_ATTRIBS = Sets.newHashSet("id", "type", "createdAt", "scope");

//	public static final RestResponse<Object> NOT_FOUND_REPLY = RestResponseBuilderImpl.create(HttpStatus.SC_NOT_FOUND)
//			.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
//			.entity(new NGSIRestResponse(ErrorType.NotFound, "Resource not found.").toJson()).build();

	public static boolean doPreflightCheck(HttpServerRequest req, List<Object> atContextLinks)
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

	public static Uni<List<Object>> getAtContext(HttpServerRequest req) {

		return parseLinkHeader(req, NGSIConstants.HEADER_REL_LDCONTEXT);
	}

	public static Uni<List<Object>> parseLinkHeader(HttpServerRequest req, String headerRelLdcontext) {
		return parseLinkHeader(req.headers().getAll("Link"), headerRelLdcontext);
	}

	public static List<Object> parseLinkHeaderNoUni(List<String> rawLinks, String headerRelLdcontext) {
		ArrayList<Object> result = new ArrayList<Object>();
		if (rawLinks != null) {

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
		}
		return result;
	}

	public static List<Object> getAtContextNoUni(HttpServerRequest request) {
		return parseLinkHeaderNoUni(request.headers().getAll("Link"), NGSIConstants.HEADER_REL_LDCONTEXT);
	}

	public static Uni<List<Object>> parseLinkHeader(List<String> rawLinks, String headerRelLdcontext) {
		return Uni.createFrom().item(parseLinkHeaderNoUni(rawLinks, headerRelLdcontext));

	}

	public static Uni<RestResponse<Object>> generateReply(HttpServerRequest request, Object reply, int endPoint) {
		return generateReply(request, reply, ArrayListMultimap.create(), endPoint);

	}

	public static Uni<RestResponse<Object>> generateReply(HttpServerRequest request, Object reply,
			ArrayListMultimap<String, String> additionalHeaders, int endPoint) {
		return generateReply(request, reply, additionalHeaders, null, endPoint);
	}

	public static Uni<RestResponse<Object>> generateReply(HttpServerRequest request, Object reply,
			ArrayListMultimap<String, String> additionalHeaders, List<Object> context, int endPoint) {
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

	public static Uni<RestResponse<Object>> generateReply(HttpServerRequest request, Object reply,
			ArrayListMultimap<String, String> additionalHeaders, List<Object> additionalContext,
			boolean forceArrayResult, int endPoint) {
		return getAtContext(request).onItem().transform(t -> {
			List<Object> result = Lists.newArrayList();

			if (additionalContext != null) {
				result.addAll(additionalContext);
			}
			if (t != null) {
				result.addAll(t);
			}
			return result;
		}).onItem().transformToUni(t -> {
			Context context = JsonLdProcessor.getCoreContextClone().parse(t, true);
			return generateReply(request, reply, additionalHeaders, context, t, forceArrayResult, endPoint);
		});

	}

	private static Uni<RestResponse<Object>> generateReply(HttpServerRequest request, String reply,
			ArrayListMultimap<String, String> additionalHeaders, Context ldContext, List<Object> contextLinks,
			boolean forceArrayResult, int endPoint) {
		try {
			return generateReply(request, JsonUtils.fromString(reply), additionalHeaders, ldContext, contextLinks,
					forceArrayResult, endPoint);
		} catch (IOException e) {
			return Uni.createFrom().failure(e);
		}
	}

	public static Uni<RestResponse<Object>> generateReply(HttpServerRequest request, Object expanded,
			ArrayListMultimap<String, String> additionalHeaders, Context ldContext, List<Object> contextLinks,
			boolean forceArrayResult, int endPoint) {
		return getReplyBody(request.headers().getAll(HttpHeaders.ACCEPT), endPoint, additionalHeaders, expanded,
				forceArrayResult, ldContext, contextLinks, getGeometry(request)).onItem().transformToUni(t -> {
					boolean compress = false;
					String options = request.params().get(NGSIConstants.QUERY_PARAMETER_OPTIONS);
					if (options != null && options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_COMPRESS)) {
						compress = true;
					}
					return generateReply(t, additionalHeaders, compress);
				});

	}

	private static Uni<String> getReplyBody(List<String> acceptHeader, int endPoint,
			ArrayListMultimap<String, String> additionalHeaders, Object expanded, boolean forceArrayResult,
			Context ldContext, List<Object> contextLinks, String geometryProperty) {
		String replyBody;
		int sendingContentType = parseAcceptHeader(acceptHeader);
		Map<String, Object> compacted;
		try {
			compacted = JsonLdProcessor.compact(expanded, contextLinks, ldContext, opts, endPoint);
		} catch (JsonLdError | ResponseException e4) {
			return Uni.createFrom().failure(e4);
		}
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
			try {
				replyBody = JsonUtils.toPrettyString(result);
			} catch (IOException e3) {
				return Uni.createFrom().failure(e3);
			}
			if (contextLinks != null) {
				for (Object entry : contextLinks) {
					if (entry instanceof String) {
						additionalHeaders.put(com.google.common.net.HttpHeaders.LINK, "<" + entry
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
			try {
				replyBody = JsonUtils.toPrettyString(result);
			} catch (IOException e2) {
				return Uni.createFrom().failure(e2);
			}
			break;
		case 3:
			additionalHeaders.put(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_NQUADS);
			try {
				replyBody = RDFDatasetUtils.toNQuads((RDFDataset) JsonLdProcessor.toRDF(result));
			} catch (JsonLdError | ResponseException e1) {
				return Uni.createFrom().failure(e1);
			}
			break;
		case 4:// geo+json
			switch (endPoint) {
			case AppConstants.QUERY_ENDPOINT:
				additionalHeaders.put(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_GEO_JSON);
				try {
					replyBody = JsonUtils.toPrettyString(generateGeoJson(result, geometryProperty, context));
				} catch (IOException e) {
					return Uni.createFrom().failure(e);
				}
				break;
			default:
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotAcceptable,
						"Provided accept types " + acceptHeader + " are not supported"));
			}
			break;
		case -1:
		default:
			return Uni.createFrom().failure(new ResponseException(ErrorType.NotAcceptable,
					"Provided accept types " + acceptHeader + " are not supported"));
		}
		return Uni.createFrom().item(replyBody);

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

	private static String getGeometry(HttpServerRequest request) {
		String result = request.params().get(NGSIConstants.QUERY_PARAMETER_GEOMETRY_PROPERTY);
		if (result == null) {
			return NGSIConstants.NGSI_LD_LOCATION_SHORT;
		}
		return result;
	}

	private static String getAtContextServing(Object entry) {
		// TODO Auto-generated method stub
		return "http change this";
	}

	public static Uni<RestResponse<Object>> generateReply(String replyBody,
			ArrayListMultimap<String, String> additionalHeaders, boolean compress) {
		return generateReply(replyBody, additionalHeaders, HttpStatus.SC_OK, compress);
	}

	public static Uni<RestResponse<Object>> generateReply(HttpServerRequest request, QueryResult qResult,
			boolean forceArray, boolean count, Context context, List<Object> contextLinks, int endPoint) {
		ArrayListMultimap<String, String> additionalHeaders = ArrayListMultimap.create();
		if (count == true) {
			additionalHeaders.put(NGSIConstants.COUNT_HEADER_RESULT, String.valueOf(qResult.getCount()));
		}
		if (qResult == null || qResult.getData() == null || qResult.getData().size() == 0) {
			return HttpUtils.generateReply(request, Lists.newArrayList(), additionalHeaders, endPoint);
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
		return HttpUtils.generateReply(request, qResult.getData(), additionalHeaders, context, contextLinks, forceArray,
				endPoint);
	}

	public static Uni<RestResponse<Object>> generateReply(String replyBody,
			ArrayListMultimap<String, String> additionalHeaders, int status, boolean compress) {

		ResponseBuilder<Object> builder = RestResponseBuilderImpl.create(status);

		if (additionalHeaders != null) {
			for (Entry<String, String> entry : additionalHeaders.entries()) {
				builder = builder.header(entry.getKey(), entry.getValue());
			}
		}
		Object body;
		if (compress) {
			body = zipResult(replyBody);
			builder = builder.header(HttpHeaders.CONTENT_TYPE, "application/zip");
		} else {
			body = replyBody;
		}
		return Uni.createFrom().item(builder.entity(body).build());
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

	public static ArrayListMultimap<String, String> getHeaders(HttpServerRequest request) {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		for (Entry<String, String> entry : request.headers()) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}

	public static String getTenantFromHeaders(ArrayListMultimap<String, String> headers) {
		if (headers.containsKey(NGSIConstants.TENANT_HEADER)) {
			return headers.get(NGSIConstants.TENANT_HEADER).get(0);
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

	static String generateNextLink(HttpServerRequest request, QueryResult qResult) {
		if (qResult.getResultsLeftAfter() == null || qResult.getResultsLeftAfter() <= 0) {
			return null;
		}
		return generateFollowUpLinkHeader(request, qResult.getOffset() + qResult.getLimit(), qResult.getLimit(),
				qResult.getqToken(), "next");
	}

	public static String generateFollowUpLinkHeader(HttpServerRequest request, int offset, int limit, String token,
			String rel) {
		StringBuilder builder = new StringBuilder("</");
		builder.append("?");
		for (Entry<String, String> entry : request.params().entries()) {
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
			builder.append(key + "=" + entry.getValue() + "&");
		}
		builder.append("offset=" + offset + "&");
		builder.append("limit=" + limit + "&");
		// builder.append("qtoken=" + token);
		builder.append(">;rel=\"" + rel + "\"");
		return builder.toString();
	}

	private static String generatePrevLink(HttpServerRequest request, QueryResult qResult) {
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

	public static RestResponse<Object> handleControllerExceptions(Throwable e) {
		if (e instanceof ResponseException) {
			ResponseException responseException = (ResponseException) e;
			logger.debug("Exception :: ", responseException);
			return RestResponseBuilderImpl.create(responseException.getErrorCode())
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.entity(responseException.getJson()).build();
		}
		if (e instanceof DateTimeParseException) {
			logger.debug("Exception :: ", e);
			return RestResponseBuilderImpl.create(HttpStatus.SC_BAD_REQUEST)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.entity(new ResponseException(ErrorType.BadRequestData, "Failed to parse provided datetime field.")
							.getJson())
					.build();
		}
		if (e instanceof JsonProcessingException || e instanceof JsonLdError) {
			logger.debug("Exception :: ", e);
			return RestResponseBuilderImpl.create(HttpStatus.SC_BAD_REQUEST)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.entity(new ResponseException(ErrorType.InvalidRequest,
							"There is an error in the provided json document").getJson())
					.build();
		}
		logger.error("Exception :: ", e);
		return RestResponseBuilderImpl.create(HttpStatus.SC_INTERNAL_SERVER_ERROR)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.entity(new ResponseException(ErrorType.InternalError, e.getMessage()).getJson()).build();
	}

	public static URI validateUri(String uri) throws ResponseException {
		try {
			return validateUri(new URI(uri));
		} catch (URISyntaxException e) {
			throw new ResponseException(ErrorType.BadRequestData, "id is not a URI");
		}

	}

	public static URI validateUri(URI uri) throws ResponseException {
		if (!uri.isAbsolute()) {
			throw new ResponseException(ErrorType.BadRequestData, "id is not a URI");
		}
		return uri;

	}

	public static RestResponse<String> generateNotification(ArrayListMultimap<String, String> origHeaders,
			Object notificationData, List<Object> context, String geometryProperty)
			throws ResponseException, JsonGenerationException, JsonParseException, IOException {
		Context ldContext = JsonLdProcessor.getCoreContextClone().parse(context, true);

		ArrayListMultimap<String, String> headers;
		if (origHeaders == null) {
			headers = ArrayListMultimap.create();
		} else {
			headers = ArrayListMultimap.create(origHeaders);
		}
		List<String> acceptHeader = headers.get(io.vertx.mutiny.core.http.HttpHeaders.ACCEPT.toString());
		if (acceptHeader == null || acceptHeader.isEmpty()) {
			acceptHeader = new ArrayList<String>();
			acceptHeader.add("application/json");
		}
		String body = getReplyBody(acceptHeader, AppConstants.QUERY_ENDPOINT, headers, notificationData, true,
				ldContext, context, geometryProperty).await().indefinitely();
		// need to clean context for subscriptions. This is a bit bad practice but reply
		// generation relies on side effects so clean up here
		HashSet<Object> temp = Sets.newHashSet(context);
		context.clear();
		context.addAll(temp);

		ResponseBuilder<String> builder = RestResponseBuilderImpl.ok(body);
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
				for (String entry : Sets.newHashSet(headers.get(key))) {
					builder = builder.header(key, entry);
				}
			}
		}
		return builder.build();
	}

	@SuppressWarnings("unchecked")
	public static MultiMap getAdditionalHeaders(Map<String, Object> registration, List<Object> context,
			List<String> accept) {
		MultiMap result = HeadersMultiMap.headers();

		// Context myContext = JsonLdProcessor.getCoreContextClone().parse(context,
		// true);
		for (String entry : accept) {
			result.add(HttpHeaders.ACCEPT, entry);
		}
		Object tenant = registration.get(NGSIConstants.NGSI_LD_TENANT);
		if (tenant != null) {
			result.add(NGSIConstants.TENANT_HEADER,
					(String) ((List<Map<String, Object>>) tenant).get(0).get(NGSIConstants.JSON_LD_ID));
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

	public static MultiMap getQueryParamMap(HttpServerRequest request) {
		if (request.query() == null)
			return null;
		MultiMap mMap = MultiMap.caseInsensitiveMultiMap();
		String[] params = request.query().split("&");
		for (String param : params) {
			mMap.add(param.split("=", 2)[0], param.split("=", 2)[1]);
		}
		return mMap;
	}

	public static String utfDecoder(String data) {
		try {
			return URLDecoder.decode(data, NGSIConstants.ENCODE_FORMAT);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	public static RestResponse<Object> generateUpdateResultResponse(NGSILDOperationResult updateResult) {
		if (updateResult.getFailures().isEmpty()) {
			return RestResponse.noContent();
		}
		if (updateResult.getSuccesses().isEmpty()) {
			if (updateResult.getFailures().size() == 1) {
				ResponseException failure = updateResult.getFailures().get(0);
				return handleControllerExceptions(failure);
			}
		} else {
			boolean only404 = true;
			for (ResponseException failure : updateResult.getFailures()) {
				if (failure.getErrorCode() != 404) {
					only404 = false;
					break;
				}
			}
			if (only404) {
				return RestResponse.noContent();
			}
		}
		return new RestResponseBuilderImpl().status(207).entity(new JsonObject(updateResult.getJson())).build();

	}

	public static MultiMap getHeadersForRemoteCall(JsonArray headerFromReg, String tenant) {
		MultiMap result = HeadersMultiMap.headers();
		headerFromReg.forEach(t -> {
			JsonObject obj = (JsonObject) t;
			obj.forEach(headerEntry -> {
				result.add(headerEntry.getKey(), (String) headerEntry.getValue());
			});
		});
		result.add("Accept", "application/json");
		if (tenant != null) {
			result.add(NGSIConstants.TENANT_HEADER, tenant);
		}
		return result;
	}

	public static RestResponse<Object> generateEntityResult(List<Object> contextHeader, Context context,
			int acceptHeader, Map<String, Object> entity, String geometryProperty, Set<String> options) {
		String replyBody;
		String contentType;
		Object result;
		Map<String, Object> compacted;
		ResponseBuilder<Object> resp = RestResponseBuilderImpl.ok();
		switch (acceptHeader) {
		case 1:
			try {
				// todo add options to compact
				compacted = JsonLdProcessor.compact(entity, contextHeader, context, opts, -1);
			} catch (Exception e) {
				return handleControllerExceptions(e);
			}
			Object bodyContext = compacted.remove(NGSIConstants.JSON_LD_CONTEXT);
			if (contextHeader.isEmpty()) {
				contextHeader.add(((List<Object>) bodyContext).get(0));
			}
			try {
				replyBody = JsonUtils.toPrettyString(compacted);
			} catch (Exception e) {
				return handleControllerExceptions(e);
			}
			for (Object entry : contextHeader) {
				resp.header(NGSIConstants.LINK_HEADER, getLinkHeader(entry));
			}
			contentType = AppConstants.NGB_APPLICATION_JSON;
		case 2:
			try {
				// todo add options to compact
				compacted = JsonLdProcessor.compact(entity, contextHeader, context, opts, -1);
			} catch (Exception e) {
				return handleControllerExceptions(e);
			}
			contentType = AppConstants.NGB_APPLICATION_JSONLD;
			try {
				replyBody = JsonUtils.toPrettyString(compacted);
			} catch (Exception e) {
				return handleControllerExceptions(e);
			}
			break;
		case 3:

			try {
				replyBody = RDFDatasetUtils.toNQuads((RDFDataset) JsonLdProcessor.toRDF(entity));
			} catch (JsonLdError | ResponseException e) {
				return HttpUtils.handleControllerExceptions(e);
			}
			contentType = AppConstants.NGB_APPLICATION_NQUADS;
			break;
		case 4:// geo+json
			try {
				// todo add options to compact
				compacted = JsonLdProcessor.compact(entity, contextHeader, context, opts, -1);
				replyBody = JsonUtils.toPrettyString(generateGeoJson(compacted, geometryProperty, contextHeader));
			} catch (Exception e) {
				return handleControllerExceptions(e);
			}

			contentType = AppConstants.NGB_APPLICATION_GEO_JSON;
			break;
		default:
			return handleControllerExceptions(new ResponseException(ErrorType.InternalError));
		}
		if (options.contains("compress")) {
			result = zipResult(replyBody);
			contentType = AppConstants.NGB_APPLICATION_ZIP;
		} else {
			result = replyBody;
		}
		return resp.header(HttpHeaders.CONTENT_TYPE, contentType).entity(result).build();
	}

	private static String getLinkHeader(Object entry) {
		return "<" + entry + ">; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"";
	}

	public static String getTenant(HttpServerRequest request) {
		String tenant = request.headers().get(NGSIConstants.TENANT_HEADER);
		if (tenant == null) {
			return AppConstants.INTERNAL_NULL_KEY;
		}
		return tenant;
	}

	public static Set<Attrib> getAttribsFromCompactedPayload(Map<String, Object> value) {
		Set<Attrib> result = new HashSet<>(value.size() - 2);
		String datasetId;
		Map<String, Object> map;
		for (Entry<String, Object> entry : value.entrySet()) {
			datasetId = null;
			if (DO_NOT_SCAN_ATTRIBS.contains(entry.getKey())) {
				continue;
			}
			map = (Map<String, Object>) entry.getValue();
			if (map.containsKey("datasetId")) {
				datasetId = (String) map.get("datasetId");
			}
			result.add(new Attrib(entry.getKey(), datasetId));
		}
		return result;
	}

	public static RestResponse<Object> generateBatchResult(List<NGSILDOperationResult> t) {
		// TODO Auto-generated method stub
		return null;
	}

	public static Context getContextFromPayload(Map<String, Object> originalPayload, List<Object> atContextHeader,
			boolean atContextAllowed) throws ResponseException {
		Context context = JsonLdProcessor.getCoreContextClone();
		Object payloadAtContext = originalPayload.remove(NGSIConstants.JSON_LD_CONTEXT);
		if (payloadAtContext == null) {
			if (atContextAllowed) {
				throw new ResponseException(ErrorType.BadRequestData, "@Context entry is needed");
			}
			context.parse(atContextHeader, true);
		} else {
			if (!atContextAllowed) {
				throw new ResponseException(ErrorType.BadRequestData, "@context entry in body is not allowed");
			}
			context.parse(payloadAtContext, true);

		}
		return null;
	}

	public static RestResponse<Object> generateDeleteResult(NGSILDOperationResult result) {
		// TODO Auto-generated method stub
		return null;
	}

	public static Tuple2<Context, Map<String, Object>> expandBody(HttpServerRequest request, String payload,
			int payloadType) throws Exception {
		boolean atContextAllowed;
		List<Object> atContext = getAtContextNoUni(request);
		atContextAllowed = HttpUtils.doPreflightCheck(request, atContext);
		if (payload == null || payload.isEmpty()) {
			throw new ResponseException(ErrorType.InvalidRequest, "You have to provide a valid payload");
		}
		Map<String, Object> originalPayload;
		originalPayload = (Map<String, Object>) JsonUtils.fromString(payload);
		Context context = HttpUtils.getContextFromPayload(originalPayload, atContext, atContextAllowed);
		Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor
				.expand(context, originalPayload, opts, payloadType, atContextAllowed).get(0);
		return Tuple2.of(context, resolved);
	}

	public static RestResponse<Object> generateCreateResult(NGSILDOperationResult operationResult, String baseUrl) {
		List<ResponseException> fails = operationResult.getFailures();
		List<CRUDSuccess> successes = operationResult.getSuccesses();
		if (fails.isEmpty()) {
			if (!operationResult.isWasUpdated()) {
				try {
					return RestResponse.created(new URI(baseUrl + operationResult.getEntityId()));
				} catch (URISyntaxException e) {
					return HttpUtils.handleControllerExceptions(e);
				}
			} else {
				return RestResponse.ok();
			}
		} else if (successes.isEmpty() && fails.size() == 1) {
			return HttpUtils.handleControllerExceptions(fails.get(0));
		} else {
			try {
				return new RestResponseBuilderImpl<Object>().status(207).type(AppConstants.NGB_APPLICATION_JSON)
						.entity(JsonUtils.toPrettyString(operationResult.getJson())).build();
			} catch (Exception e) {
				return HttpUtils.handleControllerExceptions(e);
			}
		}
	}

}
