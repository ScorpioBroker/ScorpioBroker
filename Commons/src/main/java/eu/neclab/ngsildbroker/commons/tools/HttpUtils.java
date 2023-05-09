package eu.neclab.ngsildbroker.commons.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.HttpStatus;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.results.Attrib;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpResponse;

/**
 * A utility class to handle HTTP Requests and Responses.
 * 
 * @author the scorpio team
 * 
 */

public final class HttpUtils {

	private static final Logger logger = LoggerFactory.getLogger(HttpUtils.class);

	public static final Uni<RestResponse<Object>> getInvalidHeader() {
		return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
				new ResponseException(ErrorType.NotAcceptable, "Provided accept types are not supported")));
	}

	private static final String CORE_CONTEXT_URL_LINK = null;;
	/** Timeout for all requests to respond. */

	private static Pattern headerPattern = Pattern.compile(
			"((\\*\\/\\*)|(application\\/\\*)|(application\\/json)|(application\\/ld\\+json)|(application\\/n-quads)|(application\\/geo\\+json))(\\s*\\;\\s*q=(\\d(\\.\\d)*))?\\s*\\,?\\s*");

	public static JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

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

	public static List<Object> getAtContext(HttpServerRequest request) {
		return parseLinkHeaderNoUni(request.headers().getAll("Link"), NGSIConstants.HEADER_REL_LDCONTEXT);
	}

	public static Uni<List<Object>> parseLinkHeader(List<String> rawLinks, String headerRelLdcontext) {
		return Uni.createFrom().item(parseLinkHeaderNoUni(rawLinks, headerRelLdcontext));

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

	public static byte[] zipResult(String replyBody) {
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

	static String generateNextLink(MultiMap params, QueryResult qResult) {
		if (qResult.getResultsLeftAfter() == null || qResult.getResultsLeftAfter() <= 0) {
			return null;
		}
		return generateFollowUpLinkHeader(params, qResult.getOffset() + qResult.getLimit(), qResult.getLimit(),
				qResult.getqToken(), "next");
	}

	public static String generateFollowUpLinkHeader(MultiMap params, int offset, int limit, String token, String rel) {
		StringBuilder builder = new StringBuilder("</");
		builder.append("?");

		for (Entry<String, String> entry : params.entries()) {
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

	private static String generatePrevLink(MultiMap params, QueryResult qResult) {
		if (qResult.getResultsLeftBefore() == null || qResult.getResultsLeftBefore() <= 0) {
			return null;
		}
		int offset = qResult.getOffset() - qResult.getLimit();
		if (offset < 0) {
			offset = 0;
		}
		int limit = qResult.getLimit();
		return generateFollowUpLinkHeader(params, offset, limit, qResult.getqToken(), "prev");
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

//	public static String utfDecoder(String data) {
//		try {
//			return URLDecoder.decode(data, NGSIConstants.ENCODE_FORMAT);
//		} catch (UnsupportedEncodingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return null;
//
//	}

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
		if (headerFromReg != null) {
			headerFromReg.forEach(t -> {
				JsonObject obj = (JsonObject) t;
				obj.forEach(headerEntry -> {
					result.add(headerEntry.getKey(), (String) headerEntry.getValue());
				});
			});
		}
		result.add("Accept", "application/json");
		if (tenant != null) {
			result.add(NGSIConstants.TENANT_HEADER, tenant);
		}
		return result;
	}

	public static RestResponse<Object> generateEntityResult(List<Object> contextHeader, Context context,
			int acceptHeader, Object entity, String geometryProperty, String options, LanguageQueryTerm langQuery) {

		try {
			Tuple2<Object, List<Tuple2<String, String>>> resultBodyAndHeaders = generateCompactedResult(contextHeader,
					context, acceptHeader, entity, geometryProperty, options, langQuery);
			ResponseBuilder<Object> resp = RestResponseBuilderImpl.ok();
			List<Tuple2<String, String>> headers = resultBodyAndHeaders.getItem2();
			for (Tuple2<String, String> entry : headers) {
				resp = resp.header(entry.getItem1(), entry.getItem2());
			}
			return resp.entity(resultBodyAndHeaders.getItem1()).build();
		} catch (Exception e) {
			return handleControllerExceptions(e);
		}
	}
	private static void makeConcise(Object compacted){
		makeConcise(compacted,null,null);
	}
	private static void makeConcise(Object compacted, Map<?, ?> parent, String key) {
		if (compacted instanceof ArrayList<?> list) {
			list.forEach(item -> {
				if (item instanceof Map<?, ?> mapItem) {
					makeConcise(mapItem, null, "");
				}
			});
		} else if (compacted instanceof Map<?, ?> map) {
			if (!map.containsKey(NGSIConstants.ID)) {
				if (map.containsKey(NGSIConstants.TYPE)
						&& !map.get(NGSIConstants.TYPE).equals(NGSIConstants.GEO_TYPE_POINT))
					map.remove(NGSIConstants.TYPE); // if object is top element then type should not be removed
				if (map.size() == 1 && map.containsKey(NGSIConstants.VALUE)) {
					((Map<String, Object>) parent).put(key, map.get(NGSIConstants.VALUE));
				}
			}
			map.forEach((str, nestedObj) -> {
				if (!str.equals(NGSIConstants.JSON_LD_CONTEXT) && nestedObj instanceof Map<?, ?> || nestedObj instanceof ArrayList<?>) {
					makeConcise(nestedObj, map, str.toString());
				}
			});
		}

	}
	public static Tuple2<Object, List<Tuple2<String, String>>> generateCompactedResult(List<Object> contextHeader,
			Context context, int acceptHeader, Object entity, String geometryProperty, String options,
			LanguageQueryTerm langQuery) throws Exception {
		String replyBody;
		String contentType;
		Object result;
		Map<String, Object> compacted;
		List<Tuple2<String, String>> headers = Lists.newArrayList();

		Set<String> optionSet = null;
		if (options != null) {
			optionSet = Set.of(options.split(","));
		}
		Object finalCompacted;
		switch (acceptHeader) {

		case 1:
			compacted = JsonLdProcessor.compact(entity, contextHeader, context, opts, -1, optionSet, langQuery);
			Object bodyContext = compacted.remove(NGSIConstants.JSON_LD_CONTEXT);
			if (contextHeader.isEmpty()) {
				contextHeader.add(((List<Object>) bodyContext).get(0));
			}
			if (compacted.containsKey(JsonLdConsts.GRAPH)) {
				finalCompacted = compacted.get(JsonLdConsts.GRAPH);
			} else {
				finalCompacted = compacted;
			}
			if(options!=null && options.contains(NGSIConstants.QUERY_PARAMETER_CONCISE_VALUE)) makeConcise(finalCompacted);
			replyBody = JsonUtils.toPrettyString(finalCompacted);
			for (Object entry : contextHeader) {
				headers.add(Tuple2.of(NGSIConstants.LINK_HEADER, getLinkHeader(entry)));
			}
			contentType = AppConstants.NGB_APPLICATION_JSON;
			break;
		case 2:
			compacted = JsonLdProcessor.compact(entity, contextHeader, context, opts, -1, optionSet, langQuery);
			if (compacted.containsKey(JsonLdConsts.GRAPH)) {
				finalCompacted = compacted.get(JsonLdConsts.GRAPH);
				bodyContext = compacted.get(NGSIConstants.JSON_LD_CONTEXT);
				if (finalCompacted instanceof List) {
					List<Map<String, Object>> tmpList = (List<Map<String, Object>>) finalCompacted;
					for (Map<String, Object> entry : tmpList) {
						entry.put(NGSIConstants.JSON_LD_CONTEXT, bodyContext);
					}
				} else if (finalCompacted instanceof Map) {
					((Map<String, Object>) finalCompacted).put(NGSIConstants.JSON_LD_CONTEXT, bodyContext);
				}
			} else {
				finalCompacted = compacted;
			}
			if(options!=null && options.contains(NGSIConstants.QUERY_PARAMETER_CONCISE_VALUE)) makeConcise(finalCompacted);
			contentType = AppConstants.NGB_APPLICATION_JSONLD;
			replyBody = JsonUtils.toPrettyString(finalCompacted);
			break;
		case 3:
			replyBody = RDFDatasetUtils.toNQuads((RDFDataset) JsonLdProcessor.toRDF(entity));
			contentType = AppConstants.NGB_APPLICATION_NQUADS;
			break;
		case 4:// geo+json
			compacted = JsonLdProcessor.compact(entity, contextHeader, context, opts, -1, optionSet, langQuery);
			if(options!=null && options.contains(NGSIConstants.QUERY_PARAMETER_CONCISE_VALUE)) makeConcise(compacted);
			replyBody = JsonUtils.toPrettyString(generateGeoJson(compacted, geometryProperty, contextHeader));
			contentType = AppConstants.NGB_APPLICATION_GEO_JSON;
			break;
		default:
			return null;
		}
		if (options != null && options.contains("compress")) {
			result = zipResult(replyBody);
			contentType = AppConstants.NGB_APPLICATION_ZIP;
		} else {
			result = replyBody;
		}
		headers.add(Tuple2.of(HttpHeaders.CONTENT_TYPE, contentType));
		return Tuple2.of(result, headers);
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
		boolean isHavingError = false;
		boolean isHavingSuccess = false;
		List<Map<String, Object>> result = new ArrayList<>();
		for (NGSILDOperationResult r : t) {
			if (!r.getFailures().isEmpty()) {
				result.add(r.getJson());
				isHavingError = true;
			}
			if (!r.getSuccesses().isEmpty()) {
				result.add(r.getJson());
				isHavingSuccess = true;
			}
		}
		if (isHavingError && !isHavingSuccess) {
			String type = (String) result.get(0).get("type");
			if( type.equalsIgnoreCase("Delete") ||
					type.equalsIgnoreCase("Append"))
				return RestResponse.status(RestResponse.Status.NOT_FOUND,result);
			else return RestResponse.status(RestResponse.Status.BAD_REQUEST, result);
		}
		if (!isHavingError && isHavingSuccess) {
			String type = (String) result.get(0).get("type");
			if(type.equalsIgnoreCase("Upsert") || type.equalsIgnoreCase("Delete") ||
					type.equalsIgnoreCase("Append"))
				return RestResponse.status(RestResponse.Status.NO_CONTENT,result);
			else return RestResponse.status(RestResponse.Status.CREATED, result);
		}
		return new RestResponseBuilderImpl<>().status(207).type(AppConstants.NGB_APPLICATION_JSON).entity(result)
				.build();
	}

	public static Context getContextFromPayload(Map<String, Object> originalPayload, List<Object> atContextHeader,
			boolean atContextAllowed) throws ResponseException {
		Context context = JsonLdProcessor.getCoreContextClone();
		Object payloadAtContext = originalPayload.remove(NGSIConstants.JSON_LD_CONTEXT);
		if (payloadAtContext == null) {
			if (atContextAllowed) {
				throw new ResponseException(ErrorType.BadRequestData, "@Context entry is needed");
			}
			context = context.parse(atContextHeader, true);
		} else {
			if (!atContextAllowed) {
				throw new ResponseException(ErrorType.BadRequestData, "@context entry in body is not allowed");
			}
			context = context.parse(payloadAtContext, true);

		}
		return context;
	}

	public static RestResponse<Object> generateDeleteResult(NGSILDOperationResult result) {
		// TODO Auto-generated method stub
		return null;
	}

	public static Tuple2<Context, Map<String, Object>> expandBody(HttpServerRequest request, String payload,
			int payloadType) throws Exception {

		if (payload == null || payload.isEmpty()) {
			throw new ResponseException(ErrorType.InvalidRequest, "You have to provide a valid payload");
		}
		Map<String, Object> originalPayload;
		originalPayload = (Map<String, Object>) JsonUtils.fromString(payload);

		return expandBody(request, originalPayload, payloadType);
	}

	public static Tuple2<Context, Map<String, Object>> expandBody(HttpServerRequest request,
			Map<String, Object> originalPayload, int payloadType) throws Exception {
		boolean atContextAllowed;
		List<Object> atContext = getAtContext(request);
		atContextAllowed = HttpUtils.doPreflightCheck(request, atContext);
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
				return RestResponse.noContent();
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

	public static List<Object> getContextFromHeader(io.vertx.mutiny.core.MultiMap remoteHeaders) {
		String tmp = remoteHeaders.get("Link").split(";")[0];
		if (tmp.charAt(0) == '<') {
			tmp = tmp.substring(1, tmp.length() - 1);
		}
		return Lists.newArrayList(tmp);
	}

	public static RestResponse<Object> generateSubscriptionResult(NGSILDOperationResult t, Context context) {
		// TODO Auto-generated method stub
		if (!t.getSuccesses().isEmpty())
			return RestResponse.created(URI.create(AppConstants.SUBSCRIPTIONS_URL + t.getEntityId()));
		return null;
	}

	public static String getTriggerReason(int triggerReason) {
		// TODO Cases remaining for upsert(determine created or updated)
		//  and noLongerMatching due to update or delete attr
		return switch (triggerReason) {
			case AppConstants.CREATE_REQUEST -> "newlyMatching";
			case AppConstants.UPDATE_REQUEST,AppConstants.APPEND_REQUEST -> "updated";
			case AppConstants.DELETE_REQUEST -> "noLongerMatching";
			default -> null;
		};
	}

	public static RestResponse<Object> generateQueryResult(HttpServerRequest request, QueryResult queryResult,
			String options, String geometryProperty, int acceptHeader, boolean count, int limit, LanguageQueryTerm lang,
			Context context) {
		ResponseBuilder<Object> builder = RestResponseBuilderImpl.ok();
		if (count == true) {
			builder = builder.header(NGSIConstants.COUNT_HEADER_RESULT, queryResult.getCount());
		}
		if (limit == 0) {
			return builder.build();
		}

		try {
			Tuple2<Object, List<Tuple2<String, String>>> resultAndHeaders = generateCompactedResult(
					getAtContext(request), context, acceptHeader, queryResult.getData(), geometryProperty, options, lang);

			MultiMap urlParams = request.params();
			String nextLink = HttpUtils.generateNextLink(urlParams, queryResult);
			String prevLink = HttpUtils.generatePrevLink(urlParams, queryResult);

			if (nextLink != null) {
				builder = builder.header(HttpHeaders.LINK, nextLink);
			}
			if (prevLink != null) {
				builder = builder.header(HttpHeaders.LINK, prevLink);
			}
			List<Tuple2<String, String>> headers = resultAndHeaders.getItem2();
			for (Tuple2<String, String> entry : headers) {
				builder = builder.header(entry.getItem1(), entry.getItem2());
			}

			return builder.entity(resultAndHeaders.getItem1()).build();

		} catch (Exception e) {
			return handleControllerExceptions(e);
		}
	}

	public static NGSILDOperationResult handleWebResponse(HttpResponse<Buffer> response, Throwable failure,
			Integer[] integers, RemoteHost remoteHost, int operationType, String entityId, Set<Attrib> attrs) {

		NGSILDOperationResult result = new NGSILDOperationResult(operationType, entityId);
		if (failure != null) {
			result.addFailure(new ResponseException(ErrorType.InternalError, failure.getMessage(), remoteHost, attrs));
		} else {
			int statusCode = response.statusCode();
			if (ArrayUtils.contains(integers, statusCode)) {
				result.addSuccess(new CRUDSuccess(remoteHost, attrs));
			} else if (statusCode == 207) {
				JsonObject jsonObj = response.bodyAsJsonObject();
				if (jsonObj != null) {
					NGSILDOperationResult remoteResult;
					try {
						remoteResult = NGSILDOperationResult.getFromPayload(jsonObj.getMap());
					} catch (ResponseException e) {
						result.addFailure(e);
						return result;
					}
					result.getFailures().addAll(remoteResult.getFailures());
					result.getSuccesses().addAll(remoteResult.getSuccesses());
				}

			} else {

				JsonObject responseBody = response.bodyAsJsonObject();
				if (responseBody == null) {
					result.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
							NGSIConstants.ERROR_UNEXPECTED_RESULT_NULL_TITLE, statusCode, remoteHost, attrs));

				} else {
					if (!responseBody.containsKey(NGSIConstants.ERROR_TYPE)
							|| !responseBody.containsKey(NGSIConstants.ERROR_TITLE)
							|| !responseBody.containsKey(NGSIConstants.ERROR_DETAIL)) {
						result.addFailure(
								new ResponseException(statusCode, responseBody.getString(NGSIConstants.ERROR_TYPE),
										responseBody.getString(NGSIConstants.ERROR_TITLE),
										responseBody.getMap().get(NGSIConstants.ERROR_DETAIL), remoteHost, attrs));
					} else {
						result.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
								NGSIConstants.ERROR_UNEXPECTED_RESULT_NOT_EXPECTED_BODY_TITLE, responseBody.getMap(),
								remoteHost, attrs));
					}
				}
			}
		}
		return result;

	}
	public static Object doubleToInt(Object object) {
		if (object instanceof Double) {
			double d = (Double) object;
			int i = (int) d;
			return (d == i)?i:d;
		} else
			return object;
	}
}
