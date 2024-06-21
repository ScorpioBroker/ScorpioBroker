package eu.neclab.ngsildbroker.commons.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
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
import eu.neclab.ngsildbroker.commons.exceptions.LdContextException;
import io.vertx.core.json.DecodeException;
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
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.RDFDataset;
import com.github.jsonldjava.core.RDFDatasetUtils;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import io.smallrye.mutiny.tuples.Tuple3;
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

	private static Set<String> DO_NOT_SCAN_ATTRIBS = Sets.newHashSet("id", "type", "createdAt", "scope", "@context");

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
			throws ResponseException {
		Map<String, Object> resultMap = Maps.newLinkedHashMap();
		if (result instanceof List) {
			resultMap.put(NGSIConstants.TYPE, NGSIConstants.FEATURE_COLLECTION);
			ArrayList<Object> value = new ArrayList<Object>();
			try {
				for (Object entry : (List<Object>) result) {
					Object valueEntry = generateGeoJson(entry, geometry, context);
					((Map<String, Object>) valueEntry).remove(NGSIConstants.JSON_LD_CONTEXT);
					value.add(valueEntry);
				}
			}catch (ResponseException e){
				logger.debug("exception in generateGeoJson method from HttpUtils ",e);
			}

			resultMap.put(NGSIConstants.FEATURES, value);
			resultMap.put(NGSIConstants.JSON_LD_CONTEXT, context);
		} else {
			Map<String, Object> entryMap = (Map<String, Object>) result;
			resultMap.put(NGSIConstants.QUERY_PARAMETER_ID, entryMap.remove(NGSIConstants.QUERY_PARAMETER_ID));
			resultMap.put(NGSIConstants.TYPE, NGSIConstants.FEATURE);
			if (geometry == null) {
				geometry = NGSIConstants.NGSI_LD_LOCATION_SHORT;
			}
			Object geometryEntry = entryMap.get(geometry);
			if (geometryEntry != null) {
				resultMap.put(NGSIConstants.GEOMETRY, ((Map<String, Object>) geometryEntry).get(NGSIConstants.VALUE));
			}else {
				throw new ResponseException(ErrorType.NotAcceptable,"Geo json not support for this request");
			}
			entryMap.remove(NGSIConstants.JSON_LD_CONTEXT);
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
		builder.append("offset=" + offset);
		builder.append("&limit=" + limit);
		builder.append("&entityMap=" + token);
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
		if (e instanceof ResponseException responseException) {
			logger.debug("Exception :: ", responseException);
			return RestResponseBuilderImpl.create(responseException.getErrorCode())
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.entity(responseException.getJson()).build();
		}
		if (e instanceof LdContextException ldContextException) {
			logger.debug("Exception :: ", ldContextException);
			return RestResponseBuilderImpl.create(ErrorType.LdContextNotAvailable.getCode())
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.entity(new ResponseException(ErrorType.LdContextNotAvailable).getJson()).build();
		}
		if (e instanceof DateTimeParseException) {
			logger.debug("Exception :: ", e);
			return RestResponseBuilderImpl.create(HttpStatus.SC_BAD_REQUEST)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.entity(new ResponseException(ErrorType.BadRequestData, "Failed to parse provided datetime field.")
							.getJson())
					.build();
		}
		if (e instanceof JsonProcessingException || e instanceof JsonLdError || e instanceof DecodeException) {
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
	public static Uni<MultiMap> getAdditionalHeaders(Map<String, Object> registration, List<Object> context,
													 List<String> accept, JsonLDService ldService) {
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

			return ldService.compact(receiverInfo, context, opts).onItem().transform(headerMap -> {
				headerMap.remove(NGSIConstants.JSON_LD_CONTEXT);
				for (Entry<String, Object> entry : headerMap.entrySet()) {
					result.add(entry.getKey(), entry.getValue().toString());
				}
				return result;
			});

		}
		return Uni.createFrom().item(result);
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
				result.add(
						obj.getJsonArray(NGSIConstants.NGSI_LD_HAS_KEY).getJsonObject(0).getString(JsonLdConsts.VALUE),
						obj.getJsonArray(NGSIConstants.NGSI_LD_HAS_VALUE).getJsonObject(0)
								.getString(JsonLdConsts.VALUE));
			});
		}
		if (result.contains(NGSIConstants.JSONLD_CONTEXT)) {
			String linkHeader = "<%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"',"
					.formatted(result.get(NGSIConstants.JSONLD_CONTEXT));
			result.remove(NGSIConstants.JSONLD_CONTEXT).add("Link", linkHeader);
		}
		if(!result.contains("Accept")) {
			result.add("Accept", "application/json");
		}
		if (tenant != null) {
			result.add(NGSIConstants.TENANT_HEADER, tenant);
		}
		return result;
	}

	public static MultiMap getHeadersForRemoteCallFromRegUpdate(List<Map<String, Object>> headerFromReg,
																String tenant) {
		MultiMap result = HeadersMultiMap.headers();
		if (headerFromReg != null) {
			headerFromReg.forEach(t -> {
				JsonObject obj = new JsonObject(t);
				result.add(
						obj.getJsonArray(NGSIConstants.NGSI_LD_HAS_KEY).getJsonObject(0).getString(JsonLdConsts.VALUE),
						obj.getJsonArray(NGSIConstants.NGSI_LD_HAS_VALUE).getJsonObject(0)
								.getString(JsonLdConsts.VALUE));
			});
		}
		if (result.contains(NGSIConstants.JSONLD_CONTEXT)) {
			String linkHeader = "<%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"',"
					.formatted(result.get(NGSIConstants.JSONLD_CONTEXT));
			result.remove(NGSIConstants.JSONLD_CONTEXT).add("Link", linkHeader);
		}
		result.add("Accept", "application/json");
		if (!tenant.equals(AppConstants.INTERNAL_NULL_KEY)) {
			result.add(NGSIConstants.TENANT_HEADER, tenant);
		}
		return result;
	}
	public static Uni<RestResponse<Object>> generateEntityResult(List<Object> contextHeader, Context context,
																 int acceptHeader, Object entity, String geometryProperty, String options, LanguageQueryTerm langQuery,
																 JsonLDService ldService,List<String> omitList,List<String> pickList) {
		return generateEntityResult(contextHeader, context,acceptHeader, entity, geometryProperty, options, langQuery, ldService,omitList, pickList, false);
	}
	public static Uni<RestResponse<Object>> generateEntityResult(List<Object> contextHeader, Context context,
																 int acceptHeader, Object entity, String geometryProperty, String options, LanguageQueryTerm langQuery,
																 JsonLDService ldService,List<String> omitList,List<String> pickList,boolean forceList) {
		return generateCompactedResult(contextHeader, context, acceptHeader, entity, geometryProperty, options,
				langQuery, false, ldService).onItem().transform(resultBodyAndHeaders -> {
			ResponseBuilder<Object> resp = RestResponseBuilderImpl.ok();
			List<Tuple2<String, String>> headers = resultBodyAndHeaders.getItem2();
			for (Tuple2<String, String> entry : headers) {
				resp = resp.header(entry.getItem1(), entry.getItem2());
			}
			Object result = processPickOmit(resultBodyAndHeaders.getItem1(),pickList,omitList);
			if (forceList){
				forceList(result);
			}
			return resp.entity(result).build();
		});
	}
	public static Object processPickOmit(Object object,List<String> pickList, List<String> omitList){
		try {
			JsonObject jsonObject = new JsonObject(object.toString());
			if (omitList != null && !omitList.isEmpty()) {
				for (String key : omitList) {
					jsonObject.remove(key);
				}
			}
			if (pickList!= null && !pickList.isEmpty()) {
				JsonObject finalJsonObject = new JsonObject();
				for (String key : pickList) {
					Object value = jsonObject.getValue(key);
					if (value != null) {
						finalJsonObject.put(key, value);
					}
				}
				return finalJsonObject;
			}
			else return jsonObject;
		} catch (DecodeException decodeException) {
			JsonArray jsonArray = new JsonArray(object.toString());
			for (Object jsonObject : jsonArray) {
				if (jsonObject instanceof JsonObject) {
					if (omitList != null && !omitList.isEmpty()) {
						for (String key : omitList) {
							((JsonObject) jsonObject).remove(key);
						}
					}
				}
			}
			if (pickList!= null && !pickList.isEmpty()) {
				JsonArray finalJsonArray = new JsonArray();
				for (Object jsonObject : jsonArray) {
					if (jsonObject instanceof JsonObject) {
						JsonObject finalJsonObject = new JsonObject();
						for (String key : pickList) {
							Object value = ((JsonObject) jsonObject).getValue(key);
							if (value != null) {
								finalJsonObject.put(key, value);
							}
						}
						finalJsonArray.add(finalJsonObject);
					}
				}
				return finalJsonArray;
			} else return jsonArray;
		}
	}
	public static void makeConcise(Object compacted) {
		makeConcise(compacted, null, null);
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
			for (Object k : map.keySet()) {
				if (!k.equals(NGSIConstants.JSON_LD_CONTEXT) && map.get(k) instanceof Map<?, ?>
						|| map.get(k) instanceof ArrayList<?>) {
					makeConcise(map.get(k), map, k.toString());
				}
			}
		}

	}

	public static Uni<Tuple2<Object, List<Tuple2<String, String>>>> generateCompactedResult(List<Object> contextHeader,
																							Context context, int acceptHeader, Object entity, String geometryProperty, String options,
																							LanguageQueryTerm langQuery, boolean forceArray, JsonLDService ldService) {

		Set<String> optionSet = null;
		if (options != null) {
			optionSet = Set.of(options.split(","));
		}

		Uni<Tuple3<String, String, List<Tuple2<String, String>>>> uni;
		switch (acceptHeader) {

			case 1:
				uni = ldService.compact(entity, contextHeader, context, opts, -1, optionSet, langQuery).onItem()
						.transformToUni(compacted -> {
							List<Tuple2<String, String>> headers = Lists.newArrayList();
							Object bodyContext = compacted.remove(NGSIConstants.JSON_LD_CONTEXT);
							Object finalCompacted;
							if (contextHeader.isEmpty()) {
								contextHeader.add(((List<Object>) bodyContext).get(0));
							}
                            finalCompacted = compacted.getOrDefault(JsonLdConsts.GRAPH, compacted);
							if (options != null && options.contains(NGSIConstants.QUERY_PARAMETER_CONCISE_VALUE)) {
								makeConcise(finalCompacted);
							}
							if (forceArray && !(finalCompacted instanceof List)) {
								finalCompacted = List.of(finalCompacted);
							}

							for (Object entry : contextHeader) {
								headers.add(Tuple2.of(NGSIConstants.LINK_HEADER, getLinkHeader(entry)));
							}

							try {
								return Uni.createFrom().item(Tuple3.of(JsonUtils.toPrettyString(finalCompacted),
										AppConstants.NGB_APPLICATION_JSON, headers));
							} catch (IOException e) {
								return Uni.createFrom().failure(e);
							}
						});
				break;
			case 2:
				uni = ldService.compact(entity, contextHeader, context, opts, -1, optionSet, langQuery).onItem()
						.transformToUni(compacted -> {
							Object finalCompacted;
							if (compacted.containsKey(JsonLdConsts.GRAPH)) {
								finalCompacted = compacted.get(JsonLdConsts.GRAPH);
								Object bodyContext = compacted.get(NGSIConstants.JSON_LD_CONTEXT);
								if (finalCompacted instanceof List) {
									List<Map<String, Object>> tmpList = (List<Map<String, Object>>) finalCompacted;
									for (Map<String, Object> entry : tmpList) {
										entry.put(NGSIConstants.JSON_LD_CONTEXT, bodyContext);
									}
								} else if (finalCompacted instanceof Map) {
									((Map<String, Object>) finalCompacted).put(NGSIConstants.JSON_LD_CONTEXT,
											bodyContext);
								}
							} else {
								finalCompacted = compacted;
							}
							if (options != null && options.contains(NGSIConstants.QUERY_PARAMETER_CONCISE_VALUE)) {
								makeConcise(finalCompacted);
							}
							if (forceArray && !(finalCompacted instanceof List)) {
								finalCompacted = List.of(finalCompacted);
							}

							try {
								return Uni.createFrom().item(Tuple3.of(JsonUtils.toPrettyString(finalCompacted),
										AppConstants.NGB_APPLICATION_JSONLD, null));
							} catch (IOException e) {
								return Uni.createFrom().failure(e);
							}
						});
				break;
			case 3:
				uni = ldService.toRDF(entity).onItem().transform(rdf -> {
					return Tuple3.of(RDFDatasetUtils.toNQuads((RDFDataset) rdf), AppConstants.NGB_APPLICATION_NQUADS,
							null);
				});
				break;
			case 4:// geo+json
				uni = ldService.compact(entity, contextHeader, context, opts, -1, optionSet, langQuery).onItem()
						.transformToUni(compacted -> {
							Object finalCompacted = compacted;
							if (compacted.containsKey(JsonLdConsts.GRAPH)) {
								finalCompacted = compacted.get(JsonLdConsts.GRAPH);
								Object bodyContext = compacted.get(NGSIConstants.JSON_LD_CONTEXT);
								if (finalCompacted instanceof List) {
									List<Map<String, Object>> tmpList = (List<Map<String, Object>>) finalCompacted;
									for (Map<String, Object> entry : tmpList) {
										entry.put(NGSIConstants.JSON_LD_CONTEXT, bodyContext);
									}
								} else if (finalCompacted instanceof Map) {
									((Map<String, Object>) finalCompacted).put(NGSIConstants.JSON_LD_CONTEXT,
											bodyContext);
								}
							}
							if (options != null && options.contains(NGSIConstants.QUERY_PARAMETER_CONCISE_VALUE)) {
								makeConcise(finalCompacted);
							}
							if (forceArray && !(finalCompacted instanceof List)) {
								finalCompacted = List.of(finalCompacted);
							}
							try {
								return Uni.createFrom().item(Tuple3.of(
										JsonUtils.toPrettyString(
												generateGeoJson(finalCompacted, geometryProperty, contextHeader)),
										AppConstants.NGB_APPLICATION_GEO_JSON, null));
							} catch (Exception e) {
								return Uni.createFrom().failure(e);
							}
						});
				break;
			default:
				return Uni.createFrom().nullItem();
		}
		return uni.onItem().transform(tuple -> {
			String replyBody = tuple.getItem1();
			String contentType = tuple.getItem2();
			List<Tuple2<String, String>> headers = tuple.getItem3();
			if (headers == null) {
				headers = Lists.newArrayList();
			}
			Object result;
			if (options != null && options.contains("compress")) {
				result = zipResult(replyBody);
				contentType = AppConstants.NGB_APPLICATION_ZIP;
			} else {
				result = replyBody;
			}
			headers.add(Tuple2.of(HttpHeaders.CONTENT_TYPE, contentType));

			return Tuple2.of(result, headers);
		});
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
			Object attribObj = entry.getValue();
			if (attribObj instanceof Map) {
				map = (Map<String, Object>) entry.getValue();
				if (map.containsKey("datasetId")) {
					datasetId = (String) map.get("datasetId");
				}
				result.add(new Attrib(entry.getKey(), datasetId));
			} else if (attribObj instanceof List) {
				List<Map<String, Object>> list = (List<Map<String, Object>>) attribObj;
				for (Map<String, Object> attribEntry : list) {
					if (attribEntry.containsKey("datasetId")) {
						datasetId = (String) attribEntry.get("datasetId");
					}
					result.add(new Attrib(entry.getKey(), datasetId));
				}

			}

		}
		return result;
	}

	public static RestResponse<Object> generateBatchResult(List<NGSILDOperationResult> t) {
		boolean isHavingError = false;
		boolean isHavingSuccess = false;
		boolean wasUpdated = true;
		boolean allConflict = true;
		String opType = (String) t.get(0).getJson().get("type");
		List<String> createdIds = new ArrayList<>();
		List<String> successes = new ArrayList<>();
		List<Map<String, Object>> errors = new ArrayList<>();
		Map<String, Object>result = new HashMap<>();
		result.put("success",successes);
		result.put("errors",errors);
		for (NGSILDOperationResult r : t) {
			if (!r.getFailures().isEmpty()) {
				Map<String,Object> error = r.getJson();
				Map<String,Object> failure = ((List<Map<String, Object>>)error.get("failure")).get(0);
				error.put("ProblemDetails",failure);
				error.remove("failure");
				errors.add(error);
				allConflict = allConflict && failure.get(NGSIConstants.STATUS).equals(409);
				isHavingError = true;
			}
			if (!r.getSuccesses().isEmpty()) {
				if(!r.isWasUpdated()){
					createdIds.add(r.getEntityId());
				}
				successes.add(r.getEntityId());
				isHavingSuccess = true;
			}
			wasUpdated = wasUpdated && r.isWasUpdated();
		}
		if (isHavingError && !isHavingSuccess) {
			result.remove("success");
			if (opType.equalsIgnoreCase("Delete") || opType.equalsIgnoreCase("Append")) {
				return new RestResponseBuilderImpl<>().status(404).type(AppConstants.NGB_APPLICATION_JSON).entity(result)
						.build();
			}
			else if (errors.toString().contains("503")) {
				return new RestResponseBuilderImpl<>().status(503).type(AppConstants.NGB_APPLICATION_JSON).entity(result)
						.build();
			}
			else if (allConflict) {
				return new RestResponseBuilderImpl<>().status(409).type(AppConstants.NGB_APPLICATION_JSON).entity(result)
						.build();
			}
			else {
				return new RestResponseBuilderImpl<>().status(400).type(AppConstants.NGB_APPLICATION_JSON).entity(result)
						.build();
			}
		}
		if (!isHavingError && isHavingSuccess) {
			if ((opType.equalsIgnoreCase("Upsert") && wasUpdated)  || opType.equalsIgnoreCase("Merge")  || opType.equalsIgnoreCase("Delete")
					|| opType.equalsIgnoreCase("Append"))
				return RestResponse.status(RestResponse.Status.NO_CONTENT);
			else
				return new RestResponseBuilderImpl<>().status(201).type(AppConstants.NGB_APPLICATION_JSON).entity(createdIds)
						.build();
		}
		return new RestResponseBuilderImpl<>().status(207).type(AppConstants.NGB_APPLICATION_JSON).entity(result)
				.build();
	}

	public static Uni<Context> getContextFromPayload(Map<String, Object> originalPayload, List<Object> atContextHeader,
													 boolean atContextAllowed, JsonLDService ldService) {

		Object payloadAtContext = originalPayload.get(NGSIConstants.JSON_LD_CONTEXT);
		if (payloadAtContext == null) {
			if (atContextAllowed) {
				return Uni.createFrom()
						.failure(new ResponseException(ErrorType.BadRequestData, "@Context entry is needed"));
			}
			if (atContextHeader.isEmpty()) {
				return Uni.createFrom().item(ldService.getCoreContext());
			} else {
				return ldService.parse(atContextHeader);
			}

		} else {
			if (!atContextAllowed) {
				return Uni.createFrom().failure(
						new ResponseException(ErrorType.BadRequestData, "@context entry in body is not allowed"));
			}
			return ldService.parse(payloadAtContext);

		}
	}

	public static RestResponse<Object> generateDeleteResult(NGSILDOperationResult result) {
		// TODO Auto-generated method stub
		return null;
	}

	public static Uni<Tuple2<Context, Map<String, Object>>> expandBody(HttpServerRequest request, String payload,
																	   int payloadType, JsonLDService ldService) {

		if (payload == null || payload.isEmpty()) {
			return Uni.createFrom()
					.failure(new ResponseException(ErrorType.BadRequestData, "You have to provide a valid payload"));
		}
		return JsonUtils.fromString(payload).onItem().transformToUni(json -> {
			Map<String, Object> originalPayload= new HashMap<>();
			if(json instanceof Map) {
				originalPayload = (Map<String, Object>) json;
			}else {
				return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData));
			}
			return expandBody(request, originalPayload, payloadType, ldService);

		});
	}

	public static Uni<Tuple2<Context, Map<String, Object>>> expandBody(HttpServerRequest request,
																	   Map<String, Object> originalPayload, int payloadType, JsonLDService ldService) {
		boolean atContextAllowed;
		List<Object> atContext = getAtContext(request);
		if(originalPayload==null){
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,"body can not be empty"));
		}
		if(originalPayload.toString().contains(NGSIConstants.VALUE + "=null")
				|| originalPayload.toString().contains(NGSIConstants.TYPE + "=null")){
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData));
		}
		if(originalPayload.containsKey(NGSIConstants.SCOPE) && !(originalPayload.get(NGSIConstants.SCOPE) instanceof String
				|| originalPayload.get(NGSIConstants.SCOPE) instanceof List)){
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,"scope is invalid"));
		}
		try{
			if(originalPayload.get(NGSIConstants.SCOPE) instanceof List) {
				String scope = ((List<String>) originalPayload.get(NGSIConstants.SCOPE)).get(0);
			}
		}catch(Exception e){
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,"scope is invalid"));
		}
		try {
			atContextAllowed = HttpUtils.doPreflightCheck(request, atContext);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		return HttpUtils.getContextFromPayload(originalPayload, atContext, atContextAllowed, ldService).onItem()
				.transformToUni(context -> {
					return ldService.expand(context, originalPayload, opts, payloadType, atContextAllowed).onItem()
							.transform(list -> {

								Map<String, Object> resolved = (Map<String, Object>) list.get(0);
								return Tuple2.of(context, resolved);
							});

				});

	}

	public static RestResponse<Object> generateCreateResult(NGSILDOperationResult operationResult, String baseUrl) {
		List<ResponseException> fails = operationResult.getFailures();
		List<CRUDSuccess> successes = operationResult.getSuccesses();
		RestResponse<Object> response;
        successes.removeIf(crudSuccess -> crudSuccess.getAttribs().isEmpty());
		if (fails.isEmpty()) {
			if (!operationResult.isWasUpdated()) {
				try {
					response = RestResponse.created(new URI(baseUrl + operationResult.getEntityId()));
				} catch (URISyntaxException e) {
					response = HttpUtils.handleControllerExceptions(e);
				}
			} else {
				response = RestResponse.noContent();
			}
		} else if (successes.isEmpty() && fails.size() == 1) {
			response = HttpUtils.handleControllerExceptions(fails.get(0));
		} else {
			try {
				response = new RestResponseBuilderImpl<Object>().status(207).type(AppConstants.NGB_APPLICATION_JSON)
						.entity(JsonUtils.toPrettyString(operationResult.getJson())).build();
			} catch (Exception e) {
				response = HttpUtils.handleControllerExceptions(e);
			}
		}
		logger.debug("sending restresponse");
		return response;
	}

	public static List<Object> getContextFromHeader(io.vertx.mutiny.core.MultiMap remoteHeaders) {
		String link = remoteHeaders.get("Link");
		if (link == null) {
			return Lists.newArrayList();
		}
		String tmp = link.split(";")[0];
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
		// and noLongerMatching due to update or delete attr
		return switch (triggerReason) {
			case AppConstants.CREATE_REQUEST -> "newlyMatching";
			case AppConstants.UPDATE_REQUEST, AppConstants.APPEND_REQUEST -> "updated";
			case AppConstants.DELETE_REQUEST -> "noLongerMatching";
			default -> null;
		};
	}
	public static Uni<RestResponse<Object>> generateQueryResult(HttpServerRequest request, QueryResult queryResult,
																String options, String geometryProperty, int acceptHeader, boolean count, int limit, LanguageQueryTerm lang,
																Context context, JsonLDService ldService,List<String> omitList,List<String> pickList) {
		return generateQueryResult(request,queryResult, options, geometryProperty, acceptHeader, count, limit, lang, context, ldService, omitList, pickList,false);
	}
	public static Uni<RestResponse<Object>> generateQueryResult(HttpServerRequest request, QueryResult queryResult,
																String options, String geometryProperty, int acceptHeader, boolean count, int limit, LanguageQueryTerm lang,
																Context context, JsonLDService ldService,List<String> omitList,List<String> pickList,boolean forceList) {
		ResponseBuilder<Object> builder;
		if (count) {
			builder = RestResponseBuilderImpl.ok().header(NGSIConstants.COUNT_HEADER_RESULT, queryResult.getCount());
		} else {
			builder = RestResponseBuilderImpl.ok();
		}
		if (limit == 0) {
			return Uni.createFrom().item(builder.build());
		}
		List<Object> atContext = request == null ? Lists.newArrayList() : getAtContext(request);
		return generateCompactedResult(atContext, context, acceptHeader, queryResult.getData(), geometryProperty,
				options, lang, true, ldService).onItem().transform(resultAndHeaders -> {
			String nextLink;
			String prevLink;
			if (request != null) {
				MultiMap urlParams = request.params();
				nextLink = HttpUtils.generateNextLink(urlParams, queryResult);
				prevLink = HttpUtils.generatePrevLink(urlParams, queryResult);
			} else {
				prevLink = null;
				nextLink = null;
			}
			ResponseBuilder<Object> myBuilder = builder.header(NGSIConstants.ENTITY_MAP_TOKEN_HEADER,
					queryResult.getqToken());

			if (nextLink != null) {
				myBuilder = myBuilder.header(HttpHeaders.LINK, nextLink);
			}
			if (prevLink != null) {
				myBuilder = myBuilder.header(HttpHeaders.LINK, prevLink);
			}
			List<Tuple2<String, String>> headers = resultAndHeaders.getItem2();
			for (Tuple2<String, String> entry : headers) {
				myBuilder = myBuilder.header(entry.getItem1(), entry.getItem2());
			}
			Object result = processPickOmit(resultAndHeaders.getItem1(),pickList,omitList);
			if(forceList){
				forceList(result);
			}
			return myBuilder.entity(result).build();
		});

	}
	public static void forceList(Object object){
		if(object instanceof JsonArray jsonArray){
			jsonArray.forEach(item -> {
				if (item instanceof JsonObject jsonObject){
					makeList(jsonObject);
				}
			});
		}
		else if (object instanceof JsonObject jsonObject) {
			makeList(jsonObject);
		}
	}
	public static void makeList(JsonObject jsonObject){

		for(String key: jsonObject.fieldNames()){
			if(!key.equals(NGSIConstants.JSON_LD_CONTEXT) && !key.equals(NGSIConstants.ID) && !key.equals(NGSIConstants.TYPE) && !key.equals(NGSIConstants.CREATEDAT) && !key.equals(NGSIConstants.QUERY_PARAMETER_MODIFIED_AT) && !key.equals(NGSIConstants.SCOPE) && !((jsonObject.getValue(key) instanceof List) || (jsonObject.getValue(key) instanceof JsonArray))){
				Object tmp = jsonObject.getValue(key);
				jsonObject.put(key, JsonArray.of(tmp));
			}
		}
	}
	public static NGSILDOperationResult handleWebResponse(HttpResponse<Buffer> response, Throwable failure,
														  Integer[] integers, RemoteHost remoteHost, int operationType, String entityId, Set<Attrib> attrs) {

		NGSILDOperationResult result = new NGSILDOperationResult(operationType, entityId);
		if (failure != null) {
			result.addFailure(new ResponseException(ErrorType.UnprocessableContextSourceRegistration,
					failure.getMessage(), remoteHost, attrs));
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
			return (d == i) ? i : d;
		} else
			return object;
	}

	public static Uni<Context> getContext(List<Object> headerContext, JsonLDService ldService) {
		Uni<Context> context;
		if (headerContext.isEmpty()) {
			context = Uni.createFrom().item(ldService.getCoreContext());
		} else {
			context = ldService.parse(headerContext);
		}
		return context;
	}
	public static io.vertx.mutiny.core.MultiMap getHeadToFrwd(io.vertx.mutiny.core.MultiMap remoteHeaders, MultiMap headersFromReq) {
		io.vertx.mutiny.core.MultiMap toFrwd = io.vertx.mutiny.core.MultiMap.newInstance(HeadersMultiMap.headers());
		for(Entry<String, String> entry: remoteHeaders.entries()){
			if(entry.getValue().equals("urn:ngsi-ld:request")){
				toFrwd.add(entry.getKey(), headersFromReq.get(entry.getKey()));
			}else{
				toFrwd.add(entry.getKey(), entry.getValue());
			}
		}
        return toFrwd;
    }
}
