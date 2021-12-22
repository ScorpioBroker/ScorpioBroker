package eu.neclab.ngsildbroker.commons.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.ResponseEntity.BodyBuilder;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
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
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
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
			"((\\*\\/\\*)|(application\\/\\*)|(application\\/json)|(application\\/ld\\+json)|(application\\/n-quads))(\\s*\\;\\s*q=(\\d(\\.\\d)*))?\\s*\\,?\\s*");

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
		return generateReply(request, reply, null, endPoint);

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

	private static int parseAcceptHeader(List<String> acceptHeaders) {
		float q = 1;
		int appGroup = -1;
		Iterator<String> it = acceptHeaders.iterator();
		while (it.hasNext()) {
			String header = it.next();

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
		case 5:
			return 2; // application/ld+json
		case 2:
		case 3:
		case 4:
			return 1; // application/json
		case 6:
			return 3;// application/n-quads
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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static ResponseEntity<String> generateReply(HttpServletRequest request, String reply,
			ArrayListMultimap<String, String> additionalHeaders, Context ldContext, List<Object> contextLinks,
			boolean forceArrayResult, int endPoint) throws ResponseException {

		String replyBody;

		// CompactedJson compacted = ContextResolverBasic.compact(reply,
		// requestAtContext);
		Map<String, Object> compacted;
		try {
			compacted = JsonLdProcessor.compact(JsonUtils.fromString(reply), contextLinks, ldContext, opts, endPoint);
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
			int sendingContentType = parseAcceptHeader(Collections.list(request.getHeaders(HttpHeaders.ACCEPT)));
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
				for (Object entry : contextLinks) {
					if (entry instanceof String) {
						additionalHeaders.put(HttpHeaders.LINK, "<" + entry
								+ ">; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
					} else {
						additionalHeaders.put(HttpHeaders.LINK, "<" + getAtContextServing(entry)
								+ ">; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
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
				replyBody = RDFDatasetUtils.toNQuads((RDFDataset) JsonLdProcessor.toRDF(JsonUtils.fromString(reply)));
				break;
			case -1:
			default:
				throw new ResponseException(ErrorType.NotAcceptable, "Provided accept types are not supported");
			}

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

	private static String getAtContextServing(Object entry) {
		// TODO Auto-generated method stub
		return "http change this";
	}

	public static ResponseEntity<String> generateReply(String replyBody,
			ArrayListMultimap<String, String> additionalHeaders, boolean compress) {
		return generateReply(replyBody, additionalHeaders, HttpStatus.OK, compress);
	}

	public static ResponseEntity<String> generateReply(HttpServletRequest request, QueryResult qResult,
			boolean forceArray, boolean count, Context context, List<Object> contextLinks, int endPoint) throws ResponseException {
		String nextLink = HttpUtils.generateNextLink(request, qResult);
		String prevLink = HttpUtils.generatePrevLink(request, qResult);
		ArrayList<Object> additionalLinks = new ArrayList<Object>();
		if (nextLink != null) {
			additionalLinks.add(nextLink);
		}
		if (prevLink != null) {
			additionalLinks.add(prevLink);
		}
		ArrayListMultimap<String, String> additionalHeaders = ArrayListMultimap.create();
		if (count == true) {
			additionalHeaders.put(NGSIConstants.COUNT_HEADER_RESULT, String.valueOf(qResult.getCount()));
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
			for (Entry<String, String> entry : additionalHeaders.entries()) {
				builder.header(entry.getKey(), entry.getValue());
			}
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

	public static String generateNextLink(HttpServletRequest request, QueryResult qResult) {
		if (qResult.getResultsLeftAfter() == null || qResult.getResultsLeftAfter() <= 0) {
			return null;
		}
		return generateFollowUpLinkHeader(request, qResult.getOffset() + qResult.getLimit(), qResult.getLimit(),
				qResult.getqToken(), "next");
	}

	public static String generateFollowUpLinkHeader(HttpServletRequest request, int offset, int limit, String token,
			String rel) {

		StringBuilder builder = new StringBuilder("</");
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
		builder.append("limit=" + limit + "&");
		builder.append("qtoken=" + token + ">;rel=\"" + rel + "\"");
		return builder.toString();
	}

	public static String generatePrevLink(HttpServletRequest request, QueryResult qResult) {
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
					.body(new RestResponse(responseException).toJson());
		}
		if (e instanceof DateTimeParseException) {
			logger.debug("Exception :: ", e);
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
					new RestResponse(ErrorType.InvalidRequest, "Failed to parse provided datetime field.").toJson());
		}
		if (e instanceof JsonProcessingException) {
			logger.debug("Exception :: ", e);
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.InvalidRequest, "There is an error in the provided json document")
							.toJson());
		}
		logger.error("Exception :: ", e);
		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
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
		LinkedMultiValueMap<String, String> result = new LinkedMultiValueMap<String, String>(paramMap.size());
		for (Entry<String, String[]> entry : paramMap.entrySet()) {
			result.addAll(entry.getKey(), Arrays.asList(entry.getValue()));
		}
		return result;
	}
}
