package eu.neclab.ngsildbroker.commons.tools;

import java.io.ByteArrayOutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;

import eu.neclab.ngsildbroker.commons.datatypes.ParsedQueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.NgsiLdOperation;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;

/**
 * Central NGSI-LD aware query-string tokenizer. Works on the raw request
 * target (request.uri()) because the Netty/Vert.x parsers mangle NGSI-LD
 * syntax: they split values at ';', decode '+' to space and truncate the
 * query at a literal '#'. Rules:
 * <ul>
 * <li>pairs split on '&amp;' ONLY, name/value split at the FIRST '='</li>
 * <li>a missing '=' means value "" (?count is count=true for booleans)</li>
 * <li>'#' is ordinary data</li>
 * <li>%-decoding is UTF-8 multi-byte aware; '+' decodes to space exactly
 * like the previous stack did (real-world clients such as python-requests
 * send spaces as '+'). Exception: in scopeQ a raw '+' is the single-level
 * wildcard and stays literal — see ParsedQueryParams.getScopeQ()</li>
 * <li>parameter names not allowed for the operation give 400 InvalidRequest
 * (spec 6.3.20), duplicates give 400 BadRequestData</li>
 * </ul>
 */
public class QueryParamParser {

	private QueryParamParser() {
	}

	public static ParsedQueryParams parse(HttpServerRequest request, NgsiLdOperation operation)
			throws ResponseException {
		return parse(rawQueryOf(request), operation);
	}

	static ParsedQueryParams parse(String rawQuery, NgsiLdOperation operation) throws ResponseException {
		LinkedHashMap<String, String> rawParams = new LinkedHashMap<>();
		LinkedHashMap<String, String> decodedParams = new LinkedHashMap<>();
		if (rawQuery != null && !rawQuery.isEmpty()) {
			int start = 0;
			int length = rawQuery.length();
			while (start < length) {
				int amp = rawQuery.indexOf('&', start);
				int end = amp == -1 ? length : amp;
				if (end > start) {
					String pair = rawQuery.substring(start, end);
					int eq = pair.indexOf('=');
					String rawName = eq == -1 ? pair : pair.substring(0, eq);
					String rawValue = eq == -1 ? "" : pair.substring(eq + 1);
					String name = rfc3986Decode(rawName);
					if (!operation.getAllowedParams().contains(name)) {
						throw new ResponseException(ErrorType.InvalidRequest,
								name + " is not a valid parameter for this operation");
					}
					if (rawParams.containsKey(name)) {
						throw new ResponseException(ErrorType.BadRequestData, "Duplicate query parameter: " + name);
					}
					rawParams.put(name, rawValue);
					decodedParams.put(name, rfc3986Decode(rawValue));
				}
				if (amp == -1) {
					break;
				}
				start = amp + 1;
			}
		}
		return new ParsedQueryParams(rawParams, decodedParams);
	}

	/**
	 * The raw query part of the request target: everything after the first
	 * '?' of request.uri(), never parsed by Netty, '#' included.
	 */
	static String rawQueryOf(HttpServerRequest request) {
		String uri = request.uri();
		if (uri == null) {
			return null;
		}
		int questionMark = uri.indexOf('?');
		if (questionMark == -1) {
			return null;
		}
		return uri.substring(questionMark + 1);
	}

	/**
	 * Percent-decoding: %XX sequences are decoded as UTF-8 (multi-byte
	 * aware), '+' decodes to space (form semantics, matching what the
	 * previous Netty/JAX-RS stack did so no client behavior changes),
	 * everything else passes through literally.
	 */
	public static String rfc3986Decode(String input) throws ResponseException {
		return decode(input, true);
	}

	/**
	 * Decoding variant for scopeQ, where a raw '+' is the single-level
	 * wildcard (grammar, not data) and therefore stays literal. The encoded
	 * forms remain distinguishable: %2B gives a literal '+', %20 a space.
	 */
	public static String rfc3986DecodePlusLiteral(String input) throws ResponseException {
		return decode(input, false);
	}

	private static String decode(String input, boolean plusIsSpace) throws ResponseException {
		if (input == null || (input.indexOf('%') == -1 && input.indexOf('+') == -1)) {
			return input;
		}
		StringBuilder result = new StringBuilder(input.length());
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		int i = 0;
		int length = input.length();
		while (i < length) {
			char c = input.charAt(i);
			if (c == '+') {
				result.append(plusIsSpace ? ' ' : '+');
				i++;
			} else if (c == '%') {
				while (i < length && input.charAt(i) == '%') {
					if (i + 2 >= length) {
						throw new ResponseException(ErrorType.BadRequestData,
								"Malformed percent-encoding in query parameter");
					}
					int high = Character.digit(input.charAt(i + 1), 16);
					int low = Character.digit(input.charAt(i + 2), 16);
					if (high == -1 || low == -1) {
						throw new ResponseException(ErrorType.BadRequestData,
								"Malformed percent-encoding in query parameter");
					}
					bytes.write((high << 4) + low);
					i += 3;
				}
				result.append(new String(bytes.toByteArray(), StandardCharsets.UTF_8));
				bytes.reset();
			} else {
				result.append(c);
				i++;
			}
		}
		return result.toString();
	}

	/**
	 * RFC3986 percent-encoding for values placed into forwarded query strings.
	 */
	public static String rfc3986Encode(String input) {
		return URLEncoder.encode(input, StandardCharsets.UTF_8).replace("+", "%20");
	}

	/**
	 * The raw name/value pairs for echoing into paging Link headers: no
	 * allowlist, no decoding, duplicates kept.
	 */
	public static MultiMap rawParamsForEcho(HttpServerRequest request) {
		MultiMap result = MultiMap.caseInsensitiveMultiMap();
		String rawQuery = rawQueryOf(request);
		if (rawQuery == null || rawQuery.isEmpty()) {
			return result;
		}
		int start = 0;
		int length = rawQuery.length();
		while (start < length) {
			int amp = rawQuery.indexOf('&', start);
			int end = amp == -1 ? length : amp;
			if (end > start) {
				String pair = rawQuery.substring(start, end);
				int eq = pair.indexOf('=');
				String rawName = eq == -1 ? pair : pair.substring(0, eq);
				String rawValue = eq == -1 ? "" : pair.substring(eq + 1);
				result.add(rawName, rawValue);
			}
			if (amp == -1) {
				break;
			}
			start = amp + 1;
		}
		return result;
	}
}
