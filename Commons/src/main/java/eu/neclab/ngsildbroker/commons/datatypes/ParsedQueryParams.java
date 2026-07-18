package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParamParser;

/**
 * The query parameters of one request as produced by
 * {@link eu.neclab.ngsildbroker.commons.tools.QueryParamParser}. Keeps both
 * the RFC3986-decoded values (for local processing) and the raw still-encoded
 * values (for forwarding/echoing), both in request order.
 */
public class ParsedQueryParams {

	private final LinkedHashMap<String, String> rawParams;
	private final LinkedHashMap<String, String> decodedParams;

	public ParsedQueryParams(LinkedHashMap<String, String> rawParams, LinkedHashMap<String, String> decodedParams) {
		this.rawParams = rawParams;
		this.decodedParams = decodedParams;
	}

	public boolean has(String name) {
		return decodedParams.containsKey(name);
	}

	public String getString(String name) {
		return decodedParams.get(name);
	}

	public String getString(String name, String defaultValue) {
		return decodedParams.getOrDefault(name, defaultValue);
	}

	/**
	 * Boolean with the HttpUtils.parseBoolean semantics: absent -> false,
	 * valueless/empty -> true, otherwise only "true"/"false" are accepted.
	 */
	public boolean getBoolean(String name) throws ResponseException {
		return HttpUtils.parseBoolean(decodedParams.get(name));
	}

	public boolean getBoolean(String name, boolean defaultValue) throws ResponseException {
		String value = decodedParams.get(name);
		if (value == null) {
			return defaultValue;
		}
		return HttpUtils.parseBoolean(value);
	}

	/**
	 * Absent or empty -> null (matches JAX-RS Integer binding), non-numeric ->
	 * 400 BadRequestData.
	 */
	public Integer getInteger(String name) throws ResponseException {
		String value = decodedParams.get(name);
		if (value == null || value.isEmpty()) {
			return null;
		}
		try {
			return Integer.valueOf(value);
		} catch (NumberFormatException e) {
			throw new ResponseException(ErrorType.BadRequestData, name + " must be an integer number");
		}
	}

	public int getInt(String name, int defaultValue) throws ResponseException {
		Integer value = getInteger(name);
		return value == null ? defaultValue : value;
	}

	/**
	 * q with the double quotes stripped like the old raw-URI slicing did.
	 */
	public String getQ() {
		String q = decodedParams.get(NGSIConstants.QUERY_PARAMETER_QUERY);
		if (q == null) {
			return null;
		}
		return q.replaceAll("\"", "");
	}

	/**
	 * The intact georel=near;maxDistance==X form is preferred; when
	 * maxDistance/minDistance arrive as separate parameters they are rejoined
	 * into the georel string the way QueryParser expects it.
	 */
	public String getGeorel() {
		String georel = decodedParams.get(NGSIConstants.QUERY_PARAMETER_GEOREL);
		String maxDistance = decodedParams.get(NGSIConstants.QUERY_PARAMETER_MAX_DISTANCE);
		String minDistance = decodedParams.get(NGSIConstants.QUERY_PARAMETER_MIN_DISTANCE);
		if (maxDistance != null) {
			return georel + ";" + NGSIConstants.QUERY_PARAMETER_MAX_DISTANCE + "==" + stripLeadingEquals(maxDistance);
		} else if (minDistance != null) {
			return georel + ";" + NGSIConstants.QUERY_PARAMETER_MIN_DISTANCE + "==" + stripLeadingEquals(minDistance);
		}
		return georel;
	}

	private static String stripLeadingEquals(String value) {
		if (value.startsWith("=")) {
			return value.substring(1);
		}
		return value;
	}

	/**
	 * scopeQ decoded from the raw wire value with '+' kept LITERAL: '+' is
	 * the scope single-level wildcard (grammar, not data), unlike every other
	 * parameter where '+' means space. %20 still decodes to a space and %2B
	 * to a literal '+', so the encoded forms stay distinguishable.
	 */
	public String getScopeQ() throws ResponseException {
		return QueryParamParser
				.rfc3986DecodePlusLiteral(rawParams.get(NGSIConstants.QUERY_PARAMETER_SCOPE_QUERY));
	}

	/**
	 * "local" with the legacy "localOnly" alias as fallback.
	 */
	public boolean getLocal() throws ResponseException {
		String value = decodedParams.get(NGSIConstants.QUERY_PARAMETER_LOCAL_ONLY);
		if (value == null) {
			value = decodedParams.get(NGSIConstants.QUERY_PARAMETER_LOCAL_ONLY_LEGACY);
		}
		return HttpUtils.parseBoolean(value);
	}

	/**
	 * The merged options/format set via HttpUtils.parseOptionsAndFormat.
	 */
	public Set<String> getFinalOptions() throws ResponseException {
		return HttpUtils.parseOptionsAndFormat(decodedParams.get(NGSIConstants.QUERY_PARAMETER_OPTIONS),
				decodedParams.get(NGSIConstants.QUERY_PARAMETER_FORMAT));
	}

	/**
	 * Mutable copy of the raw (still percent-encoded) name/value pairs in
	 * request order, for rebuilding forwarded queries.
	 */
	public LinkedHashMap<String, String> getRawParams() {
		return new LinkedHashMap<>(rawParams);
	}

	public String toRawQueryString() {
		StringBuilder result = new StringBuilder();
		for (Map.Entry<String, String> entry : rawParams.entrySet()) {
			if (!result.isEmpty()) {
				result.append('&');
			}
			result.append(entry.getKey()).append('=').append(entry.getValue());
		}
		return result.toString();
	}
}
