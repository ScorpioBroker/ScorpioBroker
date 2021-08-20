package eu.neclab.ngsildbroker.commons.tools;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class Validator {

	private static HashSet<String> validParams = new HashSet<String>();
	static {
		validParams.add(NGSIConstants.QUERY_PARAMETER_TYPE);
		validParams.add(NGSIConstants.QUERY_PARAMETER_ID);
		validParams.add(NGSIConstants.QUERY_PARAMETER_IDPATTERN);
		validParams.add(NGSIConstants.QUERY_PARAMETER_ATTRS);
		validParams.add(NGSIConstants.QUERY_PARAMETER_QUERY);
		validParams.add(NGSIConstants.QUERY_PARAMETER_GEOREL);
		validParams.add(NGSIConstants.QUERY_PARAMETER_GEOMETRY);
		validParams.add(NGSIConstants.QUERY_PARAMETER_COORDINATES);
		validParams.add(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY);
		validParams.add(NGSIConstants.QUERY_PARAMETER_TIMEPROPERTY);
		validParams.add(NGSIConstants.QUERY_PARAMETER_OFFSET);
		validParams.add(NGSIConstants.QUERY_PARAMETER_LIMIT);
		validParams.add(NGSIConstants.QUERY_PARAMETER_QTOKEN);
		validParams.add(NGSIConstants.QUERY_PARAMETER_OPTIONS);
		validParams.add(NGSIConstants.QUERY_PARAMETER_DETAILS);
		validParams.add(NGSIConstants.COUNT_HEADER_RESULT);
	}

	public static void validate(Map<String, String[]> parameterMap, int maxLimit, boolean ignoreType)
			throws ResponseException, URISyntaxException {

		
		for (String key : parameterMap.keySet()) {
			if (!validParams.contains(key)) {
				throw new ResponseException(ErrorType.BadRequestData, key + " is not valid parameter");
			}
			if (key.equals(NGSIConstants.QUERY_PARAMETER_LIMIT)) {
				int value = Integer.parseInt(parameterMap.get(key)[0]);
				if (value > maxLimit) {
					throw new ResponseException(ErrorType.TooManyResults,
							"The limit in the request is too big. To request with the max limit of " + maxLimit
									+ " remove the limit parameter");
				}
			}
			validateIdAndIdPattern(key, parameterMap);
		}
		if (!ignoreType && !parameterMap.containsKey(NGSIConstants.QUERY_PARAMETER_TYPE)
				&& !parameterMap.containsKey(NGSIConstants.QUERY_PARAMETER_ATTRS)) {
			throw new ResponseException(ErrorType.BadRequestData, "Missing mandatory minimum parameter "
					+ NGSIConstants.QUERY_PARAMETER_TYPE + " or " + NGSIConstants.QUERY_PARAMETER_ATTRS);
		}
	}

	public static void validateCsourceGetParameter(Map<String, String[]> parameterMap)
			throws ResponseException, URISyntaxException {

		for (String key : parameterMap.keySet()) {
			if (!validParams.contains(key)) {
				throw new ResponseException(ErrorType.BadRequestData, key + " is not valid parameter");
			}
			validateIdAndIdPattern(key, parameterMap);
		}
		if (!parameterMap.containsKey(NGSIConstants.QUERY_PARAMETER_TYPE)
				&& !parameterMap.containsKey(NGSIConstants.QUERY_PARAMETER_ATTRS)) {
			throw new ResponseException(ErrorType.BadRequestData, "Missing mandatory minimum parameter "
					+ NGSIConstants.QUERY_PARAMETER_TYPE + " or " + NGSIConstants.QUERY_PARAMETER_ATTRS);
		}
	}

	private static void validateIdAndIdPattern(String key, Map<String, String[]> parameterMap)
			throws ResponseException, URISyntaxException {
		// validate idpattern and id
		if (key.equals(NGSIConstants.QUERY_PARAMETER_IDPATTERN)) {
			String value = parameterMap.get(key)[0];
			if (!new URI(value).isAbsolute()) {
				throw new ResponseException(ErrorType.BadRequestData, "idPattern is not a URI");
			}
		}
		if (key.equals(NGSIConstants.QUERY_PARAMETER_ID)) {
			String value = parameterMap.get(key)[0];
			if (!new URI(value).isAbsolute()) {
				throw new ResponseException(ErrorType.BadRequestData, "id is not a URI");
			}
		}
	}
}

