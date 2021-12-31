package eu.neclab.ngsildbroker.commons.tools;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;

import org.springframework.util.MultiValueMap;

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
		validParams.add(NGSIConstants.QUERY_PARAMETER_TIMEREL);
		validParams.add(NGSIConstants.QUERY_PARAMETER_TIME);
		validParams.add(NGSIConstants.QUERY_PARAMETER_LAST_N);
	}

	public static void validate(MultiValueMap<String, String> parameterMap, int maxLimit, boolean ignoreType)
			throws ResponseException, URISyntaxException {

		for (String key : parameterMap.keySet()) {
			if (!validParams.contains(key)) {
				throw new ResponseException(ErrorType.BadRequestData, key + " is not valid parameter");
			}
			if (key.equals(NGSIConstants.QUERY_PARAMETER_LIMIT)) {
				int value = Integer.parseInt(parameterMap.getFirst(key));
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

	public static void validateCsourceGetParameter(MultiValueMap<String, String> multiValueMap)
			throws ResponseException, URISyntaxException {

		for (String key : multiValueMap.keySet()) {
			if (!validParams.contains(key)) {
				throw new ResponseException(ErrorType.BadRequestData, key + " is not valid parameter");
			}
			validateIdAndIdPattern(key, multiValueMap);
		}
		if (!multiValueMap.containsKey(NGSIConstants.QUERY_PARAMETER_TYPE)
				&& !multiValueMap.containsKey(NGSIConstants.QUERY_PARAMETER_ATTRS)) {
			throw new ResponseException(ErrorType.BadRequestData, "Missing mandatory minimum parameter "
					+ NGSIConstants.QUERY_PARAMETER_TYPE + " or " + NGSIConstants.QUERY_PARAMETER_ATTRS);
		}
	}

	private static void validateIdAndIdPattern(String key, MultiValueMap<String, String> multiValueMap)
			throws ResponseException, URISyntaxException {
		// validate idpattern and id
		if (key.equals(NGSIConstants.QUERY_PARAMETER_IDPATTERN)) {
			String value = multiValueMap.getFirst(key);
			if (!new URI(value).isAbsolute()) {
				throw new ResponseException(ErrorType.BadRequestData, "idPattern is not a URI");
			}
		}
		if (key.equals(NGSIConstants.QUERY_PARAMETER_ID)) {
			String value = multiValueMap.getFirst(key);
			if (!new URI(value).isAbsolute()) {
				throw new ResponseException(ErrorType.BadRequestData, "id is not a URI");
			}
		}
	}

}
