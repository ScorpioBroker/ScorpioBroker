package eu.neclab.ngsildbroker.entityhandler.validationutil;

import java.util.HashSet;
import java.util.Map;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class Validator {

	private static HashSet<String> validParams = new HashSet<String>();
	static {
		validParams.add(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID);
		validParams.add(NGSIConstants.QUERY_PARAMETER_DELETE_ALL);
	}

	/**
	 * Validate the query parameter and call this method from EntityController class.
	 * @param parameterMap
	 * @throws ResponseException
	 */
	public static void validate(Map<String, String[]> parameterMap) throws ResponseException {
		for (String key : parameterMap.keySet()) {
			if (!validParams.contains(key)) {
				throw new ResponseException(ErrorType.BadRequestData, key + " is not valid parameter");
			}
		}
	}
}
