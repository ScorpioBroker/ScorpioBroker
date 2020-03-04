package eu.neclab.ngsildbroker.historymanager.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class Validator {
	/*
	 * @Autowired static ObjectMapper objectMapper;
	 */

	private static Map<String, String> getParameterMap() {
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put(NGSIConstants.QUERY_PARAMETER_TYPE, NGSIConstants.QUERY_PARAMETER_TYPE);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_ID, NGSIConstants.QUERY_PARAMETER_ID);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_IDPATTERN, NGSIConstants.QUERY_PARAMETER_IDPATTERN);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_ATTRS, NGSIConstants.QUERY_PARAMETER_ATTRS);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_QUERY, NGSIConstants.QUERY_PARAMETER_QUERY);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_GEOREL, NGSIConstants.QUERY_PARAMETER_GEOREL);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_GEOMETRY, NGSIConstants.QUERY_PARAMETER_GEOMETRY);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_COORDINATES, NGSIConstants.QUERY_PARAMETER_COORDINATES);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY, NGSIConstants.QUERY_PARAMETER_GEOPROPERTY);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_TIMEREL, NGSIConstants.QUERY_PARAMETER_TIMEREL);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_TIME, NGSIConstants.QUERY_PARAMETER_TIME);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_TIMEPROPERTY, NGSIConstants.QUERY_PARAMETER_TIMEPROPERTY);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_ENDTIME, NGSIConstants.QUERY_PARAMETER_ENDTIME);
		paramMap.put(NGSIConstants.QUERY_PARAMETER_OPTIONS, NGSIConstants.QUERY_PARAMETER_OPTIONS);
		return paramMap;
	}

	private static List<String> splitQueryParameter(String url) {
		List<String> queryPairs = null;
		if (url != null && url.trim().length() > 0) {
			queryPairs = new ArrayList<String>();
			String[] pairs = url.split("&");
			for (String pair : pairs) {
				int idx = pair.indexOf("=");
				queryPairs.add(pair.substring(0, idx));
			}
		}
		return queryPairs;
	}

	public static boolean validate(String queryString) {
		boolean result = true;
		Map<String, String> paramMap = getParameterMap();
		List<String> queriesKey = splitQueryParameter(queryString);
		for (String key : queriesKey) {
			String value = paramMap.get(key);
			if (value == null) {
				result = false;
				break;
			}
		}
		return result;
	}

	public static void validateTemporalEntity(String payload) throws ResponseException, Exception {
		JsonParser parser = new JsonParser();
		if (payload == null) {
			throw new ResponseException(ErrorType.UnprocessableEntity);
		}
		try {
			JsonObject jsonObject = parser.parse(payload).getAsJsonObject();
			if (jsonObject.isJsonNull()) {
				throw new ResponseException(ErrorType.OperationNotSupported);
			}
			if (!jsonObject.has(NGSIConstants.QUERY_PARAMETER_ID)
					|| !jsonObject.has(NGSIConstants.QUERY_PARAMETER_TYPE)) {
				throw new ResponseException(ErrorType.BadRequestData);
			}
			//for {"id":""} case
			if (jsonObject.get(NGSIConstants.QUERY_PARAMETER_ID).getAsString().trim().length() == 0
					|| jsonObject.get(NGSIConstants.QUERY_PARAMETER_TYPE).getAsString().trim().length() == 0) {
				throw new ResponseException(ErrorType.BadRequestData);
			}
		} catch (Exception e) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
	}

}
