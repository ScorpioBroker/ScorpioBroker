package eu.neclab.ngsildbroker.commons.tools;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map.Entry;

import org.springframework.util.MultiValueMap;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class Validator {

	private static Gson gson = new Gson();
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

	public static void subscriptionValidation(String payload) throws ResponseException {
		boolean validatePayload = isJSONValid(payload);
		if (validatePayload == false) {
			throw new ResponseException(ErrorType.InvalidRequest, "Json is not valid");
		}
		JsonElement jsonElement = new JsonParser().parse(payload);
		JsonObject top = jsonElement.getAsJsonObject();
		if (!top.has(NGSIConstants.NOTIFICATION)) {
			throw new ResponseException(ErrorType.BadRequestData, "no notification parameter provided");
		}

		for (Entry<String, JsonElement> entry : top.entrySet()) {
			String key = entry.getKey();
			JsonElement value = entry.getValue();
			if (key.equals(NGSIConstants.CSOURCE_TYPE)) {
				if (value.isJsonNull()) {
					throw new ResponseException(ErrorType.BadRequestData, "invalid type value");
				}
				if (!value.getAsString().equalsIgnoreCase(NGSIConstants.NGSI_LD_SUBSCRIPTION_SHORT)) {
					throw new ResponseException(ErrorType.BadRequestData, "No type or type is not Subscription");
				}
			}
			if (key.equals(NGSIConstants.NOTIFICATION)) {
				if (!value.isJsonNull()) {
					JsonObject ldObj = value.getAsJsonObject();
					if (ldObj.has(NGSIConstants.CSOURCE_ENDPOINT)) {
						if (ldObj.get(NGSIConstants.CSOURCE_ENDPOINT).isJsonNull()) {
							throw new ResponseException(ErrorType.BadRequestData, "endpoint is a mandatory field");
						}
					} else {
						throw new ResponseException(ErrorType.BadRequestData, "endpoint is a mandatory field");
					}
				} else {
					throw new ResponseException(ErrorType.BadRequestData, "notification parameter is mandatory field");
				}
			}
		}

		if (top.has(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES_SHORT)) {
			JsonArray tempJson = top.get(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES_SHORT).getAsJsonArray();
			if (tempJson.size() == 0) {
				throw new ResponseException(ErrorType.BadRequestData, "watchedAttributes is not empty field");
			} else {
				String temp = tempJson.toString();
				if (temp.matches(NGSIConstants.NGSI_LD_FORBIDDEN_KEY_CHARS_REGEX)) {
					throw new ResponseException(ErrorType.BadRequestData, "Invalid character in attribute names");
				}
			}
		}
		if (top.has(NGSIConstants.CSOURCE_EXPIRES)) {
			JsonElement json = top.get(NGSIConstants.CSOURCE_EXPIRES);
			if (!json.isJsonNull()) {
				String dateExpiredAt = top.get(NGSIConstants.CSOURCE_EXPIRES).getAsString();
				try {
					checkExpiredAtDate(dateExpiredAt);
					Long expiredAtValidate = SerializationTools.date2Long(dateExpiredAt);
					if (!isValidFutureDate(expiredAtValidate)) {
						throw new ResponseException(ErrorType.BadRequestData, "Invalid expire date!");
					}
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, e.getMessage());
				}
			}
		}
	}

	private static void checkExpiredAtDate(String expiredAt) throws ResponseException {
		try {
			SerializationTools.date2Long(expiredAt);

		} catch (Exception e) {
			throw new ResponseException(ErrorType.BadRequestData, "Failed to parse expiresAt");
		}

	}

	private static boolean isJSONValid(String jsonInString) {
		try {
			gson.fromJson(jsonInString, Object.class);
			return true;
		} catch (com.google.gson.JsonSyntaxException ex) {
			return false;
		}
	}

	private static boolean isValidFutureDate(Long date) {

		return System.currentTimeMillis() < date;
	}

}
