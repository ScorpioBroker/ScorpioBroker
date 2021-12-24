package eu.neclab.ngsildbroker.entityhandler.validationutil;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.springframework.util.MultiValueMap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchFailure;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchResult;

public class Validator {

	private static HashSet<String> validParams = new HashSet<String>();
	static {
		validParams.add(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID);
		validParams.add(NGSIConstants.QUERY_PARAMETER_DELETE_ALL);
	}

	/**
	 * Validate the query parameter and call this method from EntityController
	 * class.
	 * 
	 * @param multiValueMap
	 * @throws ResponseException
	 */
	public static void validate(MultiValueMap<String, String> multiValueMap) throws ResponseException {
		for (String key : multiValueMap.keySet()) {
			if (!validParams.contains(key)) {
				throw new ResponseException(ErrorType.BadRequestData, key + " is not valid parameter");
			}
		}
	}

	public static boolean validatePayloadType(JsonNode next, String entityId, BatchResult result) {
		boolean validatePayload = false;
		ObjectNode objectNode = (ObjectNode) next;
		Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
		while (iter.hasNext()) {
			validatePayload = false;
			Map.Entry<String, JsonNode> entry = iter.next();
			if (entry.getValue().isArray() && entry.getValue().has(0) && entry.getValue().get(0).isObject()) {
				ObjectNode attrObj = (ObjectNode) entry.getValue().get(0);
				if (attrObj.has(NGSIConstants.JSON_LD_TYPE) && attrObj.get(NGSIConstants.JSON_LD_TYPE).isArray()
						&& attrObj.get(NGSIConstants.JSON_LD_TYPE).has(0)) {
				} else {
					validatePayload = true;
					result.addFail(new BatchFailure(entityId,
							new RestResponse(ErrorType.BadRequestData, "Bad Request Data.")));
					break;
				}
			}
		}
		return validatePayload;
	}
}
