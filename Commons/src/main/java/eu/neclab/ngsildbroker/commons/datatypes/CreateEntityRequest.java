package eu.neclab.ngsildbroker.commons.datatypes;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CreateEntityRequest extends EntityRequest {

	/**
	 * constructor for serialization
	 */
	public CreateEntityRequest() {
		super(AppConstants.OPERATION_CREATE_ENTITY, null);
	}
	
	public CreateEntityRequest(String payload, ArrayListMultimap<String, String> headers)
			throws ResponseException {
		super(AppConstants.OPERATION_CREATE_ENTITY, headers);
		generatePayloadVersions(payload);
	}

	private void generatePayloadVersions(String payload) throws ResponseException {
		JsonNode json = SerializationTools.parseJson(objectMapper, payload);
		JsonNode idNode = json.get(NGSIConstants.JSON_LD_ID);
		JsonNode type = json.get(NGSIConstants.JSON_LD_TYPE);
		// null id and type check
		if (idNode == null || type == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		this.id = idNode.asText();
		logger.debug("entity id " + id);
		// check in-memory hashmap for id

		String now = SerializationTools.formatter.format(Instant.now());
		setTemporalProperties(json, now, now, false);
		try {
			this.withSysAttrs = objectMapper.writeValueAsString(json);
		} catch (JsonProcessingException e) {
			// should never happen error checks are done before hand
			logger.error(e);
			throw new ResponseException(ErrorType.UnprocessableEntity, "Failed to parse entity");
		}
		removeTemporalProperties(json); // remove createdAt/modifiedAt fields informed by the user
		try {
			this.entityWithoutSysAttrs = objectMapper.writeValueAsString(json);
		} catch (JsonProcessingException e) {
			// should never happen error checks are done before hand
			logger.error(e);
			throw new ResponseException(ErrorType.UnprocessableEntity, "Failed to parse entity");
		}
		if (this.operationType == AppConstants.OPERATION_CREATE_ENTITY) {
			try {
				this.keyValue = objectMapper.writeValueAsString(getKeyValueEntity(json));
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	

}
