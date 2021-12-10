package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class CreateEntityRequest extends EntityRequest {

	/**
	 * constructor for serialization
	 */
	public CreateEntityRequest() {
		super(AppConstants.OPERATION_CREATE_ENTITY, null);
	}

	public CreateEntityRequest(Map<String, Object> resolved, ArrayListMultimap<String, String> headers)
			throws ResponseException {
		super(AppConstants.OPERATION_CREATE_ENTITY, headers);
		generatePayloadVersions(resolved);
	}

	private void generatePayloadVersions(Map<String, Object> payload) throws ResponseException {
		// JsonNode json = SerializationTools.parseJson(objectMapper, payload);
		// JsonNode idNode = json.get(NGSIConstants.JSON_LD_ID);
		// JsonNode type = json.get(NGSIConstants.JSON_LD_TYPE);
		// null id and type check
		// if (idNode == null || type == null) {
		// throw new ResponseException(ErrorType.BadRequestData);
		// }
		this.id = (String) payload.get(NGSIConstants.JSON_LD_ID);
		logger.debug("entity id " + id);
		// check in-memory hashmap for id

		String now = SerializationTools.formatter.format(Instant.now());
		setTemporalProperties(payload, now, now, false);
		try {
			this.withSysAttrs = JsonUtils.toString(payload);
		} catch (IOException e) {
			// should never happen error checks are done before hand
			logger.error(e);
			throw new ResponseException(ErrorType.UnprocessableEntity, "Failed to parse entity");
		}
		removeTemporalProperties(payload); // remove createdAt/modifiedAt fields informed by the user
		try {
			this.entityWithoutSysAttrs = JsonUtils.toString(payload);
		} catch (IOException e) {
			// should never happen error checks are done before hand
			logger.error(e);
			throw new ResponseException(ErrorType.UnprocessableEntity, "Failed to parse entity");
		}
		if (this.operationType == AppConstants.OPERATION_CREATE_ENTITY) {
			try {
				this.keyValue = JsonUtils.toPrettyString(getKeyValueEntity(payload));
			} catch (IOException e) {
				// should never happen error checks are done before hand
				logger.error(e);
				throw new ResponseException(ErrorType.UnprocessableEntity, "Failed to parse entity");
			}
		}
	}

}
