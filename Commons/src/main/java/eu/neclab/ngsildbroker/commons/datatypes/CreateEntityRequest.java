package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class CreateEntityRequest extends EntityRequest {

	/**
	 * constructor for serialization
	 */
	public CreateEntityRequest() {

	}

	public CreateEntityRequest(Map<String, Object> resolved, ArrayListMultimap<String, String> headers)
			throws ResponseException {
		super(headers, (String) resolved.get(NGSIConstants.JSON_LD_ID), resolved);
		generatePayloadVersions(resolved);
	}

	private void generatePayloadVersions(Map<String, Object> payload) throws ResponseException {

		logger.debug("entity id " + getId());
		String now = SerializationTools.formatter.format(Instant.now());
		setTemporalProperties(payload, now, now, false);
		setFinalPayload(payload);
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

		try {
			this.keyValue = JsonUtils.toPrettyString(getKeyValueEntity(payload));
		} catch (IOException e) {
			// should never happen error checks are done before hand
			logger.error(e);
			throw new ResponseException(ErrorType.UnprocessableEntity, "Failed to parse entity");
		}

	}

}
