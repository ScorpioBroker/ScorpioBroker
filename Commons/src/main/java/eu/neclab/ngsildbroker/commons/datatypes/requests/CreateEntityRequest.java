package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.time.Instant;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.vertx.core.json.JsonObject;

public class CreateEntityRequest extends EntityRequest {

	/**
	 * constructor for serialization
	 */
	public CreateEntityRequest() {

	}

	public CreateEntityRequest(Map<String, Object> resolved, String tenant)
			throws ResponseException {
		super(tenant, (String) resolved.get(NGSIConstants.JSON_LD_ID), resolved, AppConstants.CREATE_REQUEST);
		generatePayloadVersions(resolved);
	}

	private void generatePayloadVersions(Map<String, Object> payload) throws ResponseException {
		String now = SerializationTools.formatter.format(Instant.now());
		setTemporalProperties(payload, now, now, false);
		setFinalPayload(payload);
		this.withSysAttrs = JsonObject.mapFrom(payload);
		removeTemporalProperties(payload); // remove createdAt/modifiedAt fields informed by the user
		this.entityWithoutSysAttrs = JsonObject.mapFrom(payload);
		this.keyValue = JsonObject.mapFrom(getKeyValueEntity(payload));

	}

}
