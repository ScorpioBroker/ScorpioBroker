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

	private int contextHash;
	private Map<String, Object> original;
	private Map<String, Object> context;

	/**
	 * constructor for serialization
	 */
	public CreateEntityRequest() {

	}

	public CreateEntityRequest(Map<String, Object> resolved, ArrayListMultimap<String, String> headers,
			Map<String, Object> original, int contextHash, Map<String, Object> context) throws ResponseException {
		super(headers, (String) resolved.get(NGSIConstants.JSON_LD_ID), resolved, AppConstants.CREATE_REQUEST);
		generatePayloadVersions(resolved);
		this.original = original;
		this.contextHash = contextHash;
		this.context = context;
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

	public Map<String, Object> getContext() {
		return context;
	}

	public void setContext(Map<String, Object> context) {
		this.context = context;
	}

	public int getContextHash() {
		return contextHash;
	}

	public void setContextHash(int contextHash) {
		this.contextHash = contextHash;
	}

	public Map<String, Object> getOriginal() {
		return original;
	}

	public void setOriginal(Map<String, Object> original) {
		this.original = original;
	}

}
