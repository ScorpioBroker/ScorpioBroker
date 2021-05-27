package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class DeleteHistoryEntityRequest extends HistoryEntityRequest {

	JsonParser parser = new JsonParser();
	private String resolvedAttrId;
	private String instanceId;

	/**
	 * Serialization constructor
	 * 
	 * @throws NoSuchAlgorithmException
	 */
	public static void main(String[] args) throws NoSuchAlgorithmException {
		String test = "";
		for (char temp : "http://sdfsdfsdf:!@#$%^&*()__2342/asdas".toCharArray()) {
			test += "" + ((int) temp);
		}
		System.out.println(test);
	}

	public DeleteHistoryEntityRequest() {
	}

	public DeleteHistoryEntityRequest(ArrayListMultimap<String, String> headers, String resolvedAttrId,
			String instanceId, String entityId) throws ResponseException {
		super(headers, null);
		this.resolvedAttrId = resolvedAttrId;
		this.instanceId = instanceId;
		this.id = entityId;
	}

	private void deleteTemporalEntity(String payload, boolean fromEntity) throws ResponseException, Exception {
		TemporalEntityStorageKey tesk = new TemporalEntityStorageKey(this.id);
		tesk.setAttributeId(resolvedAttrId);
		tesk.setInstanceId(instanceId);
		String messageKey = DataSerializer.toJson(tesk);
		logger.trace("message key created : " + messageKey);
//		kafkaOperations.pushToKafka(producerChannels.temporalEntityWriteChannel(), messageKey.getBytes(),
//				"null".getBytes());
//		logger.trace("temporal entity (" + entityId + ") deleted");
	}

}
