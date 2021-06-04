package eu.neclab.ngsildbroker.commons.datatypes;

import java.security.NoSuchAlgorithmException;

import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonParser;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

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
	

	/*
	 * private void deleteTemporalEntity(String payload, boolean fromEntity) throws
	 * ResponseException, Exception { TemporalEntityStorageKey tesk = new
	 * TemporalEntityStorageKey(this.id); tesk.setAttributeId(resolvedAttrId);
	 * tesk.setInstanceId(instanceId); String messageKey =
	 * DataSerializer.toJson(tesk); logger.trace("message key created : " +
	 * messageKey); //
	 * kafkaOperations.pushToKafka(producerChannels.temporalEntityWriteChannel(),
	 * messageKey.getBytes(), // "null".getBytes()); //
	 * logger.trace("temporal entity (" + entityId + ") deleted"); }
	 */

}
