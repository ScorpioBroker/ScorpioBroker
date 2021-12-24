package eu.neclab.ngsildbroker.commons.datatypes.requests;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteHistoryEntityRequest extends HistoryEntityRequest {

	@SuppressWarnings("unused")
	private String resolvedAttrId;
	@SuppressWarnings("unused")
	private String instanceId;

	public DeleteHistoryEntityRequest() {
	}

	public DeleteHistoryEntityRequest(ArrayListMultimap<String, String> headers, String resolvedAttrId,
			String instanceId, String entityId) throws ResponseException {
		super(headers, null, entityId);
		this.resolvedAttrId = resolvedAttrId;
		this.instanceId = instanceId;

	}
//TODO Fill up logic
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
