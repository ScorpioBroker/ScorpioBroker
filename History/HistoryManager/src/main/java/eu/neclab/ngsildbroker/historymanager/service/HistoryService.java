package eu.neclab.ngsildbroker.historymanager.service;

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonParser;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.storage.StorageWriterDAO;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historymanager.config.ProducerChannel;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;

@Service
public class HistoryService {

	private final static Logger logger = LoggerFactory.getLogger(HistoryService.class);

	@Autowired
	KafkaOps kafkaOperations;
	@Autowired
	ParamsResolver paramsResolver;
	@Autowired
	HistoryDAO historyDAO;

	@Autowired
	@Qualifier("historydao")
	StorageWriterDAO writerDAO;
//	public static final Gson GSON = DataSerializer.GSON;

	JsonParser parser = new JsonParser();

	private final ProducerChannel producerChannels;

	private boolean directDB = true;

	public HistoryService(ProducerChannel producerChannels) {
		this.producerChannels = producerChannels;

	}

	public URI createTemporalEntityFromEntity(ArrayListMultimap<String, String> headers, String payload)
			throws ResponseException, Exception {
		return createTemporalEntity(headers, payload, true);
	}

	public URI createTemporalEntityFromBinding(ArrayListMultimap<String, String> headers, String payload)
			throws ResponseException, Exception {
		return createTemporalEntity(headers, payload, false);
	}

	private URI createTemporalEntity(ArrayListMultimap<String, String> headers, String payload, boolean fromEntity)
			throws ResponseException, Exception {
		CreateHistoryEntityRequest request = new CreateHistoryEntityRequest(headers, payload, fromEntity);
		logger.trace("creating temporal entity");
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}

		return request.getUriId();
	}

	private void pushToKafka(HistoryEntityRequest request) throws ResponseException {
		try {
			kafkaOperations.pushToKafka(producerChannels.temporalEntityWriteChannel(),
					UUID.randomUUID().toString().getBytes(), DataSerializer.toJson(request).getBytes());
		} catch (ResponseException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InternalError,
					"Failed to push entity to kafka. " + e.getLocalizedMessage());
		}

	}

	private void pushToDB(HistoryEntityRequest request) throws ResponseException {
		try {
			writerDAO.storeTemporalEntity(request);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new ResponseException(e.getLocalizedMessage());
		}

	}

	/*
	 * private void pushAttributeToKafka(String entityId, String entityType, String
	 * entityCreatedAt, String entityModifiedAt, String attributeId, String
	 * elementValue, Boolean createTemporalEntityIfNotExists, Boolean overwriteOp)
	 * throws ResponseException { String messageKey; TemporalEntityStorageKey tesk =
	 * new TemporalEntityStorageKey(entityId); if (createTemporalEntityIfNotExists
	 * != null && createTemporalEntityIfNotExists) { tesk.setEntityType(entityType);
	 * tesk.setEntityCreatedAt(entityCreatedAt);
	 * tesk.setEntityModifiedAt(entityModifiedAt); tesk.setAttributeId(attributeId);
	 * messageKey = DataSerializer.toJson(tesk); } else {
	 * tesk.setEntityModifiedAt(entityModifiedAt); tesk.setAttributeId(attributeId);
	 * tesk.setOverwriteOp(overwriteOp); messageKey = DataSerializer.toJson(tesk); }
	 * logger.debug(" message key " + messageKey + " payload element " +
	 * elementValue);
	 * kafkaOperations.pushToKafka(producerChannels.temporalEntityWriteChannel(),
	 * messageKey.getBytes(), elementValue.getBytes()); }
	 */

	/*
	 * private void pushAttributeToKafka(String id, String entityModifiedAt, String
	 * attributeId, String elementValue) throws ResponseException {
	 * pushAttributeToKafka(id, null, null, entityModifiedAt, attributeId,
	 * elementValue, null, null); }
	 */
	public void delete(ArrayListMultimap<String, String> headers, String entityId, String attributeId,
			String instanceId, List<Object> linkHeaders) throws ResponseException, Exception {
		logger.debug("deleting temporal entity with id : " + entityId + "and attributeId : " + attributeId);

		String resolvedAttrId = null;
		/*QueryParams qp = new QueryParams();
		qp.setId(entityId);
		qp.setAttrs(resolvedAttrId);
		qp.setInstanceId(instanceId);
		QueryResult queryResult = historyDAO.query(qp);
		if (queryResult.getActualDataString().size() == 0) {
			throw new ResponseException(ErrorType.NotFound);
		}*/
		if (attributeId != null) {
			resolvedAttrId = paramsResolver.expandAttribute(attributeId, linkHeaders);
		}
		DeleteHistoryEntityRequest request = new DeleteHistoryEntityRequest(headers, resolvedAttrId, instanceId,
				entityId);
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}
	}

	// endpoint "/entities/{entityId}/attrs"
	public void addAttrib2TemporalEntity(ArrayListMultimap<String, String> headers, String entityId, String payload)
			throws ResponseException, Exception {
		if (!historyDAO.entityExists(entityId, HttpUtils.getTenantFromHeaders(headers))) {
			throw new ResponseException(ErrorType.NotFound, "You cannot create an attribute on a none existing entity");
		}
		AppendHistoryEntityRequest request = new AppendHistoryEntityRequest(headers, payload, entityId);
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}
	}

	// for endpoint "entities/{entityId}/attrs/{attrId}/{instanceId}")
	public void modifyAttribInstanceTemporalEntity(ArrayListMultimap<String, String> headers, String entityId,
			String payload, String attribId, String instanceId, List<Object> linkHeaders)
			throws ResponseException, Exception {

		String resolvedAttrId = null;
		if (attribId != null) {
			resolvedAttrId = paramsResolver.expandAttribute(attribId, linkHeaders);
		}

		// check if entityId + attribId + instanceid exists. if not, throw exception
		// ResourceNotFound
		QueryParams qp = new QueryParams();
		List<Map<String, String>> temp1 = new ArrayList<Map<String, String>>();
		HashMap<String, String> temp2 = new HashMap<String, String>();
		temp2.put(NGSIConstants.JSON_LD_ID, entityId);
		temp1.add(temp2);
		qp.setEntities(temp1);
		qp.setAttrs(resolvedAttrId);
		qp.setInstanceId(instanceId);
		qp.setIncludeSysAttrs(true);
		QueryResult queryResult = historyDAO.query(qp);
		List<String> entityList = queryResult.getActualDataString(); 
		if (entityList.size() == 0) {
			throw new ResponseException(ErrorType.NotFound);
		}
		String oldEntry = historyDAO.getListAsJsonArray(entityList);
		UpdateHistoryEntityRequest request = new UpdateHistoryEntityRequest(headers, payload, entityId, resolvedAttrId,
				instanceId, oldEntry);
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}
	}

	/*
	
	 */
	@KafkaListener(topics = "${entity.create.topic}", groupId = "historyManagerCreate")
	public void handleEntityCreate(@Payload byte[] message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		logger.trace("Listener handleEntityCreate...");
		String payload = new String(message);
		logger.debug("Received message: " + payload);
		CreateHistoryEntityRequest request = new CreateHistoryEntityRequest(
				DataSerializer.getEntityRequest(new String(message)));
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}
	}

	@KafkaListener(topics = "${entity.append.topic}", groupId = "historyManagerAppend")
	public void handleEntityAppend(@Payload byte[] message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		AppendHistoryEntityRequest request = new AppendHistoryEntityRequest(
				DataSerializer.getEntityRequest(new String(message)));
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}

	}

	@KafkaListener(topics = "${entity.update.topic}", groupId = "historyManagerUpdate")
	public void handleEntityUpdate(@Payload byte[] message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		UpdateHistoryEntityRequest request = new UpdateHistoryEntityRequest(
				DataSerializer.getEntityRequest(new String(message)));
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}

	}

	@KafkaListener(topics = "${entity.delete.topic}", groupId = "historyManagerDelete")
	public void handleEntityDelete(@Payload byte[] message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		logger.trace("Listener handleEntityDelete...");

		logger.debug("Received key: " + key);
		String payload = new String(message);
		logger.debug("Received message: " + payload);
	}

}
