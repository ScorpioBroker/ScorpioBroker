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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntityCRUDService;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.storage.StorageWriterDAO;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;

@Service
public class HistoryService implements EntityCRUDService {

	private final static Logger logger = LoggerFactory.getLogger(HistoryService.class);

	@Autowired
	HistoryDAO historyDAO;

	@Autowired
	@Qualifier("hhdao")
	StorageWriterDAO writerDAO;

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	private boolean directDB = true;

	@Value("${entity.temporal.topic:TEMPORALENTITY}")
	private String TEMP_ENTITY_TOPIC;

	public URI createTemporalEntityFromEntity(ArrayListMultimap<String, String> headers, Map<String, Object> payload)
			throws ResponseException, Exception {
		return createTemporalEntity(headers, payload, true);
	}

	public String createMessage(ArrayListMultimap<String, String> headers, Map<String, Object> resolved)
			throws ResponseException, Exception {
		return createTemporalEntity(headers, resolved, false).toString();
	}

	private URI createTemporalEntity(ArrayListMultimap<String, String> headers, Map<String, Object> resolved,
			boolean fromEntity) throws ResponseException, Exception {
		CreateHistoryEntityRequest request = new CreateHistoryEntityRequest(headers, resolved, fromEntity);
		logger.trace("creating temporal entity");
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}

		return request.getUriId();
	}

	private void pushToKafka(HistoryEntityRequest request) throws ResponseException {
		kafkaTemplate.send(TEMP_ENTITY_TOPIC, UUID.randomUUID().toString(), DataSerializer.toJson(request));
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
	public boolean deleteEntity(ArrayListMultimap<String, String> headers, String entityId)
			throws ResponseException, Exception {
		return delete(headers, entityId, null, null, null);
	}

	public boolean delete(ArrayListMultimap<String, String> headers, String entityId, String attributeId,
			String instanceId, Context linkHeaders) throws ResponseException, Exception {
		logger.debug("deleting temporal entity with id : " + entityId + "and attributeId : " + attributeId);

		String resolvedAttrId = null;
		if (attributeId != null) {
			resolvedAttrId = ParamsResolver.expandAttribute(attributeId, linkHeaders);
		}
		DeleteHistoryEntityRequest request = new DeleteHistoryEntityRequest(headers, resolvedAttrId, instanceId,
				entityId);
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}
		return true;
	}

	// endpoint "/entities/{entityId}/attrs"
	public AppendResult appendMessage(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved, String options) throws ResponseException, Exception {
		if (!historyDAO.entityExists(entityId, HttpUtils.getTenantFromHeaders(headers))) {
			throw new ResponseException(ErrorType.NotFound, "You cannot create an attribute on a none existing entity");
		}
		AppendHistoryEntityRequest request = new AppendHistoryEntityRequest(headers, resolved, entityId);
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}
		return new AppendResult(resolved, request.getPayload());
	}

	// for endpoint "entities/{entityId}/attrs/{attrId}/{instanceId}")
	public void modifyAttribInstanceTemporalEntity(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved, String attribId, String instanceId, Context linkHeaders)
			throws ResponseException, Exception {

		String resolvedAttrId = null;
		if (attribId != null) {
			resolvedAttrId = ParamsResolver.expandAttribute(attribId, linkHeaders);
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
		UpdateHistoryEntityRequest request = new UpdateHistoryEntityRequest(headers, resolved, entityId, resolvedAttrId,
				instanceId, oldEntry);
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}
	}

	/*
	
	 */
	@KafkaListener(topics = "${entity.create.topic}")
	public void handleEntityCreate(@Payload String message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		logger.trace("Listener handleEntityCreate...");
		// String payload = new String(message);
		logger.debug("Received message: " + message);
		CreateHistoryEntityRequest request = new CreateHistoryEntityRequest(DataSerializer.getEntityRequest(message));
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}
	}

	@KafkaListener(topics = "${entity.append.topic}")
	public void handleEntityAppend(@Payload String message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		AppendHistoryEntityRequest request = new AppendHistoryEntityRequest(DataSerializer.getEntityRequest(message));
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}

	}

	@KafkaListener(topics = "${entity.update.topic}")
	public void handleEntityUpdate(@Payload String message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		UpdateHistoryEntityRequest request = new UpdateHistoryEntityRequest(DataSerializer.getEntityRequest(message));
		if (directDB) {
			pushToDB(request);
		} else {
			pushToKafka(request);
		}

	}

	@KafkaListener(topics = "${entity.delete.topic}")
	public void handleEntityDelete(@Payload String message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		logger.trace("Listener handleEntityDelete...");

		logger.debug("Received key: " + key);
		logger.debug("Received message: " + message);
	}
}
