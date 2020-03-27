package eu.neclab.ngsildbroker.storagemanager.services;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

import com.google.common.base.Splitter;
import com.google.gson.Gson;

import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.storagemanager.repository.EntityStorageReaderDAO;

@Service
@ConditionalOnProperty(value = "reader.enabled", havingValue = "true", matchIfMissing = false)
public class StorageReaderService {

	private final static Logger logger = LogManager.getLogger(StorageWriterService.class);
	private final static int MAX_UTF_SIZE = 65535;

	// public static final Gson GSON = DataSerializer.GSON;

	@Autowired
	EntityStorageReaderDAO storageReaderDao;

	@KafkaListener(topics = "${query.topic}", groupId = "queryHandler", properties = { "max.request.size=104857600" })
	@SendTo
	// @SendTo("QUERY_RESULT") // for tests without QueryManager
	public byte[] handleQuery(@Payload byte[] message) throws Exception {

		/*
		 * TODO: Ignore old messages in Kafka queue based on producer timestamp. There
		 * is no custom annotation in KafkaListener to always start from the latest
		 * offset. Source: https://github.com/spring-projects/spring-kafka/issues/914
		 * 
		 * Please note auto.offset.reset is a different thing and does not apply to this
		 * issue
		 * 
		 * 
		 * @Header(KafkaHeaders.TIMESTAMP) String producerTimestamp,
		 * 
		 * @Header(KafkaHeaders.TIMESTAMP_TYPE) String producerTimestampType
		 * logger.debug("Producer timestamp: " + producerTimestamp + " (" +
		 * producerTimestampType + ")");
		 * 
		 */

		logger.trace("Listener queryHandler, Thread ID: " + Thread.currentThread().getId());
		logger.trace("handleQuery() :: started");
		String payload = new String(message);
		logger.debug("Received message: " + payload);
		List<String> entityList = new ArrayList<String>();
		try {
			QueryParams qp = DataSerializer.getQueryParams(payload);
			entityList = storageReaderDao.query(qp);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.trace("Pushing result to Kafka...");
		logger.debug(storageReaderDao.getListAsJsonArray(entityList));
		// write to byte array
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		for (String element : entityList) {
			if (element.length() > MAX_UTF_SIZE) {
				for(String subelement: Splitter.fixedLength(MAX_UTF_SIZE).split(element)) {
					out.writeUTF(subelement);
				}
			} else {
				out.writeUTF(element);
			}
		}
		logger.trace("handleQuery() :: completed");
		return baos.toByteArray();
	}

}
