package eu.neclab.ngsildbroker.storagemanager.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.storagemanager.repository.StorageWriterDAO;

@Service
@ConditionalOnProperty(value="writer.enabled", havingValue = "true", matchIfMissing = false)
public class StorageWriterService {

	private final static Logger logger = LogManager.getLogger(StorageWriterService.class);

	public final static String ENTITY_LISTENER_ID = "entityWriter-1";
	public final static String KVENTITY_LISTENER_ID = "kvEntityWriter-1";
	public final static String ENTITY_WITHOUT_SYSATTRS_LISTENER_ID = "entityWithoutSysAttrsWriter-1";
	public final static String CSOURCE_LISTENER_ID = "csourceWriter-1";
	public final static String TEMPORALENTITY_LISTENER_ID = "temporalEntityWriter-1";

	@Autowired
	StorageWriterDAO storageWriterDao;

	@Value("${entity.stopListenerIfDbFails:true}")
	boolean entityStopListenerIfDbFails;
	@Value("${csource.stopListenerIfDbFails:true}")
	boolean csourceStopListenerIfDbFails;
	@Value("${entity.temporal.stopListenerIfDbFails:true}")
	boolean temporalEntityStopListenerIfDbFails;

	boolean entityListenerOk = true;
	boolean csourceListenerOk = true;
	boolean temporalEntityListenerOk = true;

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpoint;

	/*
	 * @KafkaListener(containerFactory =
	 * "kafkaListenerContainerFactoryManualOffsetCommit", topics =
	 * "${entity.topic}", groupId = "entityWriter") public void writeEntity(@Payload
	 * byte[] message, Acknowledgment acknowledgment,
	 * 
	 * @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String
	 * key, @Header(KafkaHeaders.OFFSET) Long offset) throws Exception {
	 * logger.trace("Listener entityWriter, Thread ID: " +
	 * Thread.currentThread().getId()); logger.debug("Received key: " + key); if
	 * (!entityListenerOk) // this test is needed because listenerContainer.stop()
	 * does not work properly // during boot time (probably because of concurrency)
	 * return; logger.debug("Received offset: " + offset.toString()); String payload
	 * = new String(message); logger.debug("Received message: " + payload);
	 * logger.trace("Writing data..."); if (storageWriterDao != null &&
	 * storageWriterDao.store(DBConstants.DBTABLE_ENTITY, DBConstants.DBCOLUMN_DATA,
	 * key, payload)) { acknowledgment.acknowledge();
	 * logger.trace("Kafka offset commited"); } else { if
	 * (entityStopListenerIfDbFails) { entityListenerOk = false;
	 * logger.error("DB failed, not processing any new messages");
	 * MessageListenerContainer listenerContainer = kafkaListenerEndpoint
	 * .getListenerContainer(ENTITY_LISTENER_ID); listenerContainer.stop(); } }
	 * 
	 * logger.trace("Writing is complete"); }
	 * 
	 * @KafkaListener(containerFactory =
	 * "kafkaListenerContainerFactoryManualOffsetCommit", topics =
	 * "${entity.withoutSysAttrs.topic}", groupId = "entityWithoutSysAttrsWriter")
	 * public void writeEntityWithoutSysAttrs(@Payload byte[] message,
	 * Acknowledgment acknowledgment,
	 * 
	 * @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String
	 * key, @Header(KafkaHeaders.OFFSET) Long offset) throws Exception {
	 * logger.trace("Listener entityWithoutSysAttrsWriter, Thread ID: " +
	 * Thread.currentThread().getId()); logger.debug("Received key: " + key); if
	 * (!entityListenerOk) // this test is needed because listenerContainer.stop()
	 * does not work properly // during boot time (probably because of concurrency)
	 * return; logger.debug("Received offset: " + offset.toString()); String payload
	 * = new String(message); logger.debug("Received message: " + payload);
	 * logger.trace("Writing data..."); if (storageWriterDao != null &&
	 * storageWriterDao.store(DBConstants.DBTABLE_ENTITY,
	 * DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS, key, payload)) {
	 * acknowledgment.acknowledge(); logger.trace("Kafka offset commited"); } else {
	 * if (entityStopListenerIfDbFails) { entityListenerOk = false;
	 * logger.error("DB failed, not processing any new messages");
	 * MessageListenerContainer listenerContainer = kafkaListenerEndpoint
	 * .getListenerContainer(ENTITY_WITHOUT_SYSATTRS_LISTENER_ID);
	 * listenerContainer.stop(); } } logger.trace("Writing is complete"); }
	 * 
	 * @KafkaListener(containerFactory =
	 * "kafkaListenerContainerFactoryManualOffsetCommit", topics =
	 * "${entity.keyValues.topic}", groupId = "kvEntityWriter") public void
	 * writeKeyValueEntity(@Payload byte[] message, Acknowledgment acknowledgment,
	 * 
	 * @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String
	 * key, @Header(KafkaHeaders.OFFSET) Long offset) throws Exception {
	 * logger.trace("Listener kvEntityWriter, Thread ID: " +
	 * Thread.currentThread().getId()); logger.debug("Received key: " + key); if
	 * (!entityListenerOk) // this test is needed because listenerContainer.stop()
	 * does not work properly // during boot time (probably because of concurrency)
	 * return; logger.debug("Received offset: " + offset.toString()); String payload
	 * = new String(message); logger.debug("Received message: " + payload);
	 * logger.trace("Writing data..."); if (storageWriterDao != null &&
	 * storageWriterDao.store(DBConstants.DBTABLE_ENTITY,
	 * DBConstants.DBCOLUMN_KVDATA, key, payload)) { acknowledgment.acknowledge();
	 * logger.trace("Kafka offset commited"); } else { if
	 * (entityStopListenerIfDbFails) { entityListenerOk = false;
	 * logger.error("DB failed, not processing any new messages");
	 * MessageListenerContainer listenerContainer = kafkaListenerEndpoint
	 * .getListenerContainer(KVENTITY_LISTENER_ID); listenerContainer.stop(); } }
	 * logger.trace("Writing is complete"); }
	 */
	@KafkaListener(containerFactory = "kafkaListenerContainerFactoryManualOffsetCommit", topics = "${csource.topic}", id = CSOURCE_LISTENER_ID, groupId = "csourceWriter", containerGroup = "csourceWriter-container")
	public void writeCSource(@Payload byte[] message, Acknowledgment acknowledgment,
			@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key, @Header(KafkaHeaders.OFFSET) Long offset)
			throws Exception {
		logger.trace("Listener csourceWriter, Thread ID: " + Thread.currentThread().getId());
		logger.debug("Received key: " + key);
		if (!csourceListenerOk) // this test is needed because listenerContainer.stop() does not work properly
								// during boot time (probably because of concurrency)
			return;
		String payload = new String(message);
		logger.debug("Received message: " + payload);
		logger.trace("Writing data...");
		if (storageWriterDao != null && storageWriterDao.store(DBConstants.DBTABLE_CSOURCE, DBConstants.DBCOLUMN_DATA, key, payload)) {
			acknowledgment.acknowledge();
			logger.trace("Kafka offset commited");
		} else {
			if (csourceStopListenerIfDbFails) {
				csourceListenerOk = false;
				logger.error("DB failed, not processing any new messages");
				MessageListenerContainer listenerContainer = kafkaListenerEndpoint
						.getListenerContainer(CSOURCE_LISTENER_ID);
				listenerContainer.stop();
			}
		}
		logger.trace("Writing is complete");
	}

	@KafkaListener(containerFactory = "kafkaListenerContainerFactoryManualOffsetCommit", topics = "${entity.temporal.topic}", id = TEMPORALENTITY_LISTENER_ID, groupId = "temporalEntityWriter", containerGroup = "temporalEntityWriter-container")
	public void writeTemporalEntity(@Payload byte[] message, Acknowledgment acknowledgment,
			@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key, @Header(KafkaHeaders.OFFSET) Long offset)
			throws Exception {
		logger.trace("Listener temporalEntityWriter, Thread ID: " + Thread.currentThread().getId());
		logger.debug("Received key: " + key);
		if (!temporalEntityListenerOk) // this test is needed because listenerContainer.stop() does not work properly
										// during boot time (probably because of concurrency)
			return;
		String payload = new String(message);
		logger.debug("Received message: " + payload);
		logger.trace("Writing data...");
		if (storageWriterDao != null && storageWriterDao.storeTemporalEntity(key, payload)) {
			acknowledgment.acknowledge();
			logger.trace("Kafka offset commited");
		} else {
			if (temporalEntityStopListenerIfDbFails) {
				temporalEntityListenerOk = false;
				logger.error("DB failed, not processing any new messages");
				MessageListenerContainer listenerContainer = kafkaListenerEndpoint
						.getListenerContainer(TEMPORALENTITY_LISTENER_ID);
				listenerContainer.stop();
			}
		}
		logger.trace("Writing is complete");
	}

}
