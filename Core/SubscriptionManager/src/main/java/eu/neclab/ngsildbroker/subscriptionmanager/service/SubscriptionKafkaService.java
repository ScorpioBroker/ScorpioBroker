package eu.neclab.ngsildbroker.subscriptionmanager.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.datatypes.EntityRequest;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;

@Service
public class SubscriptionKafkaService {

	private final static Logger logger = LoggerFactory.getLogger(SubscriptionService.class);
	@Autowired
	SubscriptionService subscriptionService;
	
	@KafkaListener(topics = "${entity.append.topic}")
	public void handleAppend(Message<String> message) {
		String payload = new String(message.getPayload());
		String key = (String) message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY);
		logger.debug("Append got called: " + payload);
		logger.debug(key);
		subscriptionService.checkSubscriptionsWithAppend(DataSerializer.getEntityRequest(new String(message.getPayload())),
				(long) message.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP));
	}

	@KafkaListener(topics = "${entity.update.topic}")
	public void handleUpdate(Message<String> message) {
		String payload = new String(message.getPayload());
		String key = (String) message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY);
		logger.debug("update got called: " + payload);
		logger.debug(key);
		EntityRequest updateRequest = DataSerializer.getEntityRequest(payload);
		subscriptionService.checkSubscriptionsWithUpdate(updateRequest, (long) message.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP));
	}
	
	@KafkaListener(topics = "${entity.delete.topic}")
	public void handleDelete(Message<String> message) throws Exception {
		//EntityRequest req = DataSerializer.getEntityRequest(new String(message.getPayload()));
		//this.tenant2Ids2Type.remove(req.getTenant(), req.getId());
		//nothing to do here 
	}
	@KafkaListener(topics = "${entity.create.topic}")
	public void handleCreate(Message<String> message) {
		String key = (String) message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY);
		logger.debug("Create got called: " + key);
		subscriptionService.checkSubscriptionsWithCreate(DataSerializer.getEntityRequest(new String(message.getPayload())),
				(long) message.getHeaders().get(KafkaHeaders.RECEIVED_TIMESTAMP));
	}
	
	@KafkaListener(topics = "${csource.create.topic}")
	public void handleCSourceCreate(Message<String> message) {
		
	}
	@KafkaListener(topics = "${csource.append.topic}")
	public void handleCSourceAppend(Message<String> message) {
		
	}
	@KafkaListener(topics = "${csource.update.topic}")
	public void handleCSourceUpdate(Message<String> message) {
		
	}
	@KafkaListener(topics = "${csource.delete.topic}")
	public void handleCSourceDelete(Message<String> message) {
		
	}

}
