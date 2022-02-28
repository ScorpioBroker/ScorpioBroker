package eu.neclab.ngsildbroker.subscriptionmanager.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.AnnouncementMessage;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionSyncManager;

@Service
public class SubscriptionSyncService extends BaseSubscriptionSyncManager {

	@Value("${scorpio.topics.subalive}")
	private String SUB_ALIVE_TOPIC;

	@Override
	protected String getAliveTopic() {
		return SUB_ALIVE_TOPIC;
	}

	@KafkaListener(topics = "${scorpio.topics.subsync}", groupId = "subscription")
	private void listenForSubs(@Payload SubscriptionRequest message,
			@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
		listenForSubscriptionUpdates(message, key);
	}

	@KafkaListener(topics = "${scorpio.topics.subalive}", groupId = "subscription")
	private void listenForAlive(@Payload AnnouncementMessage message,
			@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
		listenForAnnouncements(message);
	}

}
