package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import org.springframework.kafka.core.KafkaTemplate;

import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseNotificationHandler;

public class InternalNotificationHandler extends BaseNotificationHandler {

	private KafkaTemplate<String, Object> kafkaTemplate;
	private String topic;

	public InternalNotificationHandler(KafkaTemplate<String, Object> kafkaTemplate, String topic) {
		this.kafkaTemplate = kafkaTemplate;
		this.topic = topic;
	}

	@Override
	protected void sendReply(Notification notification, SubscriptionRequest request) throws Exception {
		notification.setSubscriptionId(
				notification.getSubscriptionId().substring(0, notification.getSubscriptionId().length()
						- RegistrySubscriptionService.INTERNAL_SUB_ID_SUFFIX.length() - 1));
		kafkaTemplate.send(topic,
				new InternalNotification(notification.getId(), notification.getType(), notification.getNotifiedAt(),
						notification.getSubscriptionId(), notification.getData(), notification.getTriggerReason(),
						notification.getContext(), request.getTenant()));

	}

}
