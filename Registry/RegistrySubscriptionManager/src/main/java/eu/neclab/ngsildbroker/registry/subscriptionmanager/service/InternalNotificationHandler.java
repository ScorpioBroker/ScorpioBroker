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
	protected void sendReply(Notification notification, SubscriptionRequest request, int internalState)
			throws Exception {
		notification.setSubscriptionId(notification.getSubscriptionId());
		switch (internalState) {
		case 1:
			return;
		case 0:
			break;
		case -1:
			cleanMixedResult(notification);
			break;
		default:
			break;
		}

		kafkaTemplate.send(topic, notification.getId(),
				new InternalNotification(notification.getId(), notification.getType(), notification.getNotifiedAt(),
						notification.getSubscriptionId(), notification.getData(), notification.getTriggerReason(),
						notification.getContext(), request.getTenant()));

	}

	private void cleanMixedResult(Notification notification) {
		// TODO Auto-generated method stub

	}

}
