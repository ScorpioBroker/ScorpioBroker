package eu.neclab.ngsildbroker.registrysubscriptionmanager.service;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.kafka.core.KafkaTemplate;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseNotificationHandler;
import eu.neclab.ngsildbroker.commons.subscriptionbase.SubscriptionInfoDAOInterface;

public class InternalNotificationHandler extends BaseNotificationHandler {

	private KafkaTemplate<String, Object> kafkaTemplate;
	private String topic;

	public InternalNotificationHandler(SubscriptionInfoDAOInterface subscriptionInfoDAO,
			KafkaTemplate<String, Object> kafkaTemplate, String topic) {
		super(subscriptionInfoDAO);
		this.kafkaTemplate = kafkaTemplate;
		this.topic = topic;
	}

	@Override
	protected void sendReply(Notification notification, SubscriptionRequest request) throws Exception {
		notification.setSubscriptionId(notification.getSubscriptionId());
		cleanNotificationFromInternal(notification);
		if (notification.getData().isEmpty()) {
			return;
		}
		kafkaTemplate.send(topic, notification.getId(),
				new InternalNotification(notification.getId(), notification.getType(), notification.getNotifiedAt(),
						notification.getSubscriptionId(), notification.getData(), notification.getTriggerReason(),
						notification.getContext(), request.getTenant(), request.getHeaders()));

	}

	private void cleanNotificationFromInternal(Notification notification) {
		List<Map<String, Object>> list = notification.getData();
		Iterator<Map<String, Object>> it = list.iterator();
		while (it.hasNext()) {
			Map<String, Object> next = it.next();
			if (((String) next.get(NGSIConstants.JSON_LD_ID)).contains(AppConstants.INTERNAL_REGISTRATION_ID)) {
				it.remove();
			}
		}

	}

}
