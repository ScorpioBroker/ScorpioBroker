package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseNotificationHandler;
import io.smallrye.reactive.messaging.MutinyEmitter;

public class InternalNotificationHandler extends BaseNotificationHandler {

	private MutinyEmitter<InternalNotification> kafkaSender;

	public InternalNotificationHandler(MutinyEmitter<InternalNotification> kafkaSender) {
		this.kafkaSender = kafkaSender;
	}

	@Override
	protected void sendReply(Notification notification, SubscriptionRequest request, int maxRetries) throws Exception {
		notification.setSubscriptionId(notification.getSubscriptionId());
		cleanNotificationFromInternal(notification);
		if (notification.getData().isEmpty()) {
			return;
		}
		kafkaSender.send(new InternalNotification(notification.getId(), notification.getType(),
				notification.getNotifiedAt(), notification.getSubscriptionId(), notification.getData(),
				notification.getTriggerReason(), notification.getContext(), request.getTenant(), request.getTenant()));
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
