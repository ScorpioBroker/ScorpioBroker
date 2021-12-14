package eu.neclab.ngsildbroker.registryhandler.service;

import java.util.ArrayList;

import org.springframework.kafka.core.KafkaTemplate;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceNotification;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.interfaces.CSourceNotificationHandler;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;

public class CSourceNotificationHandlerInternalKafka implements CSourceNotificationHandler {

	private KafkaTemplate<String, String> kafkaTemplate;
	private String notificationChannel;

	public CSourceNotificationHandlerInternalKafka(KafkaTemplate<String, String> kafkaTemplate,
			String notificationChannel) {
		this.kafkaTemplate = kafkaTemplate;
		this.notificationChannel = notificationChannel;
	}

	@Override
	public void notify(CSourceNotification notification, Subscription sub) {
		ArrayList<String> temp = new ArrayList<String>();
		for (CSourceRegistration regInfo : notification.getData()) {
			temp.add(regInfo.getEndpoint().toString());
		}
		kafkaTemplate.send(notificationChannel, sub.getId().toString(), DataSerializer.toJson(temp));

	}

}
