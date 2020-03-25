package eu.neclab.ngsildbroker.registryhandler.service;

import java.util.ArrayList;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceNotification;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.CSourceNotificationHandler;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.registryhandler.config.CSourceProducerChannel;


public class CSourceNotificationHandlerInternalKafka implements CSourceNotificationHandler{
	
	private KafkaOps kafkaOps;
	private CSourceProducerChannel cSourceProducerChannel;
	

	public CSourceNotificationHandlerInternalKafka(KafkaOps kafkaOps, CSourceProducerChannel cSourceProducerChannel) {
		this.kafkaOps = kafkaOps;
		this.cSourceProducerChannel = cSourceProducerChannel;
	}

	@Override
	public void notify(CSourceNotification notification, Subscription sub) {
		byte[] id = sub.getId().toString().getBytes();
		ArrayList<String> temp = new ArrayList<String>();
		for(CSourceRegistration regInfo: notification.getData()) {
			temp.add(regInfo.getEndpoint().toString());
		}
		
		byte[] body = DataSerializer.toJson(temp).getBytes();
		try {
			this.kafkaOps.pushToKafka(cSourceProducerChannel.csourceNotificationWriteChannel(),id, body);
		} catch (ResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
