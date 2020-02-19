package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.net.URI;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;

public class NotificationHandlerMQTT extends BaseNotificationHandler{
	
	private final String CLIENT_ID = "ScorpioMqttNotifier";
	public NotificationHandlerMQTT(SubscriptionService subscriptionManagerService, ContextResolverBasic contextResolver,
			ObjectMapper objectMapper) {
		super(subscriptionManagerService, contextResolver, objectMapper);
		
	}

	@Override
	protected void sendReply(ResponseEntity<Object> reply, URI callback) throws Exception {
		MqttClient client = new MqttClient(callback.getAuthority(), CLIENT_ID);
		client.connect();
		MqttMessage message = new MqttMessage(reply.getBody().toString().getBytes());
		client.publish(callback.getPath(), message);
		client.disconnect();
		client.close();
		
	}

}
