package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.net.URI;

import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.shaded.org.jetbrains.annotations.NotNull;

import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;

public class NotificationHandlerMQTT extends BaseNotificationHandler{
	
	private final String CLIENT_ID = "ScorpioMqttNotifier";
	public NotificationHandlerMQTT(SubscriptionService subscriptionManagerService, ContextResolverBasic contextResolver,
			ObjectMapper objectMapper) {
		super(subscriptionManagerService, contextResolver, objectMapper);
		
	}

	@Override
	protected void sendReply(ResponseEntity<Object> reply, URI callback) throws Exception {
		
		int port = callback.getPort();
		if(port == -1) {
			port = 1883;
		}
		Mqtt3BlockingClient client = Mqtt3Client.builder().identifier(CLIENT_ID).serverHost(callback.getHost()).serverPort(port).buildBlocking();
		client.connect();
		client.publishWith().topic(callback.getPath().substring(1)).qos(MqttQos.AT_LEAST_ONCE).payload(reply.getBody().toString().getBytes()).send();
		client.disconnect();
		
	}

}
