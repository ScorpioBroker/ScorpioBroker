package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;

public class NotificationHandlerMQTT extends BaseNotificationHandler {

	private final String CLIENT_ID = "ScorpioMqttNotifier";
	private HashMap<URI, MqttClient> uri2client = new HashMap<URI, MqttClient>();

	public NotificationHandlerMQTT(SubscriptionService subscriptionManagerService, ContextResolverBasic contextResolver,
			ObjectMapper objectMapper) {
		super(subscriptionManagerService, contextResolver, objectMapper);

	}

	@Override
	protected void sendReply(ResponseEntity<byte[]> reply, URI callback, Map<String, String> clientSettings)
			throws Exception {
		MqttClient client = getClient(callback, clientSettings);
		String qosString  = null;
		if(clientSettings != null) {
			qosString = clientSettings.get(NGSIConstants.MQTT_QOS);
			
		} else {
			qosString = String.valueOf(NGSIConstants.DEFAULT_MQTT_QOS);
		}
		int qos = 1;
		if (qosString != null) {
			qos = Integer.parseInt(qosString);
		}
		if (client instanceof Mqtt3BlockingClient) {
			Mqtt3BlockingClient client3 = (Mqtt3BlockingClient) client;
			client3.publishWith().topic(callback.getPath().substring(1)).qos(MqttQos.fromCode(qos))
					.payload(reply.getBody()).send();
		} else {
			Mqtt5BlockingClient client5 = (Mqtt5BlockingClient) client;
			client5.publishWith().topic(callback.getPath().substring(1))
					.contentType(reply.getHeaders().getFirst(HttpHeaders.CONTENT_TYPE)).qos(MqttQos.fromCode(qos))
					.payload(reply.getBody()).send();
		}
	}

	private MqttClient getClient(URI callback, Map<String, String> clientSettings) {
		URI baseURI = URI.create(callback.getScheme() + "://" + callback.getAuthority());
		MqttClient result = uri2client.get(baseURI);
		if (result == null) {
			String mqttVersion = null;
			if(clientSettings != null) {
				mqttVersion = clientSettings.get(NGSIConstants.MQTT_VERSION);
				
			} else {
				mqttVersion = NGSIConstants.DEFAULT_MQTT_VERSION; 
			}
			
			int port = callback.getPort();
			if (port == -1) {
				port = 1883;
			}
			if (mqttVersion == null || mqttVersion.equals(NGSIConstants.MQTT_VERSION_5)) {
				result = Mqtt5Client.builder().identifier(CLIENT_ID).serverHost(callback.getHost()).serverPort(port)
						.buildBlocking();
				((Mqtt5BlockingClient) result).connect();
			} else if(mqttVersion.equals(NGSIConstants.MQTT_VERSION_3)) {
				result = Mqtt3Client.builder().identifier(CLIENT_ID).serverHost(callback.getHost()).serverPort(port)
						.buildBlocking();
				((Mqtt3BlockingClient) result).connect();
			}
			uri2client.put(baseURI, result);

		}
		return result;
	}
	

}
