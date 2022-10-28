package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import com.google.common.collect.ArrayListMultimap;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

class NotificationHandlerMQTT extends BaseNotificationHandler {

	public NotificationHandlerMQTT(SubscriptionInfoDAOInterface baseSubscriptionInfoDAO) {
		super(baseSubscriptionInfoDAO);
	}

	private final String CLIENT_ID = "ScorpioMqttNotifier";
	private HashMap<URI, MqttClient> uri2client = new HashMap<URI, MqttClient>();

	@Override
	protected void sendReply(Notification notification, SubscriptionRequest request) throws Exception {
		URI callback = request.getSubscription().getNotification().getEndPoint().getUri();
		Map<String, String> clientSettings = request.getSubscription().getNotification().getEndPoint()
				.getNotifierInfo();
		ArrayListMultimap<String, String> headers = request.getHeaders();
		MqttClient client = getClient(callback, clientSettings);
		String qosString = null;
		if (clientSettings != null) {
			qosString = clientSettings.get(NGSIConstants.MQTT_QOS);

		} else {
			qosString = String.valueOf(NGSIConstants.DEFAULT_MQTT_QOS);
		}
		int qos = 1;
		if (qosString != null) {
			qos = Integer.parseInt(qosString);
		}
		String payload = getPayload(notification, headers);
		if (client instanceof Mqtt3BlockingClient) {
			Mqtt3BlockingClient client3 = (Mqtt3BlockingClient) client;
			client3.publishWith().topic(callback.getPath().substring(1)).qos(MqttQos.fromCode(qos))
					.payload(payload.getBytes()).send();
		} else {
			Mqtt5BlockingClient client5 = (Mqtt5BlockingClient) client;
			client5.publishWith().topic(callback.getPath().substring(1))
					.contentType(headers.get(AppConstants.CONTENT_TYPE).get(0)).qos(MqttQos.fromCode(qos))
					.payload(payload.getBytes()).send();
		}

	}

	private String getPayload(Notification notification, ArrayListMultimap<String, String> headers) throws Exception {

		// Map<String, String> metaData = new HashMap<String, String>();
		StringBuilder result = new StringBuilder("{\"" + NGSIConstants.METADATA + "\":{");
		for (Entry<String, Collection<String>> entry : headers.asMap().entrySet()) {
			ArrayList<String> value = new ArrayList<String>(entry.getValue());
			result.append("\"");
			result.append(entry.getKey());
			result.append("\":");
			if (entry.getValue().size() != 1) {
				result.append("[");
				for (String headerValue : entry.getValue()) {
					result.append(headerValue + ",");
				}
				result.setCharAt(result.length() - 1, ']');
			} else {
				result.append("\"");
				result.append(value.get(0));
				result.append("\"");
			}
			result.append(",");
		}
		result.setCharAt(result.length() - 1, '}');
		result.append(",");
		result.append("\"");
		result.append(NGSIConstants.BODY);
		result.append("\":");
		result.append(notification.toCompactedJson().getBody());
		result.append("}");
		return result.toString();
	}

	private MqttClient getClient(URI callback, Map<String, String> clientSettings) {
		URI baseURI = URI.create(callback.getScheme() + "://" + callback.getAuthority());
		MqttClient result = uri2client.get(baseURI);
		if (result == null) {
			String mqttVersion = null;
			if (clientSettings != null) {
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
			} else if (mqttVersion.equals(NGSIConstants.MQTT_VERSION_3)) {
				result = Mqtt3Client.builder().identifier(CLIENT_ID).serverHost(callback.getHost()).serverPort(port)
						.buildBlocking();
				((Mqtt3BlockingClient) result).connect();
			}
			uri2client.put(baseURI, result);

		}
		return result;
	}

}
