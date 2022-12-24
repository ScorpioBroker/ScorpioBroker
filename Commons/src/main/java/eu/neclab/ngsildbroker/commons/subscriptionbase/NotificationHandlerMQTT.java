package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PreDestroy;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.vertx.mutiny.core.http.HttpHeaders;

class NotificationHandlerMQTT extends BaseNotificationHandler {

	private final String CLIENT_ID = "ScorpioMqttNotifier";
	private HashMap<URI, Object> uri2client = new HashMap<URI, Object>();

	@Override
	protected void sendReply(Notification notification, SubscriptionRequest request, int maxRetries) throws Exception {
		URI callback = request.getSubscription().getNotification().getEndPoint().getUri();
		Map<String, String> clientSettings = request.getSubscription().getNotification().getEndPoint()
				.getNotifierInfo();
		
		
		Object client = getClient(callback, clientSettings);
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
		String payload = getPayload(notification, request.getSubscription().getNotification().getEndPoint().getReceiverInfo());
		if (client instanceof MqttClient) {
			// resources are closed in predestroy
			@SuppressWarnings("resource")
			MqttClient client3 = (MqttClient) client;
			client3.publish(callback.getPath().substring(1), payload.getBytes(), qos, false);
		} else {
			org.eclipse.paho.mqttv5.client.MqttClient client5 = (org.eclipse.paho.mqttv5.client.MqttClient) client;
			MqttMessage message = new MqttMessage(payload.getBytes());
			message.setQos(qos);
			if (message.getProperties() != null)
				message.getProperties().setContentType(request.getSubscription().getNotification().getEndPoint().getAccept());
			client5.publish(callback.getPath().substring(1), message);
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
		result.append(notification.toCompactedJson().getEntity());
		result.append("}");
		return result.toString();
	}

	private Object getClient(URI callback, Map<String, String> clientSettings)
			throws MqttException, org.eclipse.paho.client.mqttv3.MqttException {
		URI baseURI = URI.create(callback.getScheme() + "://" + callback.getAuthority());
		Object result = uri2client.get(baseURI);
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
				result = new org.eclipse.paho.mqttv5.client.MqttClient(baseURI.toString(), CLIENT_ID);
				((org.eclipse.paho.mqttv5.client.MqttClient) result).connect();
			} else if (mqttVersion.equals(NGSIConstants.MQTT_VERSION_3)) {
				result = new MqttClient(baseURI.toString(), CLIENT_ID);
				((MqttClient) result).connect();
			}
			uri2client.put(baseURI, result);

		}
		return result;
	}

	@PreDestroy
	public void destroy() {
		for (Object client : uri2client.values()) {
			try {
				if (client instanceof MqttClient) {
					MqttClient client3 = (MqttClient) client;
					client3.close(true);
				} else {
					org.eclipse.paho.mqttv5.client.MqttClient client5 = (org.eclipse.paho.mqttv5.client.MqttClient) client;
					client5.close(true);
				}
			} catch (Exception e) {
				logger.debug("Failed to close mqtt client", e);
			}
		}

	}

}
