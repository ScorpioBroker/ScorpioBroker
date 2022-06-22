package eu.neclab.ngsildbroker.commons.tools;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.inject.Singleton;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

@Singleton
public class MicroServiceUtils {
	private final static Logger logger = LoggerFactory.getLogger(MicroServiceUtils.class);

	@ConfigProperty(name = "scorpio.gatewayurl")
	String gatewayUrl;

	@ConfigProperty(name = "mysettings.gateway.port")
	int port;

	public URI getGatewayURL() {
		logger.trace("getGatewayURL() :: started");
		String url = null;
		try {
			if (gatewayUrl == null || gatewayUrl.strip().isEmpty()) {
				String hostIP = InetAddress.getLocalHost().getHostName();
				url = new StringBuilder("http://").append(hostIP).append(":").append(port).toString();
			} else {
				url = gatewayUrl;
			}
			logger.trace("getGatewayURL() :: completed");

			return new URI(url.toString());
		} catch (URISyntaxException | UnknownHostException e) {
			throw new AssertionError(
					"something went really wrong here when creating a URL... this should never happen but did with "
							+ url,
					e);
		}
	}

	public static Message<BaseRequest> deepCopyRequestMessage(Message<BaseRequest> original) {
		BaseRequest originalPayload = original.getPayload();
		Message<BaseRequest> result = new Message<BaseRequest>() {
			@Override
			public BaseRequest getPayload() {
				BaseRequest result = new BaseRequest(originalPayload);
				result.setFinalPayload(deepCopyMap(originalPayload.getFinalPayload()));
				result.setRequestPayload(deepCopyMap(originalPayload.getRequestPayload()));
				result.setHeaders(ArrayListMultimap.create(originalPayload.getHeaders()));
				return result;
			}

		};
		return result;

	}

	private static Map<String, Object> deepCopyMap(Map<String, Object> original) {
		Map<String, Object> result = Maps.newHashMap();
		for (Entry<String, Object> entry : original.entrySet()) {
			Object copiedValue;
			Object originalValue = entry.getValue();
			if (originalValue instanceof List) {
				copiedValue = deppCopyList((List<Object>) originalValue);
			} else if (originalValue instanceof Map) {
				copiedValue = deepCopyMap((Map<String, Object>) originalValue);
			} else if (originalValue instanceof Integer) {
				copiedValue = ((Integer) originalValue).intValue();
			} else if (originalValue instanceof Double) {
				copiedValue = ((Double) originalValue).doubleValue();
			} else if (originalValue instanceof Float) {
				copiedValue = ((Float) originalValue).floatValue();
			} else if (originalValue instanceof Boolean) {
				copiedValue = ((Boolean) originalValue).booleanValue();
			} else {
				copiedValue = originalValue.toString();
			}
			result.put(entry.getKey(), copiedValue);
		}

		return result;
	}

	private static List<Object> deppCopyList(List<Object> original) {
		List<Object> result = Lists.newArrayList();
		for (Object originalValue : original) {
			Object copiedValue;
			if (originalValue instanceof List) {
				copiedValue = deppCopyList((List<Object>) originalValue);
			} else if (originalValue instanceof Map) {
				copiedValue = deepCopyMap((Map<String, Object>) originalValue);
			} else if (originalValue instanceof Integer) {
				copiedValue = ((Integer) originalValue).intValue();
			} else if (originalValue instanceof Double) {
				copiedValue = ((Double) originalValue).doubleValue();
			} else if (originalValue instanceof Float) {
				copiedValue = ((Float) originalValue).floatValue();
			} else if (originalValue instanceof Boolean) {
				copiedValue = ((Boolean) originalValue).booleanValue();
			} else {
				copiedValue = originalValue.toString();
			}
			result.add(copiedValue);
		}
		return result;
	}

	public static Message<SubscriptionRequest> deepCopySubscriptionMessage(Message<SubscriptionRequest> busMessage) {
		SubscriptionRequest originalPayload = busMessage.getPayload();
		Message<SubscriptionRequest> result = new Message<SubscriptionRequest>() {

			@Override
			public SubscriptionRequest getPayload() {
				SubscriptionRequest tmp = new SubscriptionRequest();
				tmp.setActive(originalPayload.isActive());
				tmp.setContext(deppCopyList(originalPayload.getContext()));
				tmp.setFinalPayload(deepCopyMap(originalPayload.getFinalPayload()));
				tmp.setHeaders(ArrayListMultimap.create(originalPayload.getHeaders()));
				tmp.setId(originalPayload.getId());
				tmp.setRequestPayload(deepCopyMap(originalPayload.getRequestPayload()));
				tmp.setType(originalPayload.getRequestType());
				tmp.setSubscription(new Subscription(originalPayload.getSubscription()));
				return tmp;
			}

		};
		return result;
	}

}
