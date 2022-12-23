package eu.neclab.ngsildbroker.commons.tools;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.inject.Singleton;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.vertx.core.http.impl.headers.HeadersMultiMap;

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

	public static BaseRequest deepCopyRequestMessage(BaseRequest originalPayload) {
		BaseRequest result = new BaseRequest();
		result.setId(originalPayload.getId());
		result.setFinalPayload(deepCopyMap(originalPayload.getFinalPayload()));
		result.setRequestPayload(deepCopyMap(originalPayload.getRequestPayload()));
		if (originalPayload.getTenant() != null) {
			result.setTenant(originalPayload.getTenant());
		}
		result.setRequestType(originalPayload.getRequestType());
		result.setBatchInfo(originalPayload.getBatchInfo());
		return result;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> deepCopyMap(Map<String, Object> original) {
		if (original == null) {
			return null;
		}
		Map<String, Object> result = Maps.newHashMap();
		for (Entry<String, Object> entry : original.entrySet()) {
			Object copiedValue;
			Object originalValue = entry.getValue();
			if (originalValue == null) {
				copiedValue = null;
			} else if (originalValue instanceof List) {
				copiedValue = deppCopyList((List<Object>) originalValue);
			} else if (originalValue instanceof Map) {
				copiedValue = deepCopyMap((Map<String, Object>) originalValue);

			} else if (originalValue instanceof Boolean) {
				copiedValue = ((Boolean) originalValue).booleanValue();
			} else if (originalValue instanceof Number) {
				copiedValue = originalValue;
			}

			else {
				copiedValue = originalValue.toString();
			}
			result.put(entry.getKey(), copiedValue);
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	private static List<Object> deppCopyList(List<Object> original) {
		if (original == null) {
			return null;
		}
		List<Object> result = Lists.newArrayList();
		for (Object originalValue : original) {
			Object copiedValue;
			if (originalValue instanceof List) {
				copiedValue = deppCopyList((List<Object>) originalValue);
			} else if (originalValue instanceof Map) {
				copiedValue = deepCopyMap((Map<String, Object>) originalValue);
			} else if (originalValue instanceof Number) {
				copiedValue = originalValue;
			} else if (originalValue instanceof Boolean) {
				copiedValue = ((Boolean) originalValue).booleanValue();
			} else {
				copiedValue = originalValue.toString();
			}
			result.add(copiedValue);
		}
		return result;
	}

	public static SubscriptionRequest deepCopySubscriptionMessage(SubscriptionRequest originalPayload) {
		SubscriptionRequest tmp = new SubscriptionRequest();
		tmp.setActive(originalPayload.isActive());
		tmp.setContext(deppCopyList(originalPayload.getContext()));
		tmp.setFinalPayload(deepCopyMap(originalPayload.getFinalPayload()));
		tmp.setTenant(originalPayload.getTenant());
		tmp.setId(originalPayload.getId());
		tmp.setRequestPayload(deepCopyMap(originalPayload.getRequestPayload()));
		tmp.setType(originalPayload.getRequestType());
		tmp.setSubscription(new Subscription(originalPayload.getSubscription()));
		return tmp;
	}

	public static HeadersMultiMap getHeaders(ArrayListMultimap<String, String> receiverInfo) {
		HeadersMultiMap result = new HeadersMultiMap();
		for (Entry<String, String> entry : receiverInfo.entries()) {
			result.add(entry.getKey(), entry.getValue());
		}
		return result;
	}

	public static SyncMessage deepCopySyncMessage(SyncMessage originalSync) {
		SubscriptionRequest tmp = new SubscriptionRequest();
		SubscriptionRequest originalPayload = originalSync.getRequest();
		tmp.setActive(originalPayload.isActive());
		tmp.setContext(deppCopyList(originalPayload.getContext()));
		tmp.setFinalPayload(deepCopyMap(originalPayload.getFinalPayload()));
		tmp.setTenant(originalPayload.getTenant());
		tmp.setId(originalPayload.getId());
		tmp.setRequestPayload(deepCopyMap(originalPayload.getRequestPayload()));
		tmp.setType(originalPayload.getRequestType());
		tmp.setSubscription(new Subscription(originalPayload.getSubscription()));
		return new SyncMessage(originalSync.getSyncId(), tmp);
	}

}
