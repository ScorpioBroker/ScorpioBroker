package eu.neclab.ngsildbroker.commons.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Singleton;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

	public static void serializeAndSplitObjectAndEmit(Object obj, int messageSize, MutinyEmitter<String> emitter,
			ObjectMapper objectMapper) {
		byte[] data;
		try {
			data = objectMapper.writeValueAsBytes(obj);
		} catch (JsonProcessingException e) {
			logger.error("Failed to serialize object", e);
			return;
		}
		int size = data.length;
		List<byte[]> result;
		int id = data.hashCode();
		if (size <= messageSize) {
			result = new ArrayList<>(1);
			result.add(data);
		} else {
			int messageSizeToUse = messageSize - 9;
			int chunks = (int) Math.ceil((double) size / messageSizeToUse);
			result = new ArrayList<>(chunks);
			for (int i = 0; i < chunks; i++) {
				int from = i * messageSizeToUse + ((i + 1) * 9);
				int length = Math.min(messageSizeToUse, size - from);
				int to = Math.min(from + messageSizeToUse, size);
				ByteBuffer buffer = ByteBuffer.allocate(length + 9);
				if (i == 0) {
					buffer.put((byte) '#');
					buffer.putInt(id);
					buffer.putInt(chunks);
				} else if (i + 1 < chunks) {
					buffer.put((byte) '$');
					buffer.putInt(id);
					buffer.putInt(i);
				} else {
					buffer.put((byte) '%');
					buffer.putInt(id);
					buffer.putInt(i);
				}
				buffer.put(Arrays.copyOfRange(data, from, to));
				result.add(buffer.array());
			}
		}

		for (byte[] message : result) {
			emitter.sendAndForget(new String(message));
		}
	}

	public static BaseRequest deepCopyRequestMessage(BaseRequest originalPayload) {
		BaseRequest result;
		switch (originalPayload.getRequestType()) {
		case AppConstants.DELETE_ATTRIBUTE_REQUEST:
			result = new DeleteAttributeRequest();
			result.setPreviousEntity(originalPayload.getPreviousEntity());
			result.setAttribName(originalPayload.getAttribName());
			result.setDatasetId(originalPayload.getDatasetId());
			result.setDeleteAll(originalPayload.isDeleteAll());
			break;
		default:
			result = new BaseRequest();

		}
		result.setId(originalPayload.getId());
		result.setPayload(deepCopyMap(originalPayload.getPayload()));
		result.setTenant(originalPayload.getTenant());
		result.setRequestType(originalPayload.getRequestType());
		result.setPreviousEntity(deepCopyMap(originalPayload.getPreviousEntity()));
		return result;
	}

	public static BatchRequest deepCopyRequestMessage(BatchRequest originalPayload) {
		List<Map<String, Object>> copiedPayload = new ArrayList<>();
		if (originalPayload.getRequestPayload() != null && !originalPayload.getRequestPayload().isEmpty()) {
			for (Map<String, Object> entry : originalPayload.getRequestPayload()) {
				copiedPayload.add(deepCopyMap(entry));
			}
		}
		BatchRequest result = new BatchRequest(originalPayload.getTenant(), copiedPayload,
				originalPayload.getContexts(), originalPayload.getRequestType());
		result.setEntityIds(originalPayload.getEntityIds());
		return result;
	}

	public static Map<String, Object> deepCopyMap(Map<String, Object> original) {
		if (original == null) {
			return null;
		}
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
			} else if (originalValue == null) {
				// System.out.println(entry.getKey() + " was null");
				continue;
			} else {
				copiedValue = originalValue.toString();
			}
			result.put(entry.getKey(), copiedValue);
		}

		return result;
	}

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

	public static SubscriptionRequest deepCopySubscriptionMessage(SubscriptionRequest originalPayload) {
		SubscriptionRequest result = new SubscriptionRequest();
		result.setContext(originalPayload.getContext());
		result.setId(originalPayload.getId());
		if (originalPayload.getPayload() != null) {
			result.setPayload(deepCopyMap(originalPayload.getPayload()));
		}
		result.setTenant(originalPayload.getTenant());
		result.setRequestType(originalPayload.getRequestType());
		return result;
	}

	public static HeadersMultiMap getHeaders(ArrayListMultimap<String, String> receiverInfo) {
		HeadersMultiMap result = new HeadersMultiMap();
		for (Entry<String, String> entry : receiverInfo.entries()) {
			result.add(entry.getKey(), entry.getValue());
		}
		return result;
	}

//	public static SyncMessage deepCopySyncMessage(SyncMessage originalSync) {
//		SubscriptionRequest tmp = new SubscriptionRequest();
//		SubscriptionRequest originalPayload = originalSync.getRequest();
//		tmp.setActive(originalPayload.isActive());
//		tmp.setContext(deppCopyList(originalPayload.getContext()));
//		tmp.setPayload(deepCopyMap(originalPayload.getPayload()));
//		tmp.setHeaders(ArrayListMultimap.create(originalPayload.getHeaders()));
//		tmp.setId(originalPayload.getId());
//		tmp.setType(originalPayload.getRequestType());
//		tmp.setSubscription(new Subscription(originalPayload.getSubscription()));
//		return new SyncMessage(originalSync.getSyncId(), tmp);
//	}

}
