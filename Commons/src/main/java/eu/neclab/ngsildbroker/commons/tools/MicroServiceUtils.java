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

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
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

	public static void main(String[] args) throws Exception {

	}

	public static void serializeAndSplitObjectAndEmit(Object obj, int messageSize, MutinyEmitter<String> emitter,
			ObjectMapper objectMapper) {
		String data;
		try {
			data = objectMapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			logger.error("Failed to serialize object", e);
			return;
		}
		int size = data.getBytes().length;

		List<String> result;
		int id = data.hashCode();
		if (size <= messageSize) {
			result = new ArrayList<>(1);
			result.add(data);
		} else {
			int messageSizeToUse = messageSize - 23;
			result = splitStringByByteLength(data, messageSizeToUse);
		}
		for (String message : result) {
			emitter.sendAndForget(message);
		}
	}

	public static List<String> splitStringByByteLength(String src, int maxsize) {
		String id = String.format("%011d", src.hashCode());
		Charset cs = Charset.forName("UTF-16");
		CharsetEncoder coder = cs.newEncoder();
		ByteBuffer out = ByteBuffer.allocate(maxsize); // output buffer of required size
		CharBuffer in = CharBuffer.wrap(src);
		List<String> result = new ArrayList<>(); // a list to store the chunks
		int pos = 0;
		int i = 0;
		while (true) {
			CoderResult cr = coder.encode(in, out, true); // try to encode as much as possible
			int newpos = src.length() - in.length();
			String posS = String.format("%011d", i);
			String s = "$" + id + posS + src.substring(pos, newpos);
			i++;
			result.add(s); // add what has been encoded to the list
			pos = newpos; // store new input position
			out.rewind(); // and rewind output buffer
			if (!cr.isOverflow()) {
				break; // everything has been encoded
			}
		}
		result.set(0, "#" + id + String.format("%011d", i) + result.get(0).substring(23));
		result.set(result.size() -  1, "%" + result.get(result.size() - 1).substring(1));
		return result;
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
