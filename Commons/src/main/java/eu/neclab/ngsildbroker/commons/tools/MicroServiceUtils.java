package eu.neclab.ngsildbroker.commons.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.DeflaterOutputStream;

@Singleton
public class MicroServiceUtils {
	private final static Logger logger = LoggerFactory.getLogger(MicroServiceUtils.class);

	@ConfigProperty(name = "scorpio.gatewayurl")
	String gatewayUrl;

	@ConfigProperty(name = "mysettings.gateway.port")
	int port;
	@ConfigProperty(name = "atcontext.url", defaultValue = "http://localhost:9090/ngsi-ld/v1/jsonldContexts/")
	String contextServerUrl;

	private static final Encoder base64Encoder = Base64.getEncoder();

	@PostConstruct
	void setup() {
		if (contextServerUrl.endsWith("ngsi-ld/v1/jsonldContexts")) {
			contextServerUrl = contextServerUrl + "/";
		} else if (!contextServerUrl.endsWith("ngsi-ld/v1/jsonldContexts/")) {
			if (contextServerUrl.endsWith("/")) {
				contextServerUrl = contextServerUrl + "/ngsi-ld/v1/jsonldContexts/";
			} else {
				contextServerUrl = contextServerUrl + "ngsi-ld/v1/jsonldContexts/";
			}
		}
	}

	public static void putIntoIdMap(Map<String, List<Map<String, Object>>> localEntities, String id,
			Map<String, Object> local) {
		List<Map<String, Object>> tmp = localEntities.get(id);
		if (tmp == null) {
			tmp = Lists.newArrayList();
			localEntities.put(id, tmp);
		}
		tmp.add(local);
	}



	public static void serializeAndSplitObjectAndEmit(Object obj, int maxMessageSize, MutinyEmitter<String> emitter,
			ObjectMapper objectMapper) throws ResponseException {
		if (obj instanceof BaseRequest br) {
			String base;
			try {
				base = objectMapper.writeValueAsString(br);
			} catch (JsonProcessingException e) {
				logger.error("Failed to serialize object", e);
				throw new ResponseException(ErrorType.InternalError, "Failed to serialize object");
			}
			logger.debug("attempting to send request with max message size " + maxMessageSize);
			base = base.substring(0, base.length() - 1);
			base += ",\"" + AppConstants.PAYLOAD_SERIALIZATION_CHAR + "\":[";
			String current = base;
			Map<String, List<Map<String, Object>>> payload = br.getPayload();
			Map<String, List<Map<String, Object>>> prevPayload = br.getPrevPayload();
			boolean zip = br.isZipped();
			List<String> toSend = Lists.newArrayList();
			if (payload != null) {
				boolean first = true;
				for (Entry<String, List<Map<String, Object>>> entry : payload.entrySet()) {
					String serializedPayload;
					String serializedPrevpayload;
					String id = entry.getKey();
					for (int i = 0; i < entry.getValue().size(); i++) {
						try {
							serializedPayload = objectMapper.writeValueAsString(entry.getValue().get(i));
						} catch (JsonProcessingException e) {
							logger.error("Failed to serialize object", e);
							throw new ResponseException(ErrorType.InternalError, "Failed to serialize object");
						}
						if (prevPayload != null) {
							List<Map<String, Object>> prev = prevPayload.get(id);
							if (prev != null) {
								if (i < prev.size()) {
									Map<String, Object> prevValue = prev.get(i);
									try {
										serializedPrevpayload = objectMapper.writeValueAsString(prevValue);
									} catch (JsonProcessingException e) {
										logger.error("Failed to serialize object", e);
										throw new ResponseException(ErrorType.InternalError,
												"Failed to serialize object");
									}
								} else {
									serializedPrevpayload = "null";
								}
							} else {
								serializedPrevpayload = "null";
							}
						} else {
							serializedPrevpayload = "null";
						}

						if (zip) {

							try {
								serializedPayload = base64Encoder.encodeToString(zip(serializedPayload));
							} catch (IOException e) {
								throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
							}
							try {
								serializedPrevpayload = base64Encoder.encodeToString(zip(serializedPrevpayload));
							} catch (IOException e) {
								throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
							}
						}
						int messageLength = current.getBytes().length + id.getBytes().length + serializedPayload.getBytes().length
								+ serializedPrevpayload.getBytes().length + 18;
						logger.debug("message size after adding payload would be " + maxMessageSize);
						if (messageLength > maxMessageSize) {
							if (first) {
								throw new ResponseException(ErrorType.RequestEntityTooLarge);
							}
							logger.debug("finalizing message");
							current = current.substring(0, current.length() - 1) + "]}";
							logger.debug("finale messagesize: " + current.getBytes().length);
							toSend.add(current);
							current = base + "\"" + id + "\",";
							if (zip) {
								current += "\"";
							}
							current += serializedPayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
							if (zip) {
								current += "\"";
							}
							current += serializedPrevpayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
							first = true;
						} else if (messageLength == maxMessageSize) {
							logger.debug("finalizing message");
							current = current.substring(0, current.length() - 1) + "]}";
							logger.debug("finale messagesize: " + current.getBytes().length);
							toSend.add(current);
							current = base;
							first = true;
						} else {
							current += "\"" + id + "\",";
							if (zip) {
								current += "\"";
							}
							current += serializedPayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
							if (zip) {
								current += "\"";
							}
							current += serializedPrevpayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
						}
					}
					first = false;
				}
				if (current.length() != base.length()) {
					logger.debug("finalizing message");
					current = current.substring(0, current.length() - 1) + "]}";
					logger.debug("finale messagesize: " + current.getBytes().length);
					toSend.add(current);
					current = base;
				}
			} else if (prevPayload != null) {
				boolean first = true;
				for (Entry<String, List<Map<String, Object>>> entry : prevPayload.entrySet()) {
					String serializedPayload = "null";
					String serializedPrevpayload;
					String id = entry.getKey();
					for (Map<String, Object> mapEntry : entry.getValue()) {
						try {
							serializedPrevpayload = objectMapper.writeValueAsString(mapEntry);
						} catch (JsonProcessingException e) {
							logger.error("Failed to serialize object", e);
							throw new ResponseException(ErrorType.InternalError, "Failed to serialize object");
						}

						if (zip) {

							try {
								serializedPayload = base64Encoder.encodeToString(zip(serializedPayload));
							} catch (IOException e) {
								throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
							}
							try {
								serializedPrevpayload = base64Encoder.encodeToString(zip(serializedPrevpayload));
							} catch (IOException e) {
								throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
							}
						}
						int messageLength = current.getBytes().length + id.getBytes().length + serializedPayload.getBytes().length
								+ serializedPrevpayload.getBytes().length + 18;
						logger.debug("message size after adding payload would be " + maxMessageSize);
						if (messageLength > maxMessageSize) {
							if (first) {
								throw new ResponseException(ErrorType.RequestEntityTooLarge);
							}
							logger.debug("finalizing message only prevpayload");
							current = current.substring(0, current.length() - 1) + "]}";
							logger.debug("finale messagesize only prevpayload: " + current.getBytes().length);
							toSend.add(current);
							current = base + "\"" + id + "\",";
							if (zip) {
								current += "\"";
							}
							current += serializedPayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
							if (zip) {
								current += "\"";
							}
							current += serializedPrevpayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
							first = true;
						} else if (messageLength == maxMessageSize) {
							logger.debug("finalizing message only prevpayload");
							current = current.substring(0, current.length() - 1) + "]}";
							logger.debug("finale messagesize only prevpayload: " + current.getBytes().length);
							toSend.add(current);
							current = base;
							first = true;
						} else {
							current += "\"" + id + "\",";
							if (zip) {
								current += "\"";
							}
							current += serializedPayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
							if (zip) {
								current += "\"";
							}
							current += serializedPrevpayload;
							if (zip) {
								current += "\"";
							}
							current += ",";
						}
					}
					first = false;
				}
				if (current.length() != base.length()) {
					logger.debug("finalizing message only prevpayload");
					current = current.substring(0, current.length() - 1) + "]}";
					logger.debug("finale messagesize only prevpayload: " + current.getBytes().length);
					toSend.add(current);
					current = base;
				}
			} else if (br.getIds() != null) {
				boolean first = true;
				for (String entry : br.getIds()) {
					String serializedPayload = "null";
					String serializedPrevpayload = "null";
					String id = entry;

					if (zip) {

						try {
							serializedPayload = base64Encoder.encodeToString(zip(serializedPayload));
						} catch (IOException e) {
							throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
						}
						try {
							serializedPrevpayload = base64Encoder.encodeToString(zip(serializedPrevpayload));
						} catch (IOException e) {
							throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
						}
					}
					int messageLength = current.getBytes().length + id.getBytes().length + serializedPayload.getBytes().length
							+ serializedPrevpayload.getBytes().length + 18;
					logger.debug("message size after adding payload would be " + maxMessageSize);
					if (messageLength > maxMessageSize) {
						if (first) {
							throw new ResponseException(ErrorType.RequestEntityTooLarge);
						}
						logger.debug("finalizing message only ids");
						current = current.substring(0, current.length() - 1) + "]}";
						logger.debug("finale messagesize only ids: " + current.getBytes().length);
						toSend.add(current);
						current = base + "\"" + id + "\",";
						if (zip) {
							current += "\"";
						}
						current += serializedPayload;
						if (zip) {
							current += "\"";
						}
						current += ",";
						if (zip) {
							current += "\"";
						}
						current += serializedPrevpayload;
						if (zip) {
							current += "\"";
						}
						current += ",";
						first = true;
					} else if (messageLength == maxMessageSize) {
						logger.debug("finalizing message only ids");
						current = current.substring(0, current.length() - 1) + "]}";
						logger.debug("finale messagesize only ids: " + current.getBytes().length);
						toSend.add(current);
						current = base;
						first = true;
					} else {
						current += "\"" + id + "\",";
						if (zip) {
							current += "\"";
						}
						current += serializedPayload;
						if (zip) {
							current += "\"";
						}
						current += ",";
						if (zip) {
							current += "\"";
						}
						current += serializedPrevpayload;
						if (zip) {
							current += "\"";
						}
						current += ",";
					}
					first = false;
				}
				if (current.length() != base.length()) {
					logger.debug("finalizing message only ids");
					current = current.substring(0, current.length() - 1) + "]}";
					logger.debug("finale messagesize only ids: " + current.getBytes().length);
					toSend.add(current);
					current = base;
				}
			} else {
				throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
			}
			toSend.forEach(entry -> {
				logger.debug("sending entry of size: " + entry.getBytes().length);
				emitter.sendAndForget(entry);
			});

		} else {
			String data;
			try {
				data = objectMapper.writeValueAsString(obj);
			} catch (JsonProcessingException e) {
				throw new ResponseException(ErrorType.InternalError, "Failed to serialize object");
			}
			emitter.sendAndForget(data);
		}

	}

	private static byte[] zip(String data) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		DeflaterOutputStream deflateOut = new DeflaterOutputStream(byteArrayOutputStream);
		deflateOut.write(data.getBytes());
		deflateOut.flush();
		deflateOut.close();
		byte[] tmp = byteArrayOutputStream.toByteArray();
		byteArrayOutputStream.close();
		return tmp;
	}

	public static List<String> splitStringByByteLength(String src, int maxsize) {
		String id = String.format("%020d", src.hashCode() * System.currentTimeMillis());
		logger.debug("Splitting into size " + maxsize);
		logger.debug(src);
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
		result.set(0, "#" + id + String.format("%011d", i) + result.get(0).substring(32));
		result.set(result.size() - 1, "%" + result.get(result.size() - 1).substring(1));
		return result;
	}

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

	public static Map<String, List<Map<String, Object>>> deepCopyIdMap(
			Map<String, List<Map<String, Object>>> original) {
		Map<String, List<Map<String, Object>>> result = new HashMap<>(original.size());
		original.forEach((key, value) -> {
			List<Map<String, Object>> tmp = new ArrayList<>(value.size());
			value.forEach(map -> {
				tmp.add(deepCopyMap(map));
			});
			result.put(key, tmp);
		});
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
			} else if (originalValue instanceof Long) {
				copiedValue = ((Long) originalValue).longValue();
			} else if (originalValue instanceof Float) {
				copiedValue = ((Float) originalValue).floatValue();
			} else if (originalValue instanceof Double) {
				copiedValue = ((Double) originalValue).doubleValue();
			} else if (originalValue instanceof Boolean) {
				copiedValue = ((Boolean) originalValue).booleanValue();
			} else if (originalValue == null) {
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
	public URI getContextServerURL() {
		logger.trace("getContextServerURL :: started");
		try {
			return new URI(contextServerUrl);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}

	}
}
