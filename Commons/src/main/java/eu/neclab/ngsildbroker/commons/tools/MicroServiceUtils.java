package eu.neclab.ngsildbroker.commons.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.BaseRequestHandler;
import eu.neclab.ngsildbroker.commons.interfaces.CSourceHandler;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MyByteArrayBuilder;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.pgclient.PgException;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.configuration.ConfigUtils;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

@Singleton
public class MicroServiceUtils {
	private final static Logger logger = LoggerFactory.getLogger(MicroServiceUtils.class);

	//private final static Charset UTF8_CHARSET = Charset.forName("UTF-8");

	@ConfigProperty(name = "scorpio.gatewayurl")
	String gatewayUrl;

	@ConfigProperty(name = "mysettings.gateway.port")
	int port;
	@ConfigProperty(name = "atcontext.url", defaultValue = "http://localhost:9090/ngsi-ld/v1/jsonldContexts/")
	String contextServerUrl;

	private boolean inMemoryActive = ConfigUtils.isProfileActive("in-memory");
	

	List<CSourceHandler> csourceReceivers = Lists.newArrayList();
	
	List<BaseRequestHandler> baseRequestReceivers = Lists.newArrayList();
	
	
	private static final Encoder base64Encoder = Base64.getEncoder();
	public static final byte[] NULL_ARRAY = "null".getBytes();
	private static byte[] ZIPPED_NULL_ARRAY;

	private static final byte[] BASE_APPENDIX = (",\"" + AppConstants.PAYLOAD_SERIALIZATION_CHAR + "\":[").getBytes();
	private static final byte[] FINALIZER = "]}".getBytes();
	private static final int FINALIZER_lENGTH = FINALIZER.length - 1;

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

	public void serializeAndSplitObjectAndEmit(Object obj, int maxMessageSize, MutinyEmitter<String> emitter,
			ObjectMapper objectMapper) throws ResponseException {
		if(inMemoryActive) {
			sendObjectInMemory(obj);
			return;
		}
		if (obj instanceof BaseRequest br) {
			Map<String, List<Map<String, Object>>> payload = br.getPayload();
			Map<String, List<Map<String, Object>>> prevPayload = br.getPrevPayload();
			Set<String> ids = br.getIds();
			int initialLength;
			int entrySize;
			if ((payload == null || payload.isEmpty()) && (prevPayload == null || prevPayload.isEmpty())) {
				entrySize = ids.size();
				initialLength = entrySize * 500 + 150;
			} else if ((prevPayload == null || prevPayload.isEmpty())) {
				entrySize = payload.values().size();
				initialLength = entrySize * 3500 + 150;
			} else if ((payload == null || payload.isEmpty())) {
				entrySize = prevPayload.values().size();
				initialLength = entrySize * 8500 + 150;
			} else {
				entrySize = payload.values().size();
				initialLength = entrySize * 17000 + 150;
			}
			if (initialLength > maxMessageSize) {
				initialLength = maxMessageSize;
			}
			MyByteArrayBuilder current = new MyByteArrayBuilder(initialLength, entrySize, maxMessageSize);

			try {
				objectMapper.writeValue(current, br);
			} catch (Exception e) {
				logger.error("Failed to serialize object", e);
				throw new ResponseException(ErrorType.InternalError, "Failed to serialize object");
			}
			current.reduceByOne();
			// logger.debug("attempting to send request with max message size " +
			// maxMessageSize);

			current.write(BASE_APPENDIX);
			int baseLength = current.size();
			current.setBaseRollback();

			boolean zip = br.isZipped();
			List<byte[]> toSend = Lists.newArrayList();
			if (payload != null) {
				boolean first = true;

				for (Entry<String, List<Map<String, Object>>> entry : payload.entrySet()) {

					String id = entry.getKey();
					byte[] idBytes = id.getBytes();
					for (int i = 0; i < entry.getValue().size(); i++) {
						current.nextItem();
						current.write('"');
						current.write(idBytes);
						current.write('"');
						current.write(',');
						try {
							if (zip) {
								current.write('"');
								current.write(base64Encoder
										.encode(zip(objectMapper.writeValueAsBytes(entry.getValue().get(i)))));
								current.write('"');
							} else {
								objectMapper.writeValue(current, entry.getValue().get(i));
							}
						} catch (Exception e) {
							logger.error("Failed to serialize object", e);
							throw new ResponseException(ErrorType.InternalError, "Failed to serialize object");
						}
						current.write(',');
						if (prevPayload != null) {
							List<Map<String, Object>> prev = prevPayload.get(id);
							if (prev != null) {
								if (i < prev.size()) {
									Map<String, Object> prevValue = prev.get(i);
									try {
										if (zip) {
											current.write('"');
											current.write(base64Encoder
													.encode(zip(objectMapper.writeValueAsBytes(prevValue))));
											current.write('"');
										} else {
											objectMapper.writeValue(current, prevValue);
										}

									} catch (Exception e) {
										logger.error("Failed to serialize object", e);
										throw new ResponseException(ErrorType.InternalError,
												"Failed to serialize object");
									}
								} else {
									if (zip) {
										current.write(getZippedNullArray());
									} else {
										current.write(NULL_ARRAY);
									}
								}
							} else {
								if (zip) {
									current.write(getZippedNullArray());
								} else {
									current.write(NULL_ARRAY);
								}
							}
						} else {
							if (zip) {
								current.write(getZippedNullArray());
							} else {
								current.write(NULL_ARRAY);
							}
						}
						current.write(',');
						int currentSize = current.size();
						int messageLength = currentSize + FINALIZER_lENGTH;
						// logger.debug("message size after adding payload would be " + messageLength);
						if (messageLength > maxMessageSize) {
							if (first) {
								throw new ResponseException(ErrorType.RequestEntityTooLarge);
							}
							// logger.debug("finalizing message");
							toSend.add(current.rollback());
							// current = current.substring(0, current.length() - 1) + "]}";

						} else if (messageLength == maxMessageSize) {
							toSend.add(current.finalizeMessage());
							// logger.debug("finalizing message");
						}
					}
					first = false;
				}
				if (current.size() != baseLength) {
					toSend.add(current.finalizeMessage());
				}
			} else if (prevPayload != null) {
				boolean first = true;

				for (Entry<String, List<Map<String, Object>>> entry : prevPayload.entrySet()) {

					String id = entry.getKey();
					byte[] idBytes = id.getBytes();
					for (Map<String, Object> mapEntry : entry.getValue()) {
						current.nextItem();
						current.write('"');
						current.write(idBytes);
						current.write('"');
						current.write(',');
						if (zip) {
							current.write(getZippedNullArray());
						} else {
							current.write(NULL_ARRAY);
						}
						current.write(',');
						try {
							if (zip) {
								current.write('"');
								current.write(base64Encoder.encode(zip(objectMapper.writeValueAsBytes(mapEntry))));
								current.write('"');
							} else {
								objectMapper.writeValue(current, mapEntry);
							}
						} catch (Exception e) {
							logger.error("Failed to serialize object", e);
							throw new ResponseException(ErrorType.InternalError, "Failed to serialize object");
						}
						current.write(',');
						int currentSize = current.size();
						int messageLength = currentSize + FINALIZER_lENGTH;
						// logger.debug("message size after adding payload would be " + messageLength);
						if (messageLength > maxMessageSize) {
							if (first) {
								throw new ResponseException(ErrorType.RequestEntityTooLarge);
							}
							// logger.debug("finalizing message");
							toSend.add(current.rollback());
							// current = current.substring(0, current.length() - 1) + "]}";

						} else if (messageLength == maxMessageSize) {
							toSend.add(current.finalizeMessage());
							// logger.debug("finalizing message");
						}
					}
					first = false;
				}
				if (current.size() != baseLength) {
					toSend.add(current.finalizeMessage());
				}
			} else if (ids != null) {
				boolean first = true;
				for (String entry : ids) {

					byte[] id = entry.getBytes();
					current.nextItem();
					current.write('"');
					current.write(id);
					current.write('"');
					current.write(',');
					if (zip) {
						current.write(getZippedNullArray());
						current.write(',');
						current.write(getZippedNullArray());
					} else {
						current.write(NULL_ARRAY);
						current.write(',');
						current.write(NULL_ARRAY);
					}
					current.write(',');
					// logger.debug("message size after adding payload would be " + maxMessageSize);
					int currentSize = current.size();
					int messageLength = currentSize + FINALIZER_lENGTH;
					// logger.debug("message size after adding payload would be " + messageLength);
					if (messageLength > maxMessageSize) {
						if (first) {
							throw new ResponseException(ErrorType.RequestEntityTooLarge);
						}
						// logger.debug("finalizing message");
						toSend.add(current.rollback());
						// current = current.substring(0, current.length() - 1) + "]}";

					} else if (messageLength == maxMessageSize) {
						toSend.add(current.finalizeMessage());
						// logger.debug("finalizing message");
					}

					first = false;
				}
				if (current.size() != baseLength) {
					toSend.add(current.finalizeMessage());
				}
			} else {
				throw new ResponseException(ErrorType.InternalError, "Failed to compress prevpayload");
			}
			toSend.forEach(entry -> {
				// logger.debug("sending entry of size: " + entry.getBytes().length);
				emitter.sendAndForget(new String(entry));
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

	private void sendObjectInMemory(Object obj) {
		
		if(obj instanceof BaseRequest br) {
			baseRequestReceivers.get(0).handleBaseRequest(br).subscribe().with(x -> {});
			for(int i = 1; i < baseRequestReceivers.size(); i++) {
				baseRequestReceivers.get(1).handleBaseRequest(br.copy()).subscribe().with(x -> {});
			}
		}else if(obj instanceof CSourceBaseRequest cr) {
			csourceReceivers.get(0).handleRegistryChange(cr).subscribe().with(x -> {});
			for(int i = 1; i < csourceReceivers.size(); i++) {
				csourceReceivers.get(i).handleRegistryChange(cr).subscribe().with(x -> {});
			}
		}
		
	}

	

	public static byte[] getZippedNullArray() {
		if (ZIPPED_NULL_ARRAY == null) {
			try {
				byte[] tmp = base64Encoder.encode(zip("null".getBytes()));
				ZIPPED_NULL_ARRAY = new byte[tmp.length + 2];
				ZIPPED_NULL_ARRAY[0] = '"';
				System.arraycopy(tmp, 0, ZIPPED_NULL_ARRAY, 1, tmp.length);
				ZIPPED_NULL_ARRAY[ZIPPED_NULL_ARRAY.length - 1] = '"';
			} catch (IOException e) {
				// left empty intentionally this should never happen
			}
		}
		return ZIPPED_NULL_ARRAY;
	}

	private static byte[] zip(byte[] data) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		DeflaterOutputStream deflateOut = new DeflaterOutputStream(byteArrayOutputStream);
		deflateOut.write(data);
		deflateOut.flush();
		deflateOut.close();
		byte[] tmp = byteArrayOutputStream.toByteArray();
		byteArrayOutputStream.close();
		return tmp;
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

	@SuppressWarnings("unchecked")
	public static Map<String, Object> deepCopyMap(Map<String, Object> original) {
		if (original == null) {
			return null;
		}
		Map<String, Object> result = Maps.newHashMap();
		for (Entry<String, Object> entry : original.entrySet()) {
			Object copiedValue;
			Object originalValue = entry.getValue();
			if (originalValue instanceof List<?> l) {
				copiedValue = deppCopyList(l);
			} else if (originalValue instanceof Map<?, ?> m) {
				copiedValue = deepCopyMap((Map<String, Object>) m);
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

	@SuppressWarnings("unchecked")
	public static List<Object> deppCopyList(List<?> l) {
		if (l == null) {
			return null;
		}
		List<Object> result = Lists.newArrayList();
		for (Object originalValue : l) {
			Object copiedValue;
			if (originalValue instanceof List<?> l1) {
				copiedValue = deppCopyList(l1);
			} else if (originalValue instanceof Map<?, ?> m) {
				copiedValue = deepCopyMap((Map<String, Object>) m);
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

	public static void logPGE(PgException pge, Logger logger) {
		logger.debug(pge.getSqlState());
		logger.debug(pge.getDetail());
		logger.debug(pge.getErrorMessage());
		logger.debug(pge.getErrorCode() + "");
		logger.debug(pge.getDataType());
		logger.debug(pge.getFile());
		logger.debug(pge.getHint());
		logger.debug(pge.getInternalPosition());
		logger.debug(pge.getInternalQuery());
		logger.debug(pge.getPosition());
		logger.debug(pge.getWhere());
		logger.debug(pge.getTable());
		logger.debug(pge.getRoutine());

	}

	public static int getZippedNullArrayLength() {
		return getZippedNullArray().length;
	}

	public static byte[] unzip(byte[] zippedData) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		InflaterOutputStream inflater = new InflaterOutputStream(baos);
		inflater.write(zippedData);
		inflater.flush();
		inflater.close();
		baos.flush();
		byte[] payloadBytes = baos.toByteArray();
		baos.close();
		return payloadBytes;

	}
	
	public void registerCSourceReceiver(CSourceHandler handler) {
		csourceReceivers.add(handler);
	}
	
	public void registerBaseRequestReceiver(BaseRequestHandler handler) {
		baseRequestReceivers.add(handler);
	}

}
