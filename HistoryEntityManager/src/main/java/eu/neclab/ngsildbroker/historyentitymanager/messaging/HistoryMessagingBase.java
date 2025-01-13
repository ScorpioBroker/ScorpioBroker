package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpsertEntityRequest;
import eu.neclab.ngsildbroker.commons.interfaces.BaseRequestHandler;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
//import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;

public abstract class HistoryMessagingBase implements BaseRequestHandler {

	protected static Logger logger = LoggerFactory.getLogger(HistoryMessagingBase.class);
	private ConcurrentHashMap<String, ConcurrentLinkedQueue<BaseRequest>> tenant2Buffer = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, Long> tenant2LastReceived = new ConcurrentHashMap<>();

	@Inject
	HistoryEntityService historyService;

	@ConfigProperty(name = "scorpio.history.autorecording", defaultValue = "true")
	boolean autoRecording;
	
	@ConfigProperty(name = "scorpio.history.autorecordingthreadpoolsize", defaultValue = "1")
	int histRecordingThreadPooolSzie;
	
	@ConfigProperty(name = "scorpio.history.autorecordingbuffersize", defaultValue = "50000")
	int maxSize;

	int instancesNr = 1;
	int myInstancePos = 1;

	@Inject
	Vertx vertx;

	@Inject
	ObjectMapper objectMapper;
	
	@Inject
	MicroServiceUtils microServiceUtils;
	
	Executor histRecordingExecutor;
	
	@PostConstruct
	public void setup() {
		if(autoRecording) {
			histRecordingExecutor = Executors.newScheduledThreadPool(histRecordingThreadPooolSzie);
		}
		this.microServiceUtils.registerBaseRequestReceiver(this);
	}
	

	public Uni<Void> handleCsourceRaw(String byteMessage) {
		CSourceBaseRequest baseRequest;
		try {
			baseRequest = objectMapper.readValue(byteMessage, CSourceBaseRequest.class);
		} catch (JsonProcessingException e) {
			logger.error("failed to serialize message " + byteMessage, e);
			return Uni.createFrom().voidItem();
		}
		return baseHandleCsource(baseRequest);

	}

	public Uni<Void> handleEntityRaw(String byteMessage) {
		BaseRequest baseRequest;
		try {
			baseRequest = objectMapper.readValue(byteMessage, BaseRequest.class);
		} catch (JsonProcessingException e) {
			logger.error("failed to serialize message " + byteMessage, e);
			return Uni.createFrom().voidItem();
		}
		
		if (baseRequest.getRequestType() >= 30) {
			return handleBaseRequest(baseRequest);
		} else {
			return baseHandleEntity(baseRequest);
		}

	}

	public Uni<Void> baseHandleEntity(BaseRequest message) {
		logger.debug("retrieving " + message.getFirstId());
		if (!autoRecording || (instancesNr > 1 && message.hashCode() % instancesNr != myInstancePos)) {
			logger.debug("debug " + message.getFirstId());
			logger.debug("auto recording: " + autoRecording);
			logger.debug("instancesNr: " + instancesNr);
			logger.debug("myInstancePos: " + myInstancePos);
			logger.debug("message.hashCode() % instancesNr: " + (message.hashCode() % instancesNr));

			return Uni.createFrom().voidItem();
		}
		String tenant = message.getTenant();
		ConcurrentLinkedQueue<BaseRequest> buffer = tenant2Buffer.get(tenant);
		if (buffer == null) {
			buffer = new ConcurrentLinkedQueue<>();
			tenant2Buffer.put(message.getTenant(), buffer);
		}
		buffer.add(message);
		tenant2LastReceived.put(tenant, System.currentTimeMillis());
		logger.debug("history manager got called for entity: " + message.getIds());
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> handleBaseRequest(BaseRequest message) {
		logger.debug("retrieving message with id" + message.getIds());
		if (!autoRecording || (instancesNr > 1 && message.hashCode() % instancesNr != myInstancePos)) {
			logger.debug("discarding " + message.getIds());
			logger.debug("auto recording: " + autoRecording);
			logger.debug("instancesNr: " + instancesNr);
			logger.debug("myInstancePos: " + myInstancePos);
			return Uni.createFrom().voidItem();
		}

		if (message.getRequestType() != AppConstants.DELETE_REQUEST
				&& message.getRequestType() != AppConstants.BATCH_DELETE_REQUEST
				&& (message.getPayload() == null || message.getPayload().isEmpty())) {
			logger.debug("discarding because of none delete request and empty body");
			return Uni.createFrom().voidItem();
		}
		String tenant = message.getTenant();

		ConcurrentLinkedQueue<BaseRequest> buffer = tenant2Buffer.get(tenant);
		if (buffer == null) {
			buffer = new ConcurrentLinkedQueue<>();
			tenant2Buffer.put(tenant, buffer);
		}
		tenant2LastReceived.put(tenant, System.currentTimeMillis());
		if (message.getRequestType() == AppConstants.DELETE_REQUEST
				|| message.getRequestType() == AppConstants.BATCH_DELETE_REQUEST) {
			for (String entry : message.getIds()) {
				buffer.add(new DeleteEntityRequest(tenant, entry, false));
			}
		} else {
			for (List<Map<String, Object>> entry : message.getPayload().values()) {
				for (Map<String, Object> value : entry) {
					buffer.add(new UpsertEntityRequest(tenant, value, false));
				}
			}
		}
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> baseHandleCsource(CSourceBaseRequest message) {
		logger.debug("history manager got called for csource: " + message.getId());
		return historyService.handleRegistryChange(message);
	}

	void checkBuffer() {
		if (!autoRecording) {
			return;
		}
		List<Uni<Void>> unis = Lists.newArrayList();

		for (Entry<String, ConcurrentLinkedQueue<BaseRequest>> tenant2BufferEntry : tenant2Buffer.entrySet()) {
			ConcurrentLinkedQueue<BaseRequest> buffer = tenant2BufferEntry.getValue();
			String tenant = tenant2BufferEntry.getKey();

			Long lastReceived = tenant2LastReceived.get(tenant);
			if(lastReceived == null) {
				lastReceived = -1l;
			}

			if (buffer.size() >= maxSize || (lastReceived < System.currentTimeMillis() - 1000 && !buffer.isEmpty())) {
				Map<Integer, Map<String, List<Map<String, Object>>>> opType2Payload = Maps.newHashMap();
				List<BaseRequest> notBatch = Lists.newArrayList();
				while (!buffer.isEmpty()) {

					BaseRequest request = buffer.poll();

					if (request.getRequestType() == AppConstants.DELETE_ATTRIBUTE_REQUEST
							|| request.getRequestType() == AppConstants.REPLACE_ATTRIBUTE_REQUEST
							|| request.getRequestType() == AppConstants.REPLACE_ENTITY_REQUEST
							|| request.getRequestType() == AppConstants.MERGE_PATCH_REQUEST
							|| request.getRequestType() == AppConstants.PARTIAL_UPDATE_REQUEST) {
						notBatch.add(request);
						continue;
					}
					int regTypeToUse;

					switch (request.getRequestType()) {
					case AppConstants.UPSERT_REQUEST:
						regTypeToUse = AppConstants.BATCH_UPSERT_REQUEST;
						break;
					case AppConstants.CREATE_REQUEST:
						regTypeToUse = AppConstants.BATCH_CREATE_REQUEST;
						break;

					case AppConstants.APPEND_REQUEST:
					case AppConstants.UPDATE_REQUEST:
						regTypeToUse = AppConstants.BATCH_UPDATE_REQUEST;
						break;

					case AppConstants.MERGE_PATCH_REQUEST:
						regTypeToUse = AppConstants.BATCH_MERGE_REQUEST;
						break;

					case AppConstants.DELETE_REQUEST:
						regTypeToUse = AppConstants.BATCH_DELETE_REQUEST;
						break;

					default:
						continue;
					}

					Map<String, List<Map<String, Object>>> payloads = opType2Payload.get(regTypeToUse);
					if (payloads == null) {
						payloads = Maps.newHashMap();
						opType2Payload.put(regTypeToUse, payloads);
					}
					if (regTypeToUse == AppConstants.BATCH_DELETE_REQUEST) {
						for (String id : request.getIds()) {
							payloads.put(id, null);
						}
					} else {
						Map<String, List<Map<String, Object>>> payload = request.getPayload();
						for (Entry<String, List<Map<String, Object>>> pEntry : payload.entrySet()) {
							List<Map<String, Object>> tmp = payloads.get(pEntry.getKey());
							if (tmp == null) {
								tmp = Lists.newArrayList();
								payloads.put(pEntry.getKey(), tmp);
							}
							tmp.addAll(pEntry.getValue());
						}
					}

				}

				for (Entry<Integer, Map<String, List<Map<String, Object>>>> entry : opType2Payload.entrySet()) {
					if (entry.getKey() == AppConstants.BATCH_DELETE_REQUEST) {
						unis.add(historyService.handleInternalBatchRequest(
								new BatchRequest(tenant, entry.getValue().keySet(), null, entry.getKey(), false)));
					} else {
						unis.add(historyService.handleInternalBatchRequest(new BatchRequest(tenant,
								entry.getValue().keySet(), entry.getValue(), entry.getKey(), false)));
					}

				}
				for (BaseRequest entry : notBatch) {
					unis.add(historyService.handleInternalRequest(entry));
				}

			}
		}

		if (unis.isEmpty()) {
			return;
		}

		Uni.combine().all().unis(unis).with(list -> null).onItem().transformToUni(list -> {
			return Uni.createFrom().voidItem();
		}).runSubscriptionOn(histRecordingExecutor).subscribe().with(v -> {
			logger.debug("running hist recording on threadpool");
		});
	}

	public int getInstancesNr() {
		return instancesNr;
	}

	public void setInstancesNr(int instancesNr) {
		this.instancesNr = instancesNr;
	}

	public int getMyInstancePos() {
		return myInstancePos;
	}

	public void setMyInstancePos(int myInstancePos) {
		this.myInstancePos = myInstancePos;
	}

}