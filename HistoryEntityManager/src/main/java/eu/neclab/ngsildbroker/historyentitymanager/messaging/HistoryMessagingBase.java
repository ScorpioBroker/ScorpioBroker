package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpsertEntityRequest;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
//import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;

public abstract class HistoryMessagingBase {

	protected static Logger logger = LoggerFactory.getLogger(HistoryMessagingBase.class);
	private ConcurrentHashMap<String, ConcurrentLinkedQueue<BaseRequest>> tenant2Buffer = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, Long> tenant2LastReceived = new ConcurrentHashMap<>();

	@Inject
	HistoryEntityService historyService;

	@ConfigProperty(name = "scorpio.history.autorecording", defaultValue = "true")
	boolean autoRecording;
	@ConfigProperty(name = "scorpio.history.autorecordingbuffersize", defaultValue = "50000")
	int maxSize;
	private MessageCollector collector = new MessageCollector(this.getClass().getName());

	int instancesNr = 1;
	int myInstancePos = 1;

	@Inject
	Vertx vertx;

	@Inject
	ObjectMapper objectMapper;

	@Inject
	@Channel(AppConstants.HIST_SYNC_CHANNEL)
	MutinyEmitter<String> syncEmitter;

	private EventLoopGroup executor;

	Map<String, Long> instanceId2LastAnnouncement = Maps.newHashMap();

	protected String myInstanceId = UUID.randomUUID().toString();
	protected Map<String, Object> announcement = Map.of("instanceId", myInstanceId, "upOrDown", true);

	@PostConstruct
	public void setup() {
		this.executor = vertx.getDelegate().nettyEventLoopGroup();
	}

	CollectMessageListener collectListenerEntity = new CollectMessageListener() {

		@Override
		public void collected(String byteMessage) {
			BaseRequest message;
			try {
				message = objectMapper.readValue(byteMessage, BaseRequest.class);
			} catch (IOException e) {
				logger.error("failed to read sync message", e);
				return;
			}
			baseHandleEntity(message).runSubscriptionOn(executor).subscribe()
					.with(v -> logger.debug("done handling entity"));
		}
	};

	CollectMessageListener collectListenerBatchEntity = new CollectMessageListener() {

		@Override
		public void collected(String byteMessage) {
			BatchRequest message;
			try {
				message = objectMapper.readValue(byteMessage, BatchRequest.class);
			} catch (IOException e) {
				logger.error("failed to read sync message", e);
				return;
			}
			baseHandleBatch(message).runSubscriptionOn(executor).subscribe()
					.with(v -> logger.debug("done handling batch"));
		}
	};

	CollectMessageListener collectListenerRegistry = new CollectMessageListener() {

		@Override
		public void collected(String byteMessage) {
			BaseRequest message;
			try {
				message = objectMapper.readValue(byteMessage, BaseRequest.class);
			} catch (IOException e) {
				logger.error("failed to read sync message", e);
				return;
			}
			baseHandleCsource(message).runSubscriptionOn(executor).subscribe()
					.with(v -> logger.debug("done handling registry"));
		}
	};

	@ConfigProperty(name = "scorpio.history.syncchecktime", defaultValue = "5000")
	private long syncCheckTime;

	public Uni<Void> handleCsourceRaw(String byteMessage) {
		collector.collect(byteMessage, collectListenerRegistry);
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> handleEntityRaw(String byteMessage) {
		collector.collect(byteMessage, collectListenerEntity);
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> handleBatchEntitiesRaw(String byteMessage) {
		collector.collect(byteMessage, collectListenerBatchEntity);
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> baseHandleEntity(BaseRequest message) {

		if (!autoRecording || (instancesNr > 1 && message.hashCode() % instancesNr != myInstancePos)) {
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
		logger.debug("history manager got called for entity: " + message.getId());
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> baseHandleBatch(BatchRequest message) {
		if (!autoRecording || (instancesNr > 1 && message.hashCode() % instancesNr != myInstancePos)) {
			return Uni.createFrom().voidItem();
		}
		logger.debug("history manager batch handling got called: with ids: " + message.getEntityIds());
		if (message.getRequestType() != AppConstants.DELETE_REQUEST && message.getRequestPayload().isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		String tenant = message.getTenant();
		ConcurrentLinkedQueue<BaseRequest> buffer = tenant2Buffer.get(tenant);
		if (buffer == null) {
			buffer = new ConcurrentLinkedQueue<>();
			tenant2Buffer.put(message.getTenant(), buffer);
		}

		tenant2LastReceived.put(tenant, System.currentTimeMillis());
		if (message.getRequestType() == AppConstants.DELETE_REQUEST) {
			for (String entry : message.getEntityIds()) {
				buffer.add(new DeleteEntityRequest(tenant, entry));
			}
		} else {
			for (Map<String, Object> entry : message.getRequestPayload()) {
				buffer.add(new UpsertEntityRequest(tenant, entry));
			}
		}
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> baseHandleCsource(BaseRequest message) {
		logger.debug("history manager got called for csource: " + message.getId());
		return historyService.handleRegistryChange(message);
	}

	Uni<Void> checkBuffer() {
		if (!autoRecording) {
			return Uni.createFrom().voidItem();
		}
		List<Uni<Void>> unis = Lists.newArrayList();
		for (Entry<String, ConcurrentLinkedQueue<BaseRequest>> tenant2BufferEntry : tenant2Buffer.entrySet()) {
			ConcurrentLinkedQueue<BaseRequest> buffer = tenant2BufferEntry.getValue();
			String tenant = tenant2BufferEntry.getKey();
			Long lastReceived = tenant2LastReceived.get(tenant);
			if (buffer.size() >= maxSize || (lastReceived < System.currentTimeMillis() - 1000 && !buffer.isEmpty())) {
				Map<Integer, List<Map<String, Object>>> opType2Payload = Maps.newHashMap();
				List<BaseRequest> notBatch = Lists.newArrayList();
				while (!buffer.isEmpty()) {
					BaseRequest request = buffer.poll();
					if (request.getRequestType() == AppConstants.DELETE_ATTRIBUTE_REQUEST
							|| request.getRequestType() == AppConstants.REPLACE_ATTRIBUTE_REQUEST
							|| request.getRequestType() == AppConstants.REPLACE_ENTITY_REQUEST
							|| request.getRequestType() == AppConstants.MERGE_PATCH_REQUEST) {
						notBatch.add(request);
						continue;
					}
					List<Map<String, Object>> payloads = opType2Payload.get(request.getRequestType());
					if (payloads == null) {
						payloads = Lists.newArrayList();
						opType2Payload.put(request.getRequestType(), payloads);
					}
					Map<String, Object> payload = request.getPayload();
					if (payload == null) {
						payload = Maps.newHashMap();
					}
					payload.put(NGSIConstants.JSON_LD_ID, request.getId());
					payloads.add(payload);
				}
				for (Entry<Integer, List<Map<String, Object>>> entry : opType2Payload.entrySet()) {
					unis.add(historyService.handleInternalBatchRequest(
							new BatchRequest(tenant, entry.getValue(), null, entry.getKey())));
				}
				for (BaseRequest entry : notBatch) {
					unis.add(historyService.handleInternalRequest(entry));
				}

			}
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> null).onItem()
				.transformToUni(list -> Uni.createFrom().voidItem());
	}

	void purge() {
		collector.purge(30000);
	}

	Uni<Void> handleAnnouncement(String byteMessage) {
		Map<String, Object> announcement;
		try {
			announcement = objectMapper.readValue(byteMessage, Map.class);
		} catch (JsonProcessingException e) {
			return Uni.createFrom().voidItem();
		}
		boolean upOrDown = (boolean) announcement.get("upOrDown");
		String instanceId = (String) announcement.get("instanceId");
		if (upOrDown) {
			instanceId2LastAnnouncement.put(instanceId, System.currentTimeMillis());
		} else {
			instanceId2LastAnnouncement.remove(instanceId);
		}
		instancesNr = instanceId2LastAnnouncement.size();
		ArrayList<String> l1 = Lists.newArrayList(instanceId2LastAnnouncement.keySet());
		Collections.sort(l1);
		myInstancePos = l1.indexOf(myInstanceId) + 1;
		return Uni.createFrom().voidItem();
	}

	void checkInstances() {
		instanceId2LastAnnouncement.put(myInstanceId, System.currentTimeMillis());
		Iterator<Entry<String, Long>> it = instanceId2LastAnnouncement.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Long> next = it.next();
			if (next.getValue() < System.currentTimeMillis() - syncCheckTime) {
				it.remove();
			}
		}
	}

}