package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.netty.channel.EventLoopGroup;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
//import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.smallrye.mutiny.Uni;
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

	@Inject
	Vertx vertx;

	@Inject
	ObjectMapper objectMapper;

	private EventLoopGroup executor;

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
		if (!autoRecording) {
			return Uni.createFrom().voidItem();
		}
		String tenant = message.getTenant();
		ConcurrentLinkedQueue<BaseRequest> buffer = tenant2Buffer.get(message.getTenant());
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
		if (!autoRecording) {
			return Uni.createFrom().voidItem();
		}
		logger.debug("history manager batch handling got called: with ids: " + message.getEntityIds());
		if (message.getRequestType() != AppConstants.DELETE_REQUEST && message.getRequestPayload().isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		return historyService.handleInternalBatchRequest(message);
	}

	public Uni<Void> baseHandleCsource(BaseRequest message) {
		logger.debug("history manager got called for csource: " + message.getId());
		return historyService.handleRegistryChange(message);
	}

	@Scheduled(every = "5s")
	@RunOnVirtualThread
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

}