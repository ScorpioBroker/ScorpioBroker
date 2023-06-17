package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.common.annotation.RunOnVirtualThread;
//import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.smallrye.mutiny.Uni;

public abstract class HistoryMessagingBase {

	protected static Logger logger = LoggerFactory.getLogger(HistoryMessagingBase.class);
	private ConcurrentHashMap<String, ConcurrentLinkedQueue<BaseRequest>> tenant2Buffer = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, Long> tenant2LastReceived = new ConcurrentHashMap<>();
	private int maxSize = 20000;

	@Inject
	HistoryEntityService historyService;

	public Uni<Void> baseHandleEntity(BaseRequest message) {
		String tenant = message.getTenant();
		ConcurrentLinkedQueue<BaseRequest> buffer = tenant2Buffer.get(message.getTenant());
		if (buffer == null) {
			buffer = new ConcurrentLinkedQueue<>();
			tenant2Buffer.put(message.getTenant(), buffer);
		}
		buffer.add(message);
		tenant2LastReceived.put(tenant, System.currentTimeMillis());
		logger.debug("history manager got called for entity: " + message.getId());
		// return historyService.handleInternalRequest(message);
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> baseHandleBatch(BatchRequest message) {
		logger.debug("history manager batch handling got called");
		return historyService.handleInternalBatchRequest(message);
		// return Uni.createFrom().voidItem();
	}

	public Uni<Void> baseHandleCsource(BaseRequest message) {
		logger.debug("history manager got called for csource: " + message.getId());
		return historyService.handleRegistryChange(message);
	}

	
	Uni<Void> checkBuffer() {
		List<Uni<Void>> unis = Lists.newArrayList();
		for (Entry<String, ConcurrentLinkedQueue<BaseRequest>> tenant2BufferEntry : tenant2Buffer.entrySet()) {
			ConcurrentLinkedQueue<BaseRequest> buffer = tenant2BufferEntry.getValue();
			String tenant = tenant2BufferEntry.getKey();
			Long lastReceived = tenant2LastReceived.get(tenant);
			if (buffer.size() >= maxSize || (lastReceived < System.currentTimeMillis() - 1000 && !buffer.isEmpty())) {
				Map<Integer, List<Map<String, Object>>> opType2Payload = Maps.newHashMap();
				while (!buffer.isEmpty()) {
					BaseRequest request = buffer.poll();
					List<Map<String, Object>> payloads = opType2Payload.get(request.getRequestType());
					if (payloads == null) {
						payloads = Lists.newArrayList();
						opType2Payload.put(request.getRequestType(), payloads);
					}
					payloads.add(request.getPayload());
				}
				for (Entry<Integer, List<Map<String, Object>>> entry : opType2Payload.entrySet()) {
					unis.add(historyService.handleInternalBatchRequest(
							new BatchRequest(tenant, entry.getValue(), null, entry.getKey())));
				}

			}
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> null).onItem()
				.transformToUni(list -> Uni.createFrom().voidItem());
	}

}
