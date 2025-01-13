package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;

public abstract class HistorySync {

	private static final Logger logger = LoggerFactory.getLogger(HistorySync.class);
	@Inject
	@Channel(AppConstants.HIST_SYNC_CHANNEL)
	@Broadcast
	MutinyEmitter<String> syncEmitter;

	@Inject
	ObjectMapper objectMapper;

	@Inject
	HistoryMessagingBase historyMessaging;
	
	@Inject
	MicroServiceUtils microServiceUtils;
	
	@ConfigProperty(name = "scorpio.history.syncchecktime", defaultValue = "5000")
	private long syncCheckTime;

	Map<String, Long> instanceId2LastAnnouncement = Maps.newHashMap();

	protected String myInstanceId = UUID.randomUUID().toString();
	protected Map<String, Object> announcement = Map.of("instanceId", myInstanceId, "upOrDown", true);

	void syncTask() {
		try {
			microServiceUtils.serializeAndSplitObjectAndEmit(announcement, Integer.MAX_VALUE, syncEmitter, objectMapper);
		} catch (ResponseException e) {
			logger.error("Failed to serialize sync message.", e);
		}
	}

	@PreDestroy
	void shutdown() {
		try {
			microServiceUtils.serializeAndSplitObjectAndEmit(Map.of("instanceId", myInstanceId, "upOrDown", false),
					Integer.MAX_VALUE, syncEmitter, objectMapper);
		} catch (ResponseException e) {
			logger.error("Failed to serialize sync message.", e);
		}
	}

	@SuppressWarnings("unchecked")
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
		historyMessaging.setInstancesNr(instanceId2LastAnnouncement.size());
		ArrayList<String> l1 = Lists.newArrayList(instanceId2LastAnnouncement.keySet());
		Collections.sort(l1);
		historyMessaging.setMyInstancePos(l1.indexOf(myInstanceId) + 1);
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
