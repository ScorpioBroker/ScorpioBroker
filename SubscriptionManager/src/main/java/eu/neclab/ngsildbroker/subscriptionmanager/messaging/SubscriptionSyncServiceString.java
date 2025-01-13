package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.mutiny.core.Vertx;

@Singleton
@IfBuildProfile(anyOf = { "kafka" })
@IfBuildProperty(enableIfMissing = true, name = "scorpio.subsync.enabled", stringValue = "true")
public class SubscriptionSyncServiceString extends SubscriptionSyncServiceBase {

	public static final String SYNC_ID = UUID.randomUUID().toString();

	AliveAnnouncement INSTANCE_ID;

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	private Set<String> currentInstances = Sets.newHashSet();

	private Set<String> lastInstances = Sets.newHashSet();

	@Inject
	@Channel(AppConstants.SUB_ALIVE_CHANNEL)
	@Broadcast
	MutinyEmitter<String> aliveEmitter;

	@Inject
	@Channel(AppConstants.SUB_SYNC_CHANNEL)
	@Broadcast
	MutinyEmitter<String> syncEmitter;

	@Inject
	SubscriptionService subService;

	@Inject
	ObjectMapper objectMapper;
	
	@Inject
	MicroServiceUtils microServiceUtils;

	@ConfigProperty(name = "scorpio.messaging.maxSize")
	int messageSize;

	@Inject
	Vertx vertx;

	private Executor executor;

	@PostConstruct
	public void setup() {
		INSTANCE_ID = new AliveAnnouncement(SYNC_ID);
		INSTANCE_ID.setSubType(AliveAnnouncement.NORMAL_SUB);
		subService.addSyncService(this);
		this.executor = Executors.newFixedThreadPool(2);
		try {
			microServiceUtils.serializeAndSplitObjectAndEmit(INSTANCE_ID, messageSize, aliveEmitter, objectMapper);
		} catch (ResponseException e) {
			logger.error("Failed to serialize sync message", e);
		}

	}

	@Scheduled(every = "${scorpio.sync.announcement-time}", delayed = "${scorpio.startupdelay}")
	Uni<Void> syncTask() {
		try {
			microServiceUtils.serializeAndSplitObjectAndEmit(INSTANCE_ID, messageSize, aliveEmitter, objectMapper);
		} catch (ResponseException e) {
			logger.error("Failed to serialize sync message", e);
		}
		return Uni.createFrom().voidItem();
	}

	@Scheduled(every = "${scorpio.sync.check-time}", delayed = "${scorpio.startupdelay}")
	Uni<Void> checkTask() {
		if (!currentInstances.equals(lastInstances)) {
			recalculateSubscriptions();
		}
		lastInstances.clear();
		lastInstances.addAll(currentInstances);
		currentInstances.clear();
		return Uni.createFrom().voidItem();
	}

	@Incoming(AppConstants.SUB_SYNC_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	Uni<Void> listenForSubs(String byteMessage) {
		SyncMessage message;
		try {
			message = objectMapper.readValue(byteMessage, SyncMessage.class);
		} catch (IOException e) {
			logger.error(byteMessage);
			logger.error("failed to read sub sync", e);
			return Uni.createFrom().voidItem();
		}
		return baseHandleSync(message, SYNC_ID, subService, executor);
	}

	@Incoming(AppConstants.SUB_ALIVE_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	Uni<Void> listenForAlive(String byteMessage) {
		logger.debug("receving alive");
		logger.debug(byteMessage);
		AliveAnnouncement message;
		try {
			message = objectMapper.readValue(byteMessage, AliveAnnouncement.class);
		} catch (IOException e) {
			logger.error("failed to read sync id", e);
			return Uni.createFrom().voidItem();
		}
		if (message.getId().equals(SYNC_ID) || message.getSubType() == SyncMessage.REG_SUB) {
			return Uni.createFrom().voidItem();
		}
		currentInstances.add(message.getId());
		return Uni.createFrom().voidItem();
	}

	private void recalculateSubscriptions() {
		HashSet<String> temp = Sets.newHashSet(currentInstances);
		temp.add(INSTANCE_ID.getId());
		List<String> sortedInstances = temp.stream().sorted().collect(Collectors.toList());
		int myPos = sortedInstances.indexOf(INSTANCE_ID.getId());
		List<String> sortedSubs = subService.getAllSubscriptionIds();
		int stepRange = (int) Math.ceil(((double) sortedSubs.size()) / ((double) sortedInstances.size()));
		int start = myPos * stepRange;
		int end = start + stepRange;

		if (end > sortedSubs.size()) {
			end = sortedSubs.size();
		}
		logger.debug("step:" + stepRange + " mypos: " + myPos + " start:" + start + " end:" + end + " sorrted size:"
				+ sortedSubs.size());
		List<String> mySubs = sortedSubs.subList(start, end);
		subService.activateSubs(mySubs);
	}

	public Uni<Void> sync(SubscriptionRequest request) {
		logger.debug("send sub");
		logger.debug(request.getRequestType() + "");
		logger.debug(request.getId());
		try {
			microServiceUtils.serializeAndSplitObjectAndEmit(new SyncMessage(SYNC_ID, request.getId(),
					request.getTenant(), request.getRequestType(), SyncMessage.NORMAL_SUB), messageSize, syncEmitter,
					objectMapper);
		} catch (ResponseException e) {
			logger.error("Failed to serialize sync message", e);
		}
		return Uni.createFrom().voidItem();
	}

}
