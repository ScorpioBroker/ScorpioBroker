package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.mutiny.core.Vertx;

@Singleton
@IfBuildProfile(anyOf = { "mqtt", "rabbitmq" })
@IfBuildProperty(enableIfMissing = true, name = "scorpio.subsync.enabled", stringValue = "true")
public class RegistrySubscriptionSyncServiceByteArray implements SyncService {

	public static final String SYNC_ID = UUID.randomUUID().toString();

	AliveAnnouncement INSTANCE_ID;

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	private Set<String> currentInstances = Sets.newHashSet();

	private Set<String> lastInstances = Sets.newHashSet();

	private MessageCollector collector = new MessageCollector(this.getClass().getName());

	@Inject
	@Channel(AppConstants.SUB_ALIVE_CHANNEL)
	MutinyEmitter<String> aliveEmitter;

	@Inject
	@Channel(AppConstants.SUB_SYNC_CHANNEL)
	MutinyEmitter<String> syncEmitter;

	@Inject
	RegistrySubscriptionService subService;
	@Inject
	ObjectMapper objectMapper;
	@Inject
	Vertx vertx;

	private EventLoopGroup executor;

	CollectMessageListener collectListenerSubs = new CollectMessageListener() {

		@Override
		public void collected(String byteMessage) {
			SyncMessage message;
			try {
				message = objectMapper.readValue(byteMessage, SyncMessage.class);
			} catch (IOException e) {
				logger.error("failed to read sync message", e);
				return;
			}
			String key = message.getSyncId();
			SubscriptionRequest sub = message.getRequest();
			if (key.equals(SYNC_ID) || message.getSubType() == SyncMessage.NORMAL_SUB) {
				return;
			}
			switch (sub.getRequestType()) {
			case AppConstants.DELETE_REQUEST:
				subService.syncDeleteSubscription(sub).runSubscriptionOn(executor).subscribe()
						.with(v -> logger.debug("done handling delete"));
				break;
			case AppConstants.UPDATE_REQUEST:
				subService.syncUpdateSubscription(sub).runSubscriptionOn(executor).subscribe()
						.with(v -> logger.debug("done handling update"));
				break;
			case AppConstants.CREATE_REQUEST:
				subService.syncCreateSubscription(sub).runSubscriptionOn(executor).subscribe()
						.with(v -> logger.debug("done handling create"));
				break;
			default:
				return;
			}

		}
	};
	CollectMessageListener collectListenerAlive = new CollectMessageListener() {

		@Override
		public void collected(String byteMessage) {
			AliveAnnouncement message;
			try {
				message = objectMapper.readValue(byteMessage, AliveAnnouncement.class);
			} catch (IOException e) {
				logger.error("failed to read sync id", e);
				return;
			}
			if (message.getId().equals(SYNC_ID) || message.getSubType() == SyncMessage.NORMAL_SUB) {
				return;
			}
			currentInstances.add(message.getId());
		}
	};

	@ConfigProperty(name = "scorpio.messaging.maxSize")
	int messageSize;

	@PostConstruct
	public void setup() {
		INSTANCE_ID = new AliveAnnouncement(SYNC_ID);
		INSTANCE_ID.setSubType(AliveAnnouncement.REG_SUB);
		subService.addSyncService(this);
		this.executor = vertx.getDelegate().nettyEventLoopGroup();
	}

	@Scheduled(every = "${scorpio.sync.announcement-time}", delayed = "${scorpio.startupdelay}")
	Uni<Void> syncTask() {
		MicroServiceUtils.serializeAndSplitObjectAndEmit(INSTANCE_ID, messageSize, aliveEmitter, objectMapper);
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
	Uni<Void> listenForSubs(byte[] byteMessage) {
		collector.collect(new String(byteMessage), collectListenerSubs);
		return Uni.createFrom().voidItem();
	}

	@Incoming(AppConstants.SUB_ALIVE_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	Uni<Void> listenForAlive(byte[] byteMessage) {
		collector.collect(new String(byteMessage), collectListenerAlive);
		return Uni.createFrom().voidItem();
	}

	private void recalculateSubscriptions() {
		HashSet<String> temp = Sets.newHashSet(currentInstances);
		temp.add(INSTANCE_ID.getId());
		List<String> sortedInstances = temp.stream().sorted().collect(Collectors.toList());
		int myPos = sortedInstances.indexOf(INSTANCE_ID.getId());
		List<String> sortedSubs = subService.getAllSubscriptionIds();
		int stepRange = sortedSubs.size() / sortedInstances.size();
		int start = myPos * stepRange;
		int end;
		if (myPos == sortedInstances.size() - 1) {
			end = sortedSubs.size();
		} else {
			end = (myPos + 1) * stepRange;
		}
		List<String> mySubs = sortedSubs.subList(start, end);
		subService.activateSubs(mySubs);
	}

	public Uni<Void> sync(SubscriptionRequest request) {
		MicroServiceUtils.serializeAndSplitObjectAndEmit(new SyncMessage(SYNC_ID, request, SyncMessage.REG_SUB), messageSize, syncEmitter,
				objectMapper);
		return Uni.createFrom().voidItem();
	}

}
